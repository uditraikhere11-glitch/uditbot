#!/usr/bin/env bash
# =====================================================================
# PLATINUM / UDITBOT - GCE VM PROVISIONING & SETUP SCRIPT
# Run this script on your Google Compute Engine VM (Debian 12/13 / Ubuntu 24.04):
#   Fresh install:   bash deploy/setup_vm.sh
#   Rebuild restore: bash deploy/setup_vm.sh --restore /path/to/backup.tar.gz.gpg
#   Opt-in full test suite in isolated temp dir (H-05):
#                    AUDITBOT_RUN_FULL_TESTS=1 bash deploy/setup_vm.sh
# =====================================================================
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

RESTORE_ARCHIVE="${AUDITBOT_RESTORE_ARCHIVE:-}"
RUN_FULL_TESTS="${AUDITBOT_RUN_FULL_TESTS:-0}"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --restore)
            [[ $# -ge 2 && -f "${2:-}" ]] || {
                echo "Usage: bash deploy/setup_vm.sh [--restore /path/to/backup.tar.gz.gpg] [--run-tests]" >&2
                exit 2
            }
            RESTORE_ARCHIVE="$(realpath -m -- "$2")"
            shift 2
            ;;
        --run-tests)
            RUN_FULL_TESTS=1
            shift
            ;;
        *)
            echo "Usage: bash deploy/setup_vm.sh [--restore /path/to/backup.tar.gz.gpg] [--run-tests]" >&2
            exit 2
            ;;
    esac
done

echo "======================================================="
echo "   STARTING PLATINUM / UDITBOT VM INSTALLATION        "
echo "   Target Directory: $REPO_DIR                          "
echo "======================================================="

# 1. Check Python version
echo "[1/6] Checking Python installation..."
if ! command -v python3 &>/dev/null; then
    echo "Error: Python 3 is not installed. Please install Python 3.11 or 3.12+."
    exit 1
fi
python3 --version

# 2. Virtual Environment Setup
echo "[2/6] Setting up Python virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "Virtual environment created at the repo: $REPO_DIR/venv"
fi
source venv/bin/activate
pip install --upgrade pip
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
else
    pip install pydantic fastapi uvicorn requests pytz telethon
fi

# 3. Create Runtime Directories
echo "[3/6] Setting up runtime storage & permissions..."
mkdir -p data logs
chmod 0700 data logs

# 4. Make CLI executable and symlink to /usr/local/bin/bot
echo "[4/6] Installing 'bot' command line utility..."
chmod +x "$REPO_DIR/bot" "$REPO_DIR/deploy/"*.sh 2>/dev/null || chmod +x "$REPO_DIR/bot"
if [ -w "/usr/local/bin" ]; then
    ln -sf "$REPO_DIR/bot" /usr/local/bin/bot
    echo "Symlinked /usr/local/bin/bot -> $REPO_DIR/bot"
elif command -v sudo &>/dev/null; then
    sudo ln -sf "$REPO_DIR/bot" /usr/local/bin/bot
    echo "Symlinked /usr/local/bin/bot -> $REPO_DIR/bot (via sudo)"
fi

# 5. Services are provisioned only after .env configuration exists.
# This avoids starting the timer/WebUI before the first configuration
# and prevents restore_runtime.sh from being blocked by a running WebUI or wizard .env.
echo "[5/6] Preparing deployment prerequisites..."

# 6. Initial Configuration or Encrypted Backup Restore (Before Service Start)
echo "[6/6] Checking configuration..."
if [ -n "$RESTORE_ARCHIVE" ]; then
    echo "Restoring encrypted runtime backup before provisioning services (zero re-registration)..."
    bash "$REPO_DIR/deploy/restore_runtime.sh" "$RESTORE_ARCHIVE"
elif [ ! -f ".env" ]; then
    echo "No .env configuration found. Launching setup wizard..."
    python3 -m src.setup_wizard
fi

# Fresh-VM service provisioning; existing units/drop-ins are not overwritten.
# Trading daemon never started automatically; boot enable is opt-in.
bash "$REPO_DIR/deploy/install_vm_services.sh" --install

echo ""
echo "Running non-mutating post-install verification checks (H-05)..."
bash -n "$REPO_DIR/bot" "$REPO_DIR/deploy/"*.sh
VERIFY_TMP="$(mktemp -d)"
PYTHONPYCACHEPREFIX="$VERIFY_TMP/pycache" TMPDIR="$VERIFY_TMP" python3 -B -m py_compile "$REPO_DIR/run.py" "$REPO_DIR/src/cli.py" "$REPO_DIR/src/config.py" "$REPO_DIR/src/main.py" "$REPO_DIR/src/setup_app.py"
PYTHONPYCACHEPREFIX="$VERIFY_TMP/pycache" TMPDIR="$VERIFY_TMP" AUDITBOT_TEST_ISOLATION=1 python3 -B -m unittest tests.test_vm_rebuild_deployment -v
if [[ "$RUN_FULL_TESTS" == "1" ]]; then
    echo "Running opt-in full test suite under AUDITBOT_TEST_ISOLATION=1..."
    PYTHONPYCACHEPREFIX="$VERIFY_TMP/pycache" TMPDIR="$VERIFY_TMP" AUDITBOT_TEST_ISOLATION=1 python3 -B -m unittest discover tests/ -v
fi
rm -rf -- "$VERIFY_TMP"

echo ""
echo "======================================================="
echo "✅ VM INSTALLATION COMPLETE!                            "
echo "You can now control the bot with the 'bot' command:     "
echo "  bot status       # Check status                      "
echo "  bot audit        # Read-only VM & systemd drift audit"
echo "  bot backup <out> # Create encrypted off-VM backup    "
echo "  bot restore <in> # Restore encrypted backup (TEST)   "
echo "  bot setup        # Re-run configuration wizard       "
echo "  bot web          # Launch web UI on port 8080        "
echo "  bot start        # Start systemd background service  "
echo "  bot service-logs # View live systemd service logs    "
echo "======================================================="
