#!/usr/bin/env bash
# =====================================================================
# PLATINUM / UDITBOT - GCE VM PROVISIONING & SETUP SCRIPT
# Run this script on your Google Compute Engine VM (Debian 12 / Ubuntu 24.04):
#   bash deploy/setup_vm.sh
# =====================================================================
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

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
    echo "Virtual environment created at: $REPO_DIR/venv"
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
chmod +x "$REPO_DIR/bot"
if [ -w "/usr/local/bin" ]; then
    ln -sf "$REPO_DIR/bot" /usr/local/bin/bot
    echo "Symlinked /usr/local/bin/bot -> $REPO_DIR/bot"
elif command -v sudo &>/dev/null; then
    sudo ln -sf "$REPO_DIR/bot" /usr/local/bin/bot
    echo "Symlinked /usr/local/bin/bot -> $REPO_DIR/bot (via sudo)"
fi

# 5. Systemd Service Installation
echo "[5/6] Configuring systemd services..."
CURRENT_USER="$(whoami)"

SERVICE_FILE="/etc/systemd/system/trading_bot.service"
TIMER_FILE="/etc/systemd/system/dhan-token-refresh.timer"
REFRESH_SERVICE="/etc/systemd/system/dhan-token-refresh.service"

if command -v sudo &>/dev/null; then
    # Write trading_bot.service
    sudo tee "$SERVICE_FILE" > /dev/null <<EOF
[Unit]
Description=Telegram-to-Dhan Trading Bot (UditBot Single Account)
After=network.target time-sync.target
Wants=time-sync.target

[Service]
Type=simple
User=$CURRENT_USER
WorkingDirectory=$REPO_DIR
EnvironmentFile=$REPO_DIR/.env
ExecStart=$REPO_DIR/venv/bin/python3 run.py
Restart=always
RestartSec=10
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

    # Write dhan-token-refresh.timer
    sudo tee "$TIMER_FILE" > /dev/null <<EOF
[Unit]
Description=Daily Dhan Token Refresh at 08:00 IST

[Timer]
OnCalendar=Mon..Fri *-*-* 08:00:00 Asia/Kolkata
Persistent=true

[Install]
WantedBy=timers.target
EOF

    # Write dhan-token-refresh.service
    sudo tee "$REFRESH_SERVICE" > /dev/null <<EOF
[Unit]
Description=Execute Dhan Token Refresh and Health Probe

[Service]
Type=oneshot
User=$CURRENT_USER
WorkingDirectory=$REPO_DIR
EnvironmentFile=$REPO_DIR/.env
ExecStart=$REPO_DIR/venv/bin/python3 -m src.cli daily_health_check
EOF

    sudo systemctl daemon-reload
    sudo systemctl enable --now dhan-token-refresh.timer || true
    echo "Systemd unit files installed and dhan-token-refresh.timer enabled."
else
    echo "Warning: sudo not available. Skipping automatic systemd installation."
fi

# 6. Initial Configuration & Health Check
echo "[6/6] Checking configuration..."
if [ ! -f ".env" ]; then
    echo "No .env configuration found. Launching setup wizard..."
    python3 -m src.setup_wizard
fi

echo ""
echo "Running automated verification tests..."
python3 -m unittest discover tests/ -v

echo ""
echo "======================================================="
echo "✅ VM INSTALLATION COMPLETE!                            "
echo "You can now control the bot with the 'bot' command:     "
echo "  bot status       # Check status                      "
echo "  bot setup        # Re-run configuration wizard       "
echo "  bot web          # Launch web UI on port 8080        "
echo "  bot start        # Start systemd background service  "
echo "  bot service-logs # View live logs                    "
echo "======================================================="
