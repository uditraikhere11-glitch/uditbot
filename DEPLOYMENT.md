# Production & VM Deployment Architecture

## 1. Hosting Architecture: Google Compute Engine (GCE)

The bot runs on a single dedicated Google Compute Engine (GCE) virtual machine provisioned in an Asia-South region (`asia-south1` Mumbai or `asia-south2` Delhi) for minimal latency to Indian exchange and broker infrastructure.

### Recommended VM Sizing
- **Machine Type**: `e2-small` (2 vCPUs, 2 GB RAM) or `e2-medium` (2 vCPUs, 4 GB RAM).
- **Operating System**: Debian 12 (Bookworm) or Ubuntu 24.04 LTS x86_64 minimal.
- **Boot Disk**: 20 GB Balanced Persistent Disk (pd-balanced).
- **Static External IP**: **Mandatory**. Dhan requires order-placement API calls to originate from an authorized, static IP whitelisted in Udit's Dhan account settings.

---

## 2. Setup System: Configuration Options

The system offers two integrated setup mechanisms matching your setup interface:

### 2.1 Terminal Setup Wizard
From the VM terminal:
```bash
python3 -m src.setup_wizard
# or using the CLI dashboard:
bot setup
```
This guides you through:
1. **Exchange Credentials**: Dhan Client ID, Access Token, PIN, TOTP Secret.
2. **Bot Configuration**: Bot Instance Name, Monitoring Group ID, Test Group ID.
3. **Risk Management**: Max Drawdown %, Daily Trade Limit, Max Order Amount.
4. **Execution Mode**: TEST (Safe Default) vs. LIVE.

It automatically writes a secure `.env` file with `chmod 0600` permissions.

### 2.2 Web Setup Dashboard
If you prefer configuring via the web interface shown in your setup mockup:
```bash
bot web
# or:
python3 -m src.setup_app
```
Connect via SSH local port forwarding (`ssh -L 8080:localhost:8080 user@vm-ip`) and open `http://localhost:8080` in your browser.

---

## 3. Visual Telegram Log Notification Format

Incoming signals, risk validations, and executions are dispatched to your configured `LOG_GROUP_ID` matching your exact card layout:

```
✅ Trade Signal Processed: TMPV (Source: Test group)

📩 The Trade Details:

o   Original Message: #TMPV 280CE @8.5-9 Sl-5 Target-16,20 #Positional

🤖 Bot Action Taken:

o   Segment: FNO
o   Status: Message sent to Dhan
o   Dhan Order Details: [Order ID: 123456 / Status: PENDING]

⚠️ Heads Up: Instrument unverified
_________________________
⏰ 00:00:59 IST
🟢 Bot - LIVE
```
*(In TEST mode, the footer displays `🟡 Bot - TEST`).*

---

## 4. Systemd Service Configuration

### 4.1 Main Service Unit (`/etc/systemd/system/trading_bot.service`)
```ini
[Unit]
Description=Telegram-to-Dhan Trading Bot (Udit Single Account)
After=network.target time-sync.target
Wants=time-sync.target

[Service]
Type=simple
User=trader
Group=trader
WorkingDirectory=/opt/trading_bot
EnvironmentFile=/opt/trading_bot/.env
ExecStart=/opt/trading_bot/venv/bin/python3 run.py
Restart=always
RestartSec=10
LimitNOFILE=65536

# Sandboxing & Security Hardening
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=/opt/trading_bot/data /opt/trading_bot/logs /opt/trading_bot/.dhan_token
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

### 4.2 Daily 08:00 IST Health Check & Token Refresh Timer
- Systemd Timer: `/etc/systemd/system/dhan-token-refresh.timer`
```ini
[Unit]
Description=Daily Dhan Token Refresh at 08:00 IST

[Timer]
OnCalendar=Mon..Fri *-*-* 08:00:00 Asia/Kolkata
Persistent=true

[Install]
WantedBy=timers.target
```
- Service: `/etc/systemd/system/dhan-token-refresh.service`
```ini
[Unit]
Description=Execute Dhan Token Refresh and Readiness Probe

[Service]
Type=oneshot
User=trader
WorkingDirectory=/opt/trading_bot
EnvironmentFile=/opt/trading_bot/.env
ExecStart=/opt/trading_bot/venv/bin/python3 -m src.cli daily_health_check
```

---

## 5. SSH Terminal Operator Dashboard (`bot` CLI)

```bash
bot status         # Displays running mode (TEST/LIVE), uptime, broker status
bot setup          # Launches interactive setup wizard
bot web            # Launches setup web dashboard on port 8080
bot mode           # Displays current mode and products active/inactive
bot journal [n]    # Shows last n entries from SQLite audit journal
bot emergency-stop # Immediate halt: blocks new entries, cancels pending entries
bot resume         # Resumes normal operation
```
