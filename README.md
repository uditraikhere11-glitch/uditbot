# Telegram-to-Dhan Trading Bot (Single-Account Architecture)

A dedicated, high-precision automated trade execution system for single operator **Udit**, connecting Telegram signal channels directly to **DhanHQ API v2** on a dedicated Google Compute Engine (GCE) VM.

---

## Core System Architecture & Principles

```
  Telegram Channels (Real-Time Ingestion)
  [Test Group | VIP Channel 1 | VIP Channel 2]
                       │
                       ▼
    Telegram Client (Telethon / Pyrogram)
  (Session isolation, auto-reconnect, 60s age check)
                       │
                       ▼
  Deterministic Gatekeeper & Parser (Fast-Path Regex)
  ├── 20s Cross-Group Deduplication Window (Per Stock)
  ├── Hard Index Blocker (Rejects NIFTY, BANKNIFTY, SENSEX)
  ├── Direction Classifier (BUY entries only; exits logged)
  └── Scrip Master Resolution (NSE/BSE Equities & Stock Options)
                       │
                       ▼
    Position Sizing & Risk Management Engine
  ├── Single TEST / LIVE Master Switch (Default: TEST)
  ├── Product Toggles (EQUITY_ACTIVE / FNO_ACTIVE)
  ├── Tick-Aligned Limit Ceiling (Options: Entry Max x 1.04)
  ├── Tick-Aligned Equity Limit (CMP + Setup Markup %)
  └── Exposure, Daily Loss, Max Order & Emergency Stop
                       │
                       ▼
               TRADING_MODE?
              /             \
        [TEST Mode]      [LIVE Mode]
             │                │
  Report Signal, Pricing,     DhanHQ v2 REST API (Static IP)
  Sizing & Risk Validation    ├── POST /v2/orders (LIMIT)
  to Telegram Log Group       ├── Poll Fills / Timeout Cancellation
  (Zero fake fills / orders)  └── Post-Fill Forever OCO (SL & Target)
             │                │
             └────────┬───────┘
                      ▼
        Durable SQLite Audit Journal
        (Audit trail, orders, executions)
```

---

## Visual Log Message Card Format

Incoming signals and execution decisions stream directly to your configured Telegram Log Group using the structured card format:

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
*(In TEST mode, the footer displays `🟡 Bot - TEST` with order status indicating zero broker orders sent).*

---

## Setup System Options

The system includes two setup interfaces:

1. **Interactive Terminal Setup Wizard** (`bot setup` or `python -m src.setup_wizard`):
   - Guides you step-by-step through Exchange Credentials, Bot Configuration, Risk Limits, and Execution Mode.
   - Automatically writes to `.env` with `chmod 0600` permissions.

2. **Setup Web Interface** (`bot web` or `python -m src.setup_app`):
   - Launches a dark-themed setup dashboard on `http://127.0.0.1:8080` (accessible locally or via SSH tunnel).
   - Allows graphical configuration of Exchange Credentials, Bot Configuration, Risk Management, and Execution Mode toggle.

---

## Directory Structure

```
├── DEPLOYMENT.md              # GCE VM setup, systemd, CLI & security guide
├── DHAN_API.md                # DhanHQ API v2 specs, auth, orders & errors
├── PROJECT_STATUS.md          # Implemented vs planned matrix & status
├── README.md                  # This file
├── REQUIREMENTS.md            # Complete requirements & confirmed decisions
├── src/
│   ├── __init__.py
│   ├── config.py              # Pydantic environment configuration
│   ├── models.py              # Strongly-typed data models
│   ├── setup_wizard.py        # Terminal setup wizard
│   ├── setup_app.py           # Setup web interface (FastAPI)
│   ├── parsers/
│   │   ├── __init__.py
│   │   ├── equity_parser.py   # Equity regex parser & filters
│   │   ├── fno_parser.py      # Stock options regex parser & filters
│   │   └── duplicate_filter.py# 20s sliding window deduplicator
│   ├── instruments/
│   │   ├── __init__.py
│   │   └── resolver.py        # Scrip master loader & contract resolver
│   ├── engine/
│   │   ├── __init__.py
│   │   ├── sizer.py           # Equity & option lot sizing algorithms
│   │   └── risk_manager.py    # Risk limits & emergency stop controller
│   ├── notifier/
│   │   ├── __init__.py
│   │   ├── formatter.py       # Visual Telegram card layout
│   │   └── telegram_notifier.py # Log group dispatcher
│   ├── journal/
│   │   ├── __init__.py
│   │   └── db.py              # SQLite audit journal & persistence
│   ├── broker/
│   │   ├── __init__.py
│   │   └── dhan_client.py     # DhanHQ API v2 client interface
│   ├── cli.py                 # SSH Terminal Operator Dashboard
│   └── main.py                # Main application orchestrator
└── tests/
    ├── test_equity_parser.py  # Equity parser & index blocker tests
    ├── test_fno_parser.py     # Stock option parser & ceiling tests
    ├── test_duplicate_filter.py # Cross-group 20s deduplication tests
    ├── test_sizer.py          # Lot & budget sizing tests
    ├── test_risk_manager.py   # Risk thresholds & emergency stop tests
    ├── test_journal.py        # SQLite audit journal tests
    ├── test_notifier.py       # Visual log card layout tests
    ├── test_setup.py          # Setup app and wizard tests
    └── test_pipeline_e2e.py   # Full pipeline integration tests
```

---

## Quick Start & Verification

### 1. Run Automated Test Suite
```bash
python3 -m unittest discover tests/ -v
```
*(51 tests pass cleanly).*

### 2. Configure via Setup Wizard
```bash
python3 -m src.setup_wizard
# or launch web interface:
python3 -m src.setup_app
```

### 3. SSH Terminal Dashboard Commands
```bash
bot status         # Check system status, mode, and health
bot setup          # Launch interactive terminal setup wizard
bot web            # Launch setup web interface on port 8080
bot emergency-stop # Instantly halt all trade entry
bot journal        # View recent signal evaluations
```
