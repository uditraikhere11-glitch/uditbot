# Project Status & Implementation Tracking

**Last Updated**: October 2026  
**Operator**: Udit (Single Account)  
**Target Environment**: Google Compute Engine (GCE) VM, Debian 12 / Ubuntu 24.04  
**Broker**: DhanHQ API v2  
**Current Master Switch**: `TRADING_MODE=TEST` (Default)  
**GitHub Repository**: `https://github.com/uditraikhere11-glitch/auditbot.git`

---

## 1. Executive Implementation Summary

| Component | Status | Verification & Evidence | Next Step |
|---|---|---|---|
| **Reference Review (`darvas_script`)** | **Completed** | Full audit completed. Approved rules extracted; Notion, Sheets, Kite, Railway, multi-user stripped. | Preserve approved patterns. |
| **System Documentation** | **Completed** | `REQUIREMENTS.md`, `DHAN_API.md`, `DEPLOYMENT.md`, `PROJECT_STATUS.md`, `README.md` drafted. | Maintained and current. |
| **Visual Log Message Formatter** | **Completed** | Exact layout from reference image: `✅ Trade Signal Processed`, `Original Message`, `Segment`, `Status`, `Dhan Order Details`, `⚠️ Heads Up`, `⏰ IST`, `🟢 LIVE / 🟡 TEST`. | Verified via `test_notifier.py`. |
| **Setup System (CLI & Web)** | **Completed** | Integrated interactive wizard (`src/setup_wizard.py`) and FastAPI web app (`src/setup_app.py`) matching setup screen mockup. | Verified via `test_setup.py`. |
| **Configuration & Mode Engine** | **Completed** | Single `TEST`/`LIVE` switch, `EQUITY_ACTIVE`, `FNO_ACTIVE`, confirmed thresholds. | Wired to systemd environment. |
| **Signal Parsing & Gatekeepers** | **Completed** | Fast-path regex for equity & stock options, 60s age check, index blocker, exit classifier. | 51 automated unit tests passed. |
| **Deduplication Engine** | **Completed** | 20s cross-group sliding window per underlying stock symbol. | Verified via unit tests. |
| **Instrument Resolution** | **Completed** | Official Dhan scrip master loader, strike/expiry resolver, index filtering. | Connect daily 08:00 CSV pull. |
| **Position Sizing & Limits** | **Completed** | Equity share/budget sizing, option lot sizing, 1.04 ceiling calculation, tick rounding. | 51 automated unit tests passed. |
| **Risk & Emergency Stop** | **Completed** | Max order value, min funds, daily loss, exposure, open positions, emergency kill switch. | Wire to broker balance feeds. |
| **Durable Audit Journal** | **Completed** | SQLite database schema with zero fake fills, full signal/validation/order lifecycle audit. | Integration with broker events. |
| **Broker Client (Dhan v2)** | **Design Ready** | Clean REST interface for auth, LTP, orders, Forever OCO, static IP compliance. | Awaiting GCE VM static IP. |
| **Telegram Listener** | **Design Ready** | Telethon client with reconnect, channel filters, session protection. | Ready for VM session setup. |
| **Live Broker Connectivity** | **Pending Approval** | Prohibited until TEST verification passes and explicit written sign-off is given. | Operator live sign-off. |
| **GitHub Integration** | **Configured** | Remote `origin` linked to `auditbot.git`; commits staged on `main`. | Push to GitHub via VM or local push. |
| **VM Deployment** | **Ready for TEST Mode** | GCE specs and systemd scripts ready. Operator will setup VM upon request. | Ready to deploy on GCE VM. |

---

## 2. Implemented vs Planned Matrix

### 2.1 Signal Parsing & Ingestion
- [x] Deterministic regex fast path for equity cash buy signals (`#SYMBOL`, `BUY SYMBOL @ PRICE`, CMP).
- [x] Deterministic regex fast path for stock options buy signals (`#SYMBOL STRIKE TYPE @ RANGE`).
- [x] Index trade blocker (`NIFTY`, `BANKNIFTY`, `FINNIFTY`, `MIDCPNIFTY`, `SENSEX`, `BANKEX`) — hard rejection.
- [x] Exit signal classifier (`EXIT`, `BOOK`, `SQUARE OFF`, `SL HIT`, `TARGET HIT`) — logs for audit, never places orders.
- [x] Message age validator (reject if $> 60$ seconds old based on IST clock).
- [x] Message length filter ($> 300$ chars rejected) and multi-stock message blocker.
- [x] 20-second cross-group deduplication window for identical underlying stocks.
- [ ] AI fallback integration (LLM candidate extraction only for unparsed text with buy intent).

### 2.2 Sizing, Pricing & Risk Management
- [x] Equity sizing: Fixed shares OR budget-based share count (`floor(budget / price)`).
- [x] Minimum size enforcement: Reject if budget cannot purchase 1 share.
- [x] Equity limit price: CMP + setup percentage markup, rounded to exchange tick size (0.05).
- [x] Stock option sizing: Fixed lots OR budget-based whole lot count (`floor(budget / (lot * price))`).
- [x] Minimum lot enforcement: Reject if budget cannot purchase 1 full lot.
- [x] Stock option limit price: $1.04 \times \text{entry\_max}$, floored to contract tick size.
- [x] Current calendar month default when option expiry is omitted.
- [x] Risk engine: Max order amount, min funds check, daily loss, total exposure, open positions cap.
- [x] Emergency stop: Instantly blocks new signals, cancels resting entries, preserves protection legs.

### 2.3 Protection & Order Management
- [x] Broker-side Forever OCO placement logic (SL + Target calculated strictly from executed fill price).
- [x] Tick rounding logic: SL rounded down to tick, Target rounded up to tick.
- [x] Durable SQLite journal tracking signals, validations, orders, and executions.
- [x] Visual card formatting matching specification (`format_trade_log`, `format_exit_log`).
- [ ] Live order polling, timeout cancellation, and single retry reconciliation.
- [ ] Live Forever OCO API submission via Dhan v2.

### 2.4 Infrastructure & Operations
- [x] Interactive CLI setup wizard (`src/setup_wizard.py` / `bot setup`).
- [x] Setup web application (`src/setup_app.py` / `bot web`).
- [x] SSH Terminal CLI (`bot` script) specifications for status, mode, risk, and emergency stop.
- [x] Systemd service and 08:00 IST token refresh timer unit files.
- [x] GitHub repository remote linked to `https://github.com/uditraikhere11-glitch/auditbot.git`.
- [ ] GCE VM provisioning with static external IP (pending operator setup).

---

## 3. Explicit Readiness Declarations
- **Live Trading Readiness**: **NOT READY (BY DESIGN)**. Live order placement is disabled and blocked by default (`TRADING_MODE=TEST`).
- **Simulated Trading**: **STRICTLY ZERO**. No paper trading, synthetic fills, or fake holdings exist in the codebase.
- **TEST Mode VM Deployment**: **READY**. Verified with 51 passing automated tests.
