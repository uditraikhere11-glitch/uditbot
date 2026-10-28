# System Requirements & Trading Specification

## 1. Project Overview & Scope

The system is a dedicated, single-account Telegram-to-Dhan automated trading bot designed for a single operator (**Udit**). The bot monitors designated Telegram channels/groups, parses trade recommendations for Indian Equities and Stock Options (F&O), deterministically validates the signals against strict risk and regulatory criteria, and manages trade execution through the official DhanHQ API v2.

### Strict Scope Boundaries & Prohibitions
- **Single Account Only**: Exactly one operator (Udit), one Telegram user/bot session, and one Dhan trading account. Multi-user, multi-tenant, or client-facing architectures are strictly excluded.
- **Excluded Integrations**: No Notion, Google Sheets, Kite (Zerodha), Railway, Discord, SMTP/Email, or third-party webhooks.
- **Zero Paper Trading / Simulated Fills**: The bot operates in either `TEST` or `LIVE` mode. In `TEST` mode, the bot performs end-to-end signal ingestion, regex extraction, security resolution, price/tick calculation, sizing, risk validation, and audit logging—without placing broker orders. **No paper trading, fake fills, simulated holdings, or synthetic positions will ever be generated.**
- **Dedicated Infrastructure**: Clean deployment on a dedicated Google Compute Engine (GCE) VM with a static external IP (mandatory for Dhan API order placement).
- **Git & Secret Hygiene**: Source code managed on GitHub as the single source of truth. Zero credentials, secrets, API keys, or Telegram `.session` files committed to version control.

---

## 2. Operating Modes & Switches

### 2.1 Single Common TEST / LIVE Master Switch
- **Switch Variable**: `TRADING_MODE=TEST` (default) or `TRADING_MODE=LIVE`.
- **Scope**: Controls both Equity and F&O simultaneously. No redundant secondary live switches or complex multi-step approvals.
- **Behavior in TEST Mode**:
  - Monitors Telegram channels in real-time.
  - Validates source channel, message age ($\le 60$ seconds), and duplicate window ($20$ seconds).
  - Resolves instruments against official Dhan scrip master.
  - Validates pricing, calculates tick-aligned limit prices and lot sizing.
  - Executes risk checks (max exposure, daily loss, order limits).
  - Emits structured evaluation logs to the configured Telegram Log Group.
  - Submits **zero** orders to the broker. Records zero simulated executions in the database.
- **Behavior in LIVE Mode**:
  - Executes full validation pipeline.
  - Places actual LIMIT orders via Dhan API v2.
  - Enforces Dhan static IP binding.
  - Manages fills, pending order timeouts, retries, and broker-side Forever OCO protection legs.
- **Mode Switching Safety Rule (Confirmed by Udit)**: When switching `LIVE` $\rightarrow$ `TEST`, existing open positions **remain untouched** under broker-side Forever OCO protection.

### 2.2 Product Active / Inactive Toggles
- `EQUITY_ACTIVE=true|false`: Enables/disables processing of equity cash signals.
- `FNO_ACTIVE=true|false`: Enables/disables processing of stock option signals.
- *Note*: These toggles determine product capability, not exchange trading sessions or AMO schedules.

---

## 3. Signal Ingestion & Filtering Rules

### 3.1 Channel Monitoring
- Monitors configured channel IDs loaded from protected environment variables.
- One designated channel is the `TEST_GROUP_ID` used for operator verification.
- Reconnection resilience: Telethon/Pyrogram client with automatic reconnection and session preservation.
- **Backlog on Restart Policy (Confirmed by Udit)**: All backlog messages arriving while the bot was offline or restarting are **strictly ignored**.

### 3.2 Gatekeepers & Message Hygiene
1. **Message Age Filter**:
   - Rejects any message where $\Delta t = (t_{\text{received}} - t_{\text{message}}) > 60\text{ seconds}$.
   - Timestamp evaluated against synchronized NTP clock (Asia/Kolkata).
2. **Edits & Deletions**:
   - Telegram message edits or deletions occurring after initial signal parsing/order placement are strictly ignored.
3. **Index Trade Blocker**:
   - Hard rejection of index underlyings: `NIFTY`, `BANKNIFTY`, `FINNIFTY`, `MIDCPNIFTY`, `SENSEX`, `BANKEX`.
   - The bot trades **only single-stock equities and single-stock options**.
4. **Direction Blocker (BUY-Only Initial Phase)**:
   - Initial phase accepts **BUY / Entry** signals only.
   - Messages indicating `SELL`, `SHORT`, `EXIT`, `BOOK PROFIT`, `SQUARE OFF`, `SL HIT`, or `TARGET HIT` are parsed, categorized as `EXIT_SIGNAL`, logged for audit, but **never submitted as broker orders**.
5. **Chatter & Ambiguity Filter**:
   - Rejects messages exceeding 300 characters.
   - Rejects messages with multiple distinct stock symbols.
   - Rejects future planning/analysis commentary (e.g., "watch for tomorrow", "support near", "holding").
6. **Cross-Group Deduplication Window**:
   - Window duration: **20 seconds**.
   - **Duplicate Key**: Normalized Stock Underlying Symbol (e.g., `TATAMOTORS`).
   - If a signal for `TATAMOTORS` arrives from Group A, any signal for `TATAMOTORS` arriving within 20 seconds from Group B (even with differing strikes, expiries, or price bounds) is flagged as a duplicate and ignored.
   - Re-entry signals for the same stock arriving *outside* the 20-second window are permitted for evaluation.

### 3.3 Rule Precedence & AI Fallback Boundary
1. **Deterministic Local Regex First**: All incoming text is evaluated against pre-compiled regex patterns (< 1 ms latency).
2. **Deterministic Validation is Supreme**: If a deterministic rule rejects a message (e.g., index trade, age > 60s, invalid price, option writing), this decision is **final and cannot be overridden by AI**.
3. **AI Fallback (Recheck Only)**:
   - AI (e.g., Gemini) is invoked *only* when a message has clear trading intent but non-standard formatting that misses regex extraction.
   - AI extracts candidate symbols/prices into strongly typed schemas (`Pydantic`).
   - The extracted candidate must re-pass all deterministic risk, age, index, and instrument validation before being accepted.
   - AI cannot automatically modify or propose production rules without explicit human approval.

---

## 4. Product Execution Rules

### 4.1 Equity Execution (Cash / Delivery / MTF)
- **Product Type**: Operator selects `CNC` (Cash Delivery) or `MTF` (Margin Trading Facility) during VM setup.
- **Order Type**: Strictly `LIMIT` orders.
- **Price Calculation**:
  - Entry price is derived from validated Current Market Price (CMP) + operator setup percentage ($P_{\text{offset}}$ entered in setup defaults):
    $$\text{Limit Price} = \text{CMP} \times (1 + P_{\text{offset}} / 100)$$
  - Rounded to nearest exchange tick size (₹0.05).
  - **Quote Staleness Limit (Confirmed by Udit)**: Maximum quote age is **2.0 seconds**. Stale quotes abort order generation.
- **Sizing**:
  - Mode A: Fixed share quantity (`EQUITY_FIXED_QTY`).
  - Mode B: Capital budget (`EQUITY_BUDGET_INR`):
    $$\text{Quantity} = \lfloor \text{EQUITY\_BUDGET\_INR} / \text{Limit Price} \rfloor$$
  - **Rejection Rule**: If calculated quantity $< 1$ share, the signal is rejected with reason `INSUFFICIENT_BUDGET_FOR_MIN_SIZE`.

### 4.2 Stock Options Execution (F&O)
- **Underlying**: Single-stock options only (NFO / BFO). Index options strictly rejected.
- **Direction**: **BUY ONLY** (Long Call / Long Put). Option writing/selling is strictly prohibited.
- **Price Requirement**: Explicit entry price or range is mandatory in the signal.
- **Limit Price Ceiling**:
  - BUY LIMIT placed with an upper entry bound markup of $+4\%$ ($1.04\times$ upper entry bound):
    $$\text{Ceiling} = \text{Signal Entry Max} \times 1.04$$
  - Aligned to contract tick size (₹0.05) **without exceeding** the calculated ceiling:
    $$\text{Limit Price} = \lfloor \text{Ceiling} / \text{Tick} \rfloor \times \text{Tick}$$
- **Expiry Resolution**:
  - If expiry month is omitted in signal: defaults to **current calendar month contract**.
  - Contract must be unique and actively listed in Dhan's instrument master. If no unique contract is found, reject.
- **Sizing**:
  - Mode A: Fixed lots (`FNO_FIXED_LOTS`).
  - Mode B: Capital budget (`FNO_CAPITAL_PER_TRADE`):
    $$\text{Lots} = \lfloor \text{FNO\_CAPITAL\_PER\_TRADE} / (\text{Lot Size} \times \text{Limit Price}) \rfloor$$
  - If `FNO_MAX_LOTS` is configured ($> 0$), cap lots to `min(Lots, FNO_MAX_LOTS)`.
  - **Rejection Rule**: If affordable lots $< 1$, reject with reason `INSUFFICIENT_BUDGET_FOR_MIN_LOT`.
- **Futures**: Strictly disabled until explicit specification is provided.

---

## 5. Protection & Order Lifecycle

### 5.1 Stop Loss & Target Management
- **Source of Levels**: Configured globally at setup (default **3% Stop Loss** and **3% Target**).
- **Signal Targets / SL Ignored**: Target and SL levels stated inside Telegram messages are informational only and strictly ignored for execution.
- **Executed Price Basis**:
  - SL and Target are calculated strictly from the **actual traded fill price** reported by Dhan (`averageTradedPrice`), never from the signal price or initial limit order price:
    $$\text{SL Trigger} = \text{Executed Price} \times (1 - \text{SL\_PCT} / 100)$$
    $$\text{Target Price} = \text{Executed Price} \times (1 + \text{TARGET\_PCT} / 100)$$
  - Tick-aligned: SL rounded down to tick; Target rounded up to tick.
- **Placement Mechanism**:
  - Placed via Dhan Forever Orders in OCO (One-Cancels-Other) mode **only after the parent buy order is fully or partially executed**.
  - Partial fills protected chunk-by-chunk for the executed quantity.
  - Over-exit prevention: System tracks executed quantities vs protection leg quantities in durable storage.

### 5.2 Pending Order Timeouts & Retry Policy (Confirmed by Udit)
- If an entry limit order remains unfilled after timeout:
  - System polls Dhan order status to verify filled vs unfilled quantity.
  - Cancels remaining open order quantity.
  - Verifies cancellation confirmation from broker before taking further action.
  - **Retry Timeout**: **2.0 seconds**. One retry for the unfilled balance is permitted only after explicit cancellation confirmation.

---

## 6. Risk Controls & Safety Bounds

The system evaluates risk limits prior to placing any live order:
1. `MAX_ORDER_AMOUNT_INR`: Maximum permissible value for any single order.
2. `MIN_TRADE_FUNDS_INR`: Required available ledger balance before order entry.
3. **Insufficient Funds Policy (Confirmed by Udit)**: **HARD REJECT**. If available funds are below trade cost, reject immediately without scaling down.
4. `MAX_DAILY_LOSS_INR`: Daily realized + unrealized loss circuit breaker.
5. `MAX_TOTAL_EXPOSURE_INR`: Maximum cumulative open position value.
6. `MAX_OPEN_POSITIONS`: Maximum simultaneous open positions.
7. `MAX_DAILY_TRADES`: Hard cap on total trades per day.
8. `EMERGENCY_STOP`:
   - Admin command that immediately blocks all new signal processing.
   - Cancels all pending unfilled entry orders.
   - Continues monitoring and protecting filled positions.
   - Exposes explicit manual close/exit trigger for operator Udit.

---

## 7. Daily Schedule & Health Check (08:00 IST)

At 08:00 Asia/Kolkata on working days:
1. **Dhan Authentication**:
   - Refreshes API Access Token using Dhan's supported TOTP / API Key flow.
   - Saves token to local protected file with `0600` permissions.
2. **Subsystem Verification**:
   - Tests Dhan REST API connectivity (read-only profile & funds query).
   - Verifies Telegram listener connection and active channel subscriptions.
   - Checks local database integrity and pending reconciliations.
   - Downloads/refreshes the latest Dhan scrip master CSV.
3. **Log Group Notification**:
   - Posts comprehensive readiness report to Telegram Log Group.
   - Alerts operator immediately on any token or connection failure.
   - Re-authentication failure semantics: Alert operator, keep bot in safe standby, never trade blindly without valid auth.

---

## 8. Catalog of Resolved Decisions (Confirmed by Operator Udit)

| # | Topic | Operator Decision | Operational Behavior |
|---|---|---|---|
| **1** | Current Market Price Max Age | **2.0 seconds** | Quotes older than 2.0s are rejected as stale. |
| **2** | Partial Fill Retry & Timeout | **2.0 seconds timeout** | Cancels unfilled remainder; 1 retry allowed within 2.0s timeout. |
| **3** | Equity Limit Markup % | **Setup defaults** | Configured in setup defaults (default 0.5% in `.env`). |
| **4** | Insufficient Funds Behavior | **Hard Reject** | If ledger funds < cost, immediately reject; no size scaling. |
| **5** | Mode Switch Position Policy | **Remain Untouched** | Switching LIVE $\rightarrow$ TEST leaves positions untouched under Forever OCO. |
| **6** | Telegram Backlog on Restart | **Ignored** | Backlog messages during downtime/restart are ignored. |
| **7** | Real Signal Samples | *Pending from Udit* | Awaiting 5–10 real channel sample messages for parser fine-tuning. |
| **8** | Futures Trading Scope | **Disabled** | Single-stock futures remain disabled (`FUTURES_ENABLED=false`). |
