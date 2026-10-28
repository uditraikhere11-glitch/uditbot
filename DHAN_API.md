# DhanHQ API v2 Integration Specification

## 1. Overview & Official API Specs

The trading bot interfaces with the official **DhanHQ API Version 2.0** (`https://api.dhan.co/v2`). DhanHQ v2 provides high-throughput, low-latency REST and WebSocket endpoints for Indian equity cash, derivatives, portfolio management, and market quotes.

### Regulatory & Infrastructure Mandates
- **Static External IP Mandate**: Dhan enforces that all order placement and modification API calls originate from a registered, static IP address configured in the trader's Dhan Web Portal. The GCE VM hosting this bot must be provisioned with a **static external IP address** and whitelisted in Udit's Dhan account before live trading can occur.
- **Token Validity**: Dhan Access Tokens are valid for a maximum of 24 hours. The bot executes an automated daily token refresh routine at 08:00 Asia/Kolkata using TOTP/API credentials.

---

## 2. Authentication & Token Management

### 2.1 Token Storage & Permissions
- Token File Path: `~/.dhan_token` (overridable via `DHAN_TOKEN_FILE`).
- File Permissions: Strictly enforced at `0600` (read/write by owner only).
- Atomicity: File writes utilize a staging temporary file (`.dhan_token.tmp`) followed by an atomic rename (`os.replace`) guarded by file locks (`fcntl.flock`).

### 2.2 Token Refresh Flow (Daily at 08:00 IST)
Dhan allows automated token generation using the client ID, pin, and Time-based One-Time Password (TOTP) secret key generated in the Dhan web portal under *My Profile $\rightarrow$ Access DhanHQ APIs*.

```
+------------------+          +-----------------------+          +--------------------+
|  Bot Daily Cron  |  08:00   | Generate TOTP Code    |  POST    | Dhan Auth API      |
|  (Asia/Kolkata)  | -------> | (pyotp from Secret)   | -------> | /v2/RenewToken     |
+------------------+          +-----------------------+          +--------------------+
                                                                            |
                                                                     200 OK | Token
                                                                            v
                                                                 +--------------------+
                                                                 | Save to file 0600  |
                                                                 | Notify Log Group   |
                                                                 +--------------------+
```

### 2.3 Authentication Error Handling
Dhan signals authentication expiration or revocation via:
- HTTP Status Code: `401 Unauthorized` or `403 Forbidden`.
- Error Body: Strings containing `"dh-901"`, `"dh-906"`, `"invalid token"`, or `"unauthorized"`.
- Action: When an auth error occurs, the bot invalidates the cached memory token, re-reads the disk file (in case another process refreshed it), and if still stale, alerts the operator via the Telegram Log Group. It **never retries blindly** in a tight loop.

---

## 3. Scrip Master & Security Resolution

Dhan maintains daily downloadable CSV scrip masters containing detailed contract specifications across all segments:
- **Detailed Master URL**: `https://images.dhan.co/api-data/api-scrip-master-detailed.csv`
- **Compact Master URL**: `https://images.dhan.co/api-data/api-scrip-master.csv`

### 3.1 Mapping Schema
| Field Name | Description | Example Values |
|---|---|---|
| `SEM_EXM_EXCH_ID` | Exchange Identifier | `NSE`, `BSE` |
| `SEM_SEGMENT` | Market Segment | `E` (Equity), `D` (Derivatives) |
| `SEM_INSTRUMENT_NAME` | Instrument Category | `EQUITY`, `OPTSTK`, `OPTIDX`, `FUTSTK` |
| `SEM_TRADING_SYMBOL` | Exchange Trading Symbol | `TATAMOTORS-EQ`, `RELIANCE-OCT2026-2500-CE` |
| `SEM_SMST_SECURITY_ID` | Unique Numeric Security ID | `3456`, `52411` |
| `SEM_EXPIRY_DATE` | Expiry Date (YYYY-MM-DD HH:MM:SS) | `2026-10-29 15:30:00` |
| `SEM_STRIKE_PRICE` | Strike Price (Options) | `2500.0` |
| `SEM_OPTION_TYPE` | Option Flavor | `CE`, `PE` |
| `SEM_LOT_UNITS` | Market Lot Size | `1` (Equity), `250`, `650` (F&O) |
| `SEM_TICK_SIZE` | Minimum Price Fluctuation | `0.05` |

### 3.2 Index Filtering Guarantee
During scrip master ingest, instruments matching `SEM_INSTRUMENT_NAME == "OPTIDX"` or underlying symbols in `{"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"}` are tagged as index instruments. Any incoming signal resolving to these instruments is rejected immediately.

---

## 4. Market Data & Quote APIs

### 4.1 Real-Time LTP Endpoint
To validate prices and compute limit orders for equity and stock options, the bot queries Dhan's real-time snapshot endpoint:

- **Endpoint**: `POST https://api.dhan.co/v2/marketfeed/ltp`
- **Rate Limit**: Maximum 1 request per second (up to 1,000 instruments per payload).
- **Request Headers**:
  - `access-token: <DHAN_ACCESS_TOKEN>`
  - `client-id: <DHAN_CLIENT_ID>`
  - `Content-Type: application/json`
- **Request Body**:
  ```json
  {
    "instruments": [
      {
        "exchangeSegment": "NSE_EQ",
        "securityId": "11536"
      }
    ]
  }
  ```
- **Response Format**:
  ```json
  {
    "status": "success",
    "data": {
      "NSE_EQ": {
        "11536": {
          "last_price": 808.45
        }
      }
    }
  }
  ```

### 4.2 Stale Quote Safeguard
Quotes fetched from the market feed are timestamped in memory. If a quote's age exceeds the configured staleness threshold (e.g., > 3.0 seconds during market hours), order generation is aborted.

---

## 5. Order Management Endpoints

### 5.1 Place Order
- **Endpoint**: `POST https://api.dhan.co/v2/orders`
- **Payload Specification**:
  ```json
  {
    "dhanClientId": "1000000000",
    "correlationId": "BOT-EQ-20261027-001",
    "transactionType": "BUY",
    "exchangeSegment": "NSE_EQ",
    "productType": "CNC",
    "orderType": "LIMIT",
    "validity": "DAY",
    "tradingSymbol": "TCS-EQ",
    "securityId": "11536",
    "quantity": 10,
    "price": 3520.50,
    "triggerPrice": 0.0,
    "disclosedQuantity": 0,
    "afterMarketOrder": false
  }
  ```
- **Product Types**:
  - `CNC`: Cash and Carry (Equity Delivery).
  - `MTF`: Margin Trading Facility (Equity Leveraged Delivery).
  - `INTRADAY`: Day trading (if enabled).
  - Derivatives (Stock Options): Use `MARGIN` or `INTRADAY` with segment `NSE_FNO`.

### 5.2 Order Status & Fills
- **Endpoint**: `GET https://api.dhan.co/v2/orders/{orderId}`
- **Statuses**:
  - `TRANSIT`: Order received by broker, routing to exchange.
  - `PENDING`: Order resting on exchange order book.
  - `TRADED`: Order fully executed.
  - `PARTIALLY_TRADED`: Order partially executed, balance resting.
  - `CANCELLED`: Order cancelled by user or system.
  - `REJECTED`: Order rejected by broker risk management or exchange.
  - `EXPIRED`: Order expired at market close.
- **Traded Details**: `GET https://api.dhan.co/v2/trades/{orderId}` retrieves execution records including `tradedPrice`, `tradedQuantity`, and `exchangeTradeId`.

### 5.3 Order Cancellation
- **Endpoint**: `DELETE https://api.dhan.co/v2/orders/{orderId}`
- Used to cancel resting unfilled quantities when an entry order reaches its timeout limit.

---

## 6. Forever Orders (OCO Protection Legs)

Once an entry buy order executes (status `TRADED` or `PARTIALLY_TRADED`), the bot reads the official `averageTradedPrice` and immediately places a Dhan **Forever Order** in **OCO** mode for the exact filled quantity.

- **Endpoint**: `POST https://api.dhan.co/v2/forever/orders`
- **Payload Specification**:
  ```json
  {
    "dhanClientId": "1000000000",
    "correlationId": "BOT-OCO-20261027-001",
    "orderFlag": "OCO",
    "transactionType": "SELL",
    "exchangeSegment": "NSE_EQ",
    "productType": "CNC",
    "securityId": "11536",
    "quantity": 10,
    "price": 3626.10,
    "triggerPrice": 3414.90,
    "targetPrice": 3626.10,
    "stopLossPrice": 3414.90
  }
  ```
- **Safety Properties**:
  - Forever orders persist on Dhan's servers independently of the bot VM.
  - If target price is hit, Dhan places a limit sell order and cancels the stop-loss leg.
  - If stop-loss trigger is hit, Dhan places a market/limit sell order and cancels the target leg.

---

## 7. Account Balances & Margin Inquiries

- **Funds Endpoint**: `GET https://api.dhan.co/v2/funds`
  - Returns `availabelBalance`, `sodLimit`, `collateralAmount`, `utilizedAmount`.
  - Polled prior to live order generation to enforce `MIN_TRADE_FUNDS_INR` and verify sufficient liquidity.
- **Margin Calculator Endpoint**: `POST https://api.dhan.co/v2/margincalculator`
  - Returns required margin for equity delivery, MTF, or options buying.
