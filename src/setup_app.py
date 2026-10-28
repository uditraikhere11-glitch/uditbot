"""
Standalone Setup Web App.
Renders the exact dark-themed visual setup interface from the reference specification (image_8182d4.png).
Runs locally via FastAPI & Uvicorn.
"""

import os
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
import uvicorn

app = FastAPI(title="UditBot Setup System")

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Sapphire Bot Setup</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Inter', -apple-system, sans-serif; }
    body {
      background-color: #0b0f19;
      color: #f1f5f9;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 30px 15px;
    }
    .container {
      width: 100%;
      max-width: 780px;
      background: #111726;
      border: 1px solid #1e293b;
      border-radius: 20px;
      padding: 40px;
      box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.5);
    }
    .header { text-align: center; margin-bottom: 35px; }
    .icon-badge {
      width: 60px;
      height: 60px;
      background: linear-gradient(135deg, #1d4ed8, #3b82f6);
      border-radius: 16px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      margin-bottom: 18px;
      box-shadow: 0 10px 20px -5px rgba(59, 130, 246, 0.4);
    }
    .icon-badge svg { width: 32px; height: 32px; fill: white; }
    h1 { font-size: 28px; font-weight: 700; color: #ffffff; margin-bottom: 8px; }
    p.subtitle { font-size: 14px; color: #94a3b8; }
    
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 30px; }
    @media (max-width: 640px) { .grid { grid-template-columns: 1fr; } }
    
    .card {
      background: #161f33;
      border: 1px solid #243048;
      border-radius: 14px;
      padding: 22px;
    }
    .card-title {
      display: flex;
      align-items: center;
      gap: 10px;
      font-size: 15px;
      font-weight: 600;
      color: #f8fafc;
      margin-bottom: 18px;
    }
    .card-title svg { width: 18px; height: 18px; color: #38bdf8; }
    
    .form-group { margin-bottom: 16px; }
    .form-group:last-child { margin-bottom: 0; }
    label { display: block; font-size: 12px; font-weight: 500; color: #94a3b8; margin-bottom: 6px; }
    
    .input-wrap {
      position: relative;
      display: flex;
      align-items: center;
      background: #0d1424;
      border: 1px solid #243048;
      border-radius: 8px;
      padding: 10px 14px;
      transition: border-color 0.2s;
    }
    .input-wrap:focus-within { border-color: #3b82f6; }
    .input-wrap svg { width: 16px; height: 16px; color: #64748b; margin-right: 10px; flex-shrink: 0; }
    input[type="text"], input[type="password"], input[type="number"] {
      background: transparent;
      border: none;
      outline: none;
      color: #ffffff;
      font-size: 13px;
      width: 100%;
    }
    input::placeholder { color: #475569; }
    
    .toggle-card {
      background: #0d1424;
      border: 1px solid #243048;
      border-radius: 10px;
      padding: 14px 16px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-top: 10px;
    }
    .toggle-info h4 { font-size: 13px; font-weight: 600; color: #ffffff; }
    .toggle-info p { font-size: 11px; color: #64748b; margin-top: 2px; }
    
    .switch {
      position: relative;
      display: inline-block;
      width: 44px;
      height: 24px;
    }
    .switch input { opacity: 0; width: 0; height: 0; }
    .slider {
      position: absolute; cursor: pointer; top: 0; left: 0; right: 0; bottom: 0;
      background-color: #334155; transition: .3s; border-radius: 24px;
    }
    .slider:before {
      position: absolute; content: ""; height: 18px; width: 18px; left: 3px; bottom: 3px;
      background-color: white; transition: .3s; border-radius: 50%;
    }
    input:checked + .slider { background-color: #3b82f6; }
    input:checked + .slider:before { transform: translateX(20px); }
    
    .btn-submit {
      width: 100%;
      background: #2563eb;
      color: white;
      border: none;
      border-radius: 10px;
      padding: 14px;
      font-size: 15px;
      font-weight: 600;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 10px;
      transition: background 0.2s, transform 0.1s;
    }
    .btn-submit:hover { background: #1d4ed8; }
    .btn-submit:active { transform: scale(0.99); }
    
    .banner {
      background: rgba(16, 185, 129, 0.15);
      border: 1px solid #10b981;
      color: #34d399;
      padding: 12px 16px;
      border-radius: 8px;
      font-size: 13px;
      margin-bottom: 24px;
      text-align: center;
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="icon-badge">
        <svg viewBox="0 0 24 24"><path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5"/></svg>
      </div>
      <h1>Sapphire Bot Setup</h1>
      <p class="subtitle">Configure your algorithmic trading parameters and connect to Dhan.</p>
    </div>

    <!--STATUS_BANNER-->

    <form method="POST" action="/setup">
      <div class="grid">
        <!-- Exchange Credentials -->
        <div class="card">
          <div class="card-title">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
            Exchange Credentials
          </div>
          <div class="form-group">
            <label>Dhan Client ID</label>
            <div class="input-wrap">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>
              <input type="text" name="dhan_client_id" value="<!--DHAN_CLIENT_ID-->" placeholder="e.g. 1100XXXXXX" required>
            </div>
          </div>
          <div class="form-group">
            <label>API Access Token</label>
            <div class="input-wrap">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 2l-2 2m-7.61 7.61a5.5 5.5 0 1 1-7.778 7.778 5.5 5.5 0 0 1 7.777-7.777zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4"/></svg>
              <input type="password" name="dhan_access_token" value="<!--DHAN_ACCESS_TOKEN-->" placeholder="eyJhbGciOiJIUzI1NiIsInR...">
            </div>
          </div>
        </div>

        <!-- Bot Configuration -->
        <div class="card">
          <div class="card-title">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>
            Bot Configuration
          </div>
          <div class="form-group">
            <label>Bot Instance Name</label>
            <div class="input-wrap">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="3" width="20" height="14" rx="2" ry="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/></svg>
              <input type="text" name="instance_name" value="<!--INSTANCE_NAME-->" placeholder="DhanAlgoTrading">
            </div>
          </div>
          <div class="form-group">
            <label>Monitoring Group ID (Alerts)</label>
            <div class="input-wrap">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>
              <input type="text" name="log_group_id" value="<!--LOG_GROUP_ID-->" placeholder="e.g. TG-GRP-994 or -100XXXXXXXXXX">
            </div>
          </div>
        </div>

        <!-- Risk Management -->
        <div class="card">
          <div class="card-title">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><polyline points="9 12 11 14 15 10"/></svg>
            Risk Management
          </div>
          <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px;">
            <div class="form-group">
              <label>Max Drawdown (%)</label>
              <div class="input-wrap">
                <input type="number" step="0.1" name="max_drawdown_pct" value="<!--MAX_DRAWDOWN_PCT-->">
              </div>
            </div>
            <div class="form-group">
              <label>Daily Trade Limit</label>
              <div class="input-wrap">
                <input type="number" name="daily_trade_limit" value="<!--DAILY_TRADE_LIMIT-->">
              </div>
            </div>
          </div>
        </div>

        <!-- Execution Mode -->
        <div class="card">
          <div class="card-title">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
            Execution Mode
          </div>
          <p style="font-size: 11px; color: #94a3b8; line-height: 1.4;">
            Toggle between TEST mode (validation & reporting without broker orders) and LIVE exchange execution.
          </p>
          <div class="toggle-card">
            <div class="toggle-info">
              <h4>TEST Mode Active</h4>
              <p>Safe signal checking; zero broker orders.</p>
            </div>
            <label class="switch">
              <input type="checkbox" name="is_live_mode" value="true" <!--LIVE_CHECKED-->>
              <span class="slider"></span>
            </label>
          </div>
        </div>
      </div>

      <button type="submit" class="btn-submit">
        Initialize Sapphire Bot <span>&rarr;</span>
      </button>
    </form>
  </div>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def get_setup_page(saved: bool = False):
    from src.config import AppConfig
    cfg = AppConfig.from_env()

    banner = ""
    if saved:
        banner = '<div class="banner">✅ Configuration updated & saved to .env (chmod 0600)</div>'

    html = (
        HTML_TEMPLATE.replace("<!--STATUS_BANNER-->", banner)
        .replace("<!--DHAN_CLIENT_ID-->", cfg.dhan_client_id or "")
        .replace("<!--DHAN_ACCESS_TOKEN-->", cfg.dhan_access_token or "")
        .replace("<!--INSTANCE_NAME-->", getattr(cfg, "instance_name", "DhanAlgoTrading"))
        .replace("<!--LOG_GROUP_ID-->", str(cfg.log_group_id or ""))
        .replace("<!--MAX_DRAWDOWN_PCT-->", str(getattr(cfg, "max_drawdown_pct", 5.0)))
        .replace("<!--DAILY_TRADE_LIMIT-->", str(cfg.max_daily_trades or 20))
        .replace("<!--LIVE_CHECKED-->", "checked" if cfg.trading_mode.value == "LIVE" else "")
    )
    return HTMLResponse(content=html)


@app.post("/setup", response_class=HTMLResponse)
def handle_setup_submission(
    dhan_client_id: str = Form(...),
    dhan_access_token: str = Form(""),
    instance_name: str = Form("DhanAlgoTrading"),
    log_group_id: str = Form(""),
    max_drawdown_pct: float = Form(5.0),
    daily_trade_limit: int = Form(20),
    is_live_mode: bool = Form(False),
):
    trading_mode = "LIVE" if is_live_mode else "TEST"
    
    env_content = f"""# Auto-generated by Sapphire Setup Web App
TRADING_MODE={trading_mode}
EQUITY_ACTIVE=true
FNO_ACTIVE=true
BOT_INSTANCE_NAME={instance_name}

DHAN_CLIENT_ID={dhan_client_id}
DHAN_ACCESS_TOKEN={dhan_access_token}
DHAN_TOKEN_FILE=~/.dhan_token

LOG_GROUP_ID={log_group_id}
IGNORE_TELEGRAM_BACKLOG_ON_RESTART=true

MAX_DRAWDOWN_PCT={max_drawdown_pct}
MAX_DAILY_TRADES={daily_trade_limit}
MAX_DAILY_LOSS_INR=15000
MAX_ORDER_AMOUNT_INR=50000
MIN_TRADE_FUNDS_INR=10000
INSUFFICIENT_FUNDS_POLICY=HARD_REJECT
MODE_SWITCH_POSITION_POLICY=REMAIN_UNTOUCHED

EQUITY_PRODUCT_TYPE=CNC
EQUITY_SIZING_MODE=BUDGET
EQUITY_BUDGET_INR=25000
EQUITY_PRICE_MARKUP_PCT=0.5

FNO_SIZING_MODE=BUDGET
FNO_CAPITAL_PER_TRADE=20000
FNO_MAX_LOTS=3

SL_PCT=3.0
TARGET_PCT=3.0
SL_LIMIT_BUFFER_PCT=1.0
MAX_QUOTE_AGE_SEC=2.0
PARTIAL_FILL_RETRY_TIMEOUT_SEC=2.0
MAX_MESSAGE_AGE_SEC=60.0
DEDUP_WINDOW_SEC=20.0
"""
    fd = os.open(".env", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(env_content)

    return get_setup_page(saved=True)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8080)
