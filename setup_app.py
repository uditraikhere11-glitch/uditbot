"""
Audit Bot Setup Dashboard.
Visual web configuration interface for Audit bot (single-operator algorithmic trading system).
Enables operators to inspect, configure, and switch trading parameters anywhere, anytime without SSH:
- Operator Name & Bot Name (Audit bot)
- Master Operating Mode (TEST vs LIVE) & Active Product Segments (Equity, F&O)
- Telegram Ingestion (API ID, API Hash, Monitored Channels, Log Group, Test Channel)
- DhanHQ Exchange Auth (Client ID, PIN, TOTP Secret for daily 08:00 refresh, Access Token)
- Capital & Position Sizing (Equity Budget, Product Type CNC/MTF, Options Budget, Max Lots)
- Risk Management Controls (Max Order Amount, Min Funds, Daily Loss Limit, Max Trades, Exposure, Max Positions)
- Protection & Timing Parameters (SL %, Target %, Buffer %, Signal Age, Dedup Window)
Includes post-save visual confirmation card and step-by-step next operational actions.
Safely reads from and writes to .env with chmod 0600 file permissions.
"""

import os
import sys
from typing import Optional
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
import uvicorn

from src.config import AppConfig

app = FastAPI(title="Audit bot - Setup Dashboard")

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Audit bot - Setup Dashboard</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Inter', -apple-system, sans-serif; }
    body {
      background-color: #0b0f19;
      color: #f1f5f9;
      min-height: 100vh;
      display: flex;
      justify-content: center;
      padding: 40px 16px;
    }
    .container {
      width: 100%;
      max-width: 960px;
      background: #111726;
      border: 1px solid #1e293b;
      border-radius: 20px;
      padding: 36px;
      box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.6);
    }
    .header { text-align: center; margin-bottom: 28px; }
    .icon-badge {
      width: 58px;
      height: 58px;
      background: linear-gradient(135deg, #1d4ed8, #3b82f6);
      border-radius: 16px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      margin-bottom: 16px;
      box-shadow: 0 10px 20px -5px rgba(59, 130, 246, 0.4);
    }
    .icon-badge svg { width: 32px; height: 32px; fill: white; }
    h1 { font-size: 28px; font-weight: 700; color: #ffffff; margin-bottom: 6px; }
    p.subtitle { font-size: 14px; color: #94a3b8; }
    
    /* Confirmation Banner */
    .banner-success {
      background: rgba(16, 185, 129, 0.12);
      border: 1px solid #10b981;
      border-radius: 12px;
      padding: 18px 22px;
      margin-bottom: 26px;
    }
    .banner-header {
      display: flex;
      align-items: center;
      gap: 10px;
      color: #34d399;
      font-size: 15px;
      font-weight: 700;
      margin-bottom: 12px;
    }
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 10px;
      margin-top: 10px;
    }
    .summary-chip {
      background: rgba(15, 23, 42, 0.6);
      border: 1px solid rgba(16, 185, 129, 0.25);
      border-radius: 8px;
      padding: 8px 12px;
    }
    .chip-label { font-size: 11px; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.5px; }
    .chip-value { font-size: 13px; font-weight: 600; color: #f8fafc; margin-top: 2px; }
    
    /* Next Steps Card */
    .next-steps-card {
      background: rgba(30, 41, 59, 0.5);
      border: 1px solid #334155;
      border-radius: 14px;
      padding: 20px;
      margin-bottom: 24px;
    }
    .next-steps-title {
      font-size: 15px;
      font-weight: 600;
      color: #f1f5f9;
      display: flex;
      align-items: center;
      gap: 8px;
      margin-bottom: 14px;
    }
    .steps-list {
      display: flex;
      flex-direction: column;
      gap: 12px;
    }
    .step-item {
      display: flex;
      gap: 12px;
      align-items: flex-start;
    }
    .step-num {
      width: 24px;
      height: 24px;
      background: #2563eb;
      color: white;
      border-radius: 50%;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 12px;
      font-weight: 700;
      flex-shrink: 0;
      margin-top: 1px;
    }
    .step-content h5 { font-size: 13px; font-weight: 600; color: #f8fafc; margin-bottom: 2px; }
    .step-content p { font-size: 12px; color: #94a3b8; line-height: 1.4; }
    .step-code {
      display: inline-block;
      background: #0d1321;
      border: 1px solid #1e293b;
      padding: 2px 6px;
      border-radius: 4px;
      font-family: monospace;
      color: #38bdf8;
      font-size: 11px;
      margin-top: 3px;
    }

    .section-card {
      background: #161f33;
      border: 1px solid #243049;
      border-radius: 14px;
      padding: 22px;
      margin-bottom: 22px;
    }
    .section-title {
      font-size: 16px;
      font-weight: 600;
      color: #38bdf8;
      display: flex;
      align-items: center;
      gap: 10px;
      margin-bottom: 16px;
      padding-bottom: 10px;
      border-bottom: 1px solid #222f46;
    }
    .section-title svg { width: 20px; height: 20px; stroke: #38bdf8; }
    
    .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    .grid-3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; }
    @media (max-width: 768px) {
      .grid-2, .grid-3 { grid-template-columns: 1fr; }
      .container { padding: 20px; }
    }
    
    .form-group { margin-bottom: 14px; }
    .form-group:last-child { margin-bottom: 0; }
    label {
      display: block;
      font-size: 12px;
      font-weight: 500;
      color: #cbd5e1;
      margin-bottom: 6px;
    }
    .help-text {
      font-size: 11px;
      color: #64748b;
      margin-top: 4px;
    }
    .input-wrap {
      position: relative;
      display: flex;
      align-items: center;
    }
    .input-wrap input, .input-wrap select {
      width: 100%;
      background: #0d1321;
      border: 1px solid #26334d;
      border-radius: 8px;
      padding: 10px 14px;
      font-size: 13px;
      color: #f8fafc;
      outline: none;
      transition: border-color 0.2s;
    }
    .input-wrap input:focus, .input-wrap select:focus {
      border-color: #3b82f6;
    }
    .input-wrap select {
      cursor: pointer;
    }
    
    /* Toggle switch */
    .toggle-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 12px 14px;
      background: #0d1321;
      border: 1px solid #26334d;
      border-radius: 10px;
      margin-bottom: 12px;
    }
    .toggle-row:last-child { margin-bottom: 0; }
    .toggle-info h4 { font-size: 13px; font-weight: 600; color: #f1f5f9; margin-bottom: 2px; }
    .toggle-info p { font-size: 11px; color: #94a3b8; }
    
    .switch {
      position: relative;
      display: inline-block;
      width: 44px;
      height: 24px;
      flex-shrink: 0;
    }
    .switch input { opacity: 0; width: 0; height: 0; }
    .slider {
      position: absolute; cursor: pointer; top: 0; left: 0; right: 0; bottom: 0;
      background-color: #334155; transition: .25s; border-radius: 24px;
    }
    .slider:before {
      position: absolute; content: ""; height: 18px; width: 18px; left: 3px; bottom: 3px;
      background-color: white; transition: .25s; border-radius: 50%;
    }
    input:checked + .slider { background-color: #3b82f6; }
    input:checked + .slider:before { transform: translateX(20px); }
    
    .mode-live {
      border-color: #f59e0b;
      background: rgba(245, 158, 11, 0.05);
    }
    
    .btn-submit {
      width: 100%;
      background: linear-gradient(135deg, #2563eb, #1d4ed8);
      color: white;
      border: none;
      border-radius: 12px;
      padding: 16px;
      font-size: 16px;
      font-weight: 600;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 10px;
      margin-top: 24px;
      box-shadow: 0 10px 20px -5px rgba(37, 99, 235, 0.4);
      transition: background 0.2s, transform 0.1s;
    }
    .btn-submit:hover { background: linear-gradient(135deg, #1d4ed8, #1e40af); }
    .btn-submit:active { transform: scale(0.99); }
    
    .badge {
      display: inline-block;
      font-size: 11px;
      font-weight: 700;
      padding: 2px 8px;
      border-radius: 6px;
      text-transform: uppercase;
    }
    .badge-live { background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.4); }
    .badge-test { background: rgba(59, 130, 246, 0.2); color: #60a5fa; border: 1px solid rgba(59, 130, 246, 0.4); }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="icon-badge">
        <svg viewBox="0 0 24 24"><path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5"/></svg>
      </div>
      <h1>Audit bot Setup</h1>
      <p class="subtitle">Sapphire Bot Setup Engine &mdash; Configure trading parameters, DhanHQ credentials, Telegram channels, and risk controls for Audit bot.</p>
    </div>

    <!--STATUS_BANNER-->

    <!--NEXT_STEPS_GUIDE-->

    <form method="POST" action="/setup">
      
      <!-- 1. Master Execution Mode & Segments -->
      <div class="section-card">
        <div class="section-title">
          <svg viewBox="0 0 24 24" fill="none" stroke-width="2"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
          Execution Mode & Segments
        </div>
        
        <div class="toggle-row mode-live">
          <div class="toggle-info">
            <h4>LIVE Trading Mode</h4>
            <p>Switch ON for real DhanHQ exchange order execution. Switch OFF for safe TEST mode (validation & visual cards only).</p>
          </div>
          <label class="switch">
            <input type="checkbox" name="is_live_mode" value="true" <!--LIVE_CHECKED-->>
            <span class="slider"></span>
          </label>
        </div>

        <div class="grid-2" style="margin-top: 12px;">
          <div class="toggle-row">
            <div class="toggle-info">
              <h4>Cash Equities (NSE_EQ)</h4>
              <p>Process cash equity buy signals</p>
            </div>
            <label class="switch">
              <input type="checkbox" name="equity_active" value="true" <!--EQUITY_CHECKED-->>
              <span class="slider"></span>
            </label>
          </div>
          <div class="toggle-row">
            <div class="toggle-info">
              <h4>Stock Options (NSE_FNO)</h4>
              <p>Process stock options (CE/PE) signals</p>
            </div>
            <label class="switch">
              <input type="checkbox" name="fno_active" value="true" <!--FNO_CHECKED-->>
              <span class="slider"></span>
            </label>
          </div>
        </div>
      </div>

      <!-- 2. Bot Configuration & Operator Details -->
      <div class="section-card">
        <div class="section-title">
          <svg viewBox="0 0 24 24" fill="none" stroke-width="2"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
          Bot Configuration & Signal Ingestion
        </div>
        
        <div class="grid-2">
          <div class="form-group">
            <label>Operator / User Name</label>
            <div class="input-wrap">
              <input type="text" name="operator_name" value="<!--OPERATOR_NAME-->" placeholder="e.g. Udit" required>
            </div>
            <p class="help-text">Name of the operator managing and authorizing bot trades</p>
          </div>
          <div class="form-group">
            <label>Bot Instance Name</label>
            <div class="input-wrap">
              <input type="text" name="instance_name" value="<!--INSTANCE_NAME-->" placeholder="Audit bot" required>
            </div>
            <p class="help-text">Displayed on startup headers and Telegram visual log cards</p>
          </div>
        </div>

        <div class="grid-2" style="margin-top: 10px;">
          <div class="form-group">
            <label>Telegram API ID</label>
            <div class="input-wrap">
              <input type="text" name="tg_api_id" value="<!--TG_API_ID-->" placeholder="e.g. 1234567" required>
            </div>
            <p class="help-text">From my.telegram.org</p>
          </div>
          <div class="form-group">
            <label>Telegram API Hash</label>
            <div class="input-wrap">
              <input type="password" name="tg_api_hash" value="<!--TG_API_HASH-->" placeholder="e.g. 0123456789abcdef..." required>
            </div>
            <p class="help-text">From my.telegram.org</p>
          </div>
        </div>

        <div class="grid-2" style="margin-top: 10px;">
          <div class="form-group">
            <label>Monitored Channels (Comma-separated IDs)</label>
            <div class="input-wrap">
              <input type="text" name="monitored_channel_ids" value="<!--MONITORED_CHANNEL_IDS-->" placeholder="-1004417022099, -5100801652" required>
            </div>
            <p class="help-text">Telegram channels from which trade signals are ingested</p>
          </div>
          <div class="form-group">
            <label>Telegram Log Group ID (Visual Card Destination)</label>
            <div class="input-wrap">
              <input type="text" name="log_group_id" value="<!--LOG_GROUP_ID-->" placeholder="e.g. -1004417022099 or -5514889030" required>
            </div>
            <p class="help-text">Where formatted trade verification cards are sent</p>
          </div>
        </div>

        <div class="grid-2" style="margin-top: 10px;">
          <div class="form-group">
            <label>Test Signal Channel ID</label>
            <div class="input-wrap">
              <input type="text" name="test_group_id" value="<!--TEST_GROUP_ID-->" placeholder="e.g. -5100801652">
            </div>
            <p class="help-text">Dedicated channel used for testing bot responses</p>
          </div>
          <div class="form-group">
            <label>Ignore Backlog on Restart</label>
            <div class="input-wrap">
              <select name="ignore_backlog">
                <option value="true" selected>Yes - Process Only Fresh Live Messages</option>
                <option value="false">No - Ingest Historical Backlog</option>
              </select>
            </div>
            <p class="help-text">Prevents stale fills when restarting</p>
          </div>
        </div>
      </div>

      <!-- 3. DhanHQ Exchange Credentials -->
      <div class="section-card">
        <div class="section-title">
          <svg viewBox="0 0 24 24" fill="none" stroke-width="2"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
          Exchange Credentials (DhanHQ v2)
        </div>

        <div class="grid-2">
          <div class="form-group">
            <label>Dhan Client ID</label>
            <div class="input-wrap">
              <input type="text" name="dhan_client_id" value="<!--DHAN_CLIENT_ID-->" placeholder="e.g. 1100XXXXXX" required>
            </div>
            <p class="help-text">10-digit Dhan Client ID</p>
          </div>
          <div class="form-group">
            <label>Dhan 6-Digit PIN</label>
            <div class="input-wrap">
              <input type="password" name="dhan_pin" value="<!--DHAN_PIN-->" placeholder="e.g. 123456">
            </div>
            <p class="help-text">Used for automated daily authentication</p>
          </div>
        </div>

        <div class="grid-2" style="margin-top: 10px;">
          <div class="form-group">
            <label>Dhan TOTP Secret Key</label>
            <div class="input-wrap">
              <input type="password" name="dhan_totp_secret" value="<!--DHAN_TOTP_SECRET-->" placeholder="e.g. JBSWY3DPEHPK3PXP">
            </div>
            <p class="help-text">From Dhan Web Portal (enables 08:00 IST daily automated token refresh)</p>
          </div>
          <div class="form-group">
            <label>Current Access Token (Optional if TOTP configured)</label>
            <div class="input-wrap">
              <input type="password" name="dhan_access_token" value="<!--DHAN_ACCESS_TOKEN-->" placeholder="eyJhbGciOiJIUzI1Ni...">
            </div>
            <p class="help-text">Valid for 24 hours</p>
          </div>
        </div>
      </div>

      <!-- 4. Capital Sizing & Trade Defaults -->
      <div class="section-card">
        <div class="section-title">
          <svg viewBox="0 0 24 24" fill="none" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M16 8h-6a2 2 0 1 0 0 4h4a2 2 0 1 1 0 4H8"/><line x1="12" y1="6" x2="12" y2="8"/><line x1="12" y1="16" x2="12" y2="18"/></svg>
          Capital Sizing & Product Defaults
        </div>

        <div class="grid-3">
          <div class="form-group">
            <label>Equity Budget per Trade (INR)</label>
            <div class="input-wrap">
              <input type="number" step="500" name="equity_budget_inr" value="<!--EQUITY_BUDGET_INR-->" required>
            </div>
            <p class="help-text">Allocated budget per cash equity buy</p>
          </div>
          <div class="form-group">
            <label>Equity Product Type</label>
            <div class="input-wrap">
              <select name="equity_product_type">
                <option value="CNC" <!--CNC_SELECTED-->>CNC (Cash & Carry Delivery)</option>
                <option value="MTF" <!--MTF_SELECTED-->>MTF (Margin Trading Facility)</option>
              </select>
            </div>
            <p class="help-text">Default execution product</p>
          </div>
          <div class="form-group">
            <label>Equity Price Markup (%)</label>
            <div class="input-wrap">
              <input type="number" step="0.1" name="equity_price_markup_pct" value="<!--EQUITY_PRICE_MARKUP_PCT-->" required>
            </div>
            <p class="help-text">Markup added to LTP for limit orders</p>
          </div>
        </div>

        <div class="grid-2" style="margin-top: 10px;">
          <div class="form-group">
            <label>Stock Options Capital per Trade (INR)</label>
            <div class="input-wrap">
              <input type="number" step="500" name="fno_capital_per_trade" value="<!--FNO_CAPITAL_PER_TRADE-->" required>
            </div>
            <p class="help-text">Used to compute whole lots (rejects if &lt;1 lot)</p>
          </div>
          <div class="form-group">
            <label>Options Max Lots Cap</label>
            <div class="input-wrap">
              <input type="number" name="fno_max_lots" value="<!--FNO_MAX_LOTS-->" required>
            </div>
            <p class="help-text">Maximum whole lots per options signal</p>
          </div>
        </div>
      </div>

      <!-- 5. Risk Management & Circuit Breakers -->
      <div class="section-card">
        <div class="section-title">
          <svg viewBox="0 0 24 24" fill="none" stroke-width="2"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><polyline points="9 12 11 14 15 10"/></svg>
          Risk Management & Circuit Breakers
        </div>

        <div class="grid-3">
          <div class="form-group">
            <label>Max Order Amount (INR)</label>
            <div class="input-wrap">
              <input type="number" step="1000" name="max_order_amount_inr" value="<!--MAX_ORDER_AMOUNT_INR-->" required>
            </div>
            <p class="help-text">Hard ceiling per single order</p>
          </div>
          <div class="form-group">
            <label>Min Required Ledger Funds (INR)</label>
            <div class="input-wrap">
              <input type="number" step="1000" name="min_trade_funds_inr" value="<!--MIN_TRADE_FUNDS_INR-->" required>
            </div>
            <p class="help-text">Trading halted if funds drop below</p>
          </div>
          <div class="form-group">
            <label>Max Daily Loss (INR)</label>
            <div class="input-wrap">
              <input type="number" step="1000" name="max_daily_loss_inr" value="<!--MAX_DAILY_LOSS_INR-->" required>
            </div>
            <p class="help-text">Daily circuit breaker threshold</p>
          </div>
        </div>

        <div class="grid-3" style="margin-top: 10px;">
          <div class="form-group">
            <label>Max Daily Trade Count</label>
            <div class="input-wrap">
              <input type="number" name="max_daily_trades" value="<!--MAX_DAILY_TRADES-->" required>
            </div>
            <p class="help-text">Daily order limit cap</p>
          </div>
          <div class="form-group">
            <label>Max Total Portfolio Exposure (INR)</label>
            <div class="input-wrap">
              <input type="number" step="5000" name="max_total_exposure_inr" value="<!--MAX_TOTAL_EXPOSURE_INR-->" required>
            </div>
            <p class="help-text">Aggregate live exposure cap</p>
          </div>
          <div class="form-group">
            <label>Max Concurrent Open Positions</label>
            <div class="input-wrap">
              <input type="number" name="max_open_positions" value="<!--MAX_OPEN_POSITIONS-->" required>
            </div>
            <p class="help-text">Open positions threshold</p>
          </div>
        </div>
      </div>

      <!-- 6. Protection & Timing Parameters -->
      <div class="section-card">
        <div class="section-title">
          <svg viewBox="0 0 24 24" fill="none" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
          Protection & Timing Rules (Forever OCO)
        </div>

        <div class="grid-3">
          <div class="form-group">
            <label>Default Stop Loss (%)</label>
            <div class="input-wrap">
              <input type="number" step="0.1" name="sl_pct" value="<!--SL_PCT-->" required>
            </div>
            <p class="help-text">Calculated from executed fill price</p>
          </div>
          <div class="form-group">
            <label>Default Target (%)</label>
            <div class="input-wrap">
              <input type="number" step="0.1" name="target_pct" value="<!--TARGET_PCT-->" required>
            </div>
            <p class="help-text">Calculated from executed fill price</p>
          </div>
          <div class="form-group">
            <label>SL Limit Buffer (%)</label>
            <div class="input-wrap">
              <input type="number" step="0.1" name="sl_limit_buffer_pct" value="<!--SL_LIMIT_BUFFER_PCT-->" required>
            </div>
            <p class="help-text">Buffer between SL trigger and limit</p>
          </div>
        </div>

        <div class="grid-3" style="margin-top: 10px;">
          <div class="form-group">
            <label>Max Signal Message Age (Sec)</label>
            <div class="input-wrap">
              <input type="number" step="1" name="max_message_age_sec" value="<!--MAX_MESSAGE_AGE_SEC-->" required>
            </div>
            <p class="help-text">Rejects stale historical signals</p>
          </div>
          <div class="form-group">
            <label>Dedup Window (Sec)</label>
            <div class="input-wrap">
              <input type="number" step="1" name="dedup_window_sec" value="<!--DEDUP_WINDOW_SEC-->" required>
            </div>
            <p class="help-text">Blocks duplicate signals per symbol</p>
          </div>
          <div class="form-group">
            <label>Max Quote Age (Sec)</label>
            <div class="input-wrap">
              <input type="number" step="0.5" name="max_quote_age_sec" value="<!--MAX_QUOTE_AGE_SEC-->" required>
            </div>
            <p class="help-text">Rejects stale market quotes</p>
          </div>
        </div>
      </div>

      <button type="submit" class="btn-submit">
        <span>💾</span> Save & Apply Configuration to Audit bot
      </button>
    </form>
  </div>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def get_setup_page(saved: bool = False, saved_info: Optional[dict] = None):
    cfg = AppConfig.from_env()

    banner = ""
    next_steps = ""
    if saved:
        info = saved_info or {}
        mode_val = info.get("mode", cfg.trading_mode.value)
        badge_cls = "badge-live" if mode_val == "LIVE" else "badge-test"
        
        banner = f"""
        <div class="banner-success">
          <div style="width: 100%;">
            <div class="banner-header">
              <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>
              <span>Audit bot Configuration Saved & Applied!</span>
            </div>
            <p style="font-size: 13px; color: #a7f3d0; margin-bottom: 8px;">
              Your parameters were successfully written to <code>.env</code> with secure locked permissions (chmod 0600).
            </p>
            <div class="summary-grid">
              <div class="summary-chip">
                <div class="chip-label">Operating Mode</div>
                <div class="chip-value"><span class="badge {badge_cls}">{mode_val}</span></div>
              </div>
              <div class="summary-chip">
                <div class="chip-label">Operator</div>
                <div class="chip-value">{info.get('operator', getattr(cfg, 'operator_name', 'Udit'))}</div>
              </div>
              <div class="summary-chip">
                <div class="chip-label">Bot Instance</div>
                <div class="chip-value">{info.get('instance', getattr(cfg, 'instance_name', 'Audit bot'))}</div>
              </div>
              <div class="summary-chip">
                <div class="chip-label">Dhan Client ID</div>
                <div class="chip-value">{info.get('dhan_id', cfg.dhan_client_id or 'Not Set')}</div>
              </div>
              <div class="summary-chip">
                <div class="chip-label">Telegram Log Group</div>
                <div class="chip-value">{info.get('log_group', cfg.log_group_id or 'Not Set')}</div>
              </div>
              <div class="summary-chip">
                <div class="chip-label">Monitored Channels</div>
                <div class="chip-value">{len(cfg.monitored_channel_ids)} Active</div>
              </div>
            </div>
          </div>
        </div>
        """

    # Always render the Next Steps card so operator knows what to do
    next_steps = f"""
    <div class="next-steps-card">
      <div class="next-steps-title">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#38bdf8" stroke-width="2"><polygon points="5 3 19 12 5 21 5 3"/></svg>
        Next Operational Steps: Running & Verifying Audit bot
      </div>
      <div class="steps-list">
        <div class="step-item">
          <div class="step-num">1</div>
          <div class="step-content">
            <h5>Start or Restart the Bot Runner</h5>
            <p>In your VM terminal, start the signal ingestion pipeline:</p>
            <div class="step-code">cd ~/uditbot && python3 run.py</div>
          </div>
        </div>
        <div class="step-item">
          <div class="step-num">2</div>
          <div class="step-content">
            <h5>Check the Telegram Handshake</h5>
            <p>On launch, Audit bot will send an online verification card directly to your Telegram Log Group (<code>{cfg.log_group_id or '-5514889030'}</code>) confirming active channel listening.</p>
          </div>
        </div>
        <div class="step-item">
          <div class="step-num">3</div>
          <div class="step-content">
            <h5>Automated Signal Processing</h5>
            <p>Any BUY signal in your monitored channels will be sized, validated, and logged in real-time. In <strong>LIVE</strong> mode, orders route to Dhan with Forever OCO protection; in <strong>TEST</strong> mode, visual cards are dispatched without live broker orders.</p>
          </div>
        </div>
      </div>
    </div>
    """

    channels_str = ", ".join(str(cid) for cid in cfg.monitored_channel_ids) if cfg.monitored_channel_ids else ""

    html = (
        HTML_TEMPLATE.replace("<!--STATUS_BANNER-->", banner)
        .replace("<!--NEXT_STEPS_GUIDE-->", next_steps)
        # Mode & Products
        .replace("<!--LIVE_CHECKED-->", "checked" if cfg.trading_mode.value == "LIVE" else "")
        .replace("<!--EQUITY_CHECKED-->", "checked" if cfg.equity_active else "")
        .replace("<!--FNO_CHECKED-->", "checked" if cfg.fno_active else "")
        # Operator & Bot Name
        .replace("<!--OPERATOR_NAME-->", getattr(cfg, "operator_name", os.environ.get("OPERATOR_NAME", "Udit")))
        .replace("<!--INSTANCE_NAME-->", getattr(cfg, "instance_name", os.environ.get("BOT_INSTANCE_NAME", "Audit bot")))
        # Telegram
        .replace("<!--TG_API_ID-->", str(cfg.tg_api_id or ""))
        .replace("<!--TG_API_HASH-->", cfg.tg_api_hash or "")
        .replace("<!--MONITORED_CHANNEL_IDS-->", channels_str)
        .replace("<!--LOG_GROUP_ID-->", str(cfg.log_group_id or ""))
        .replace("<!--TEST_GROUP_ID-->", str(cfg.test_group_id or ""))
        # Dhan
        .replace("<!--DHAN_CLIENT_ID-->", cfg.dhan_client_id or "")
        .replace("<!--DHAN_PIN-->", cfg.dhan_pin or "")
        .replace("<!--DHAN_TOTP_SECRET-->", cfg.dhan_totp_secret or "")
        .replace("<!--DHAN_ACCESS_TOKEN-->", cfg.dhan_access_token or "")
        # Sizing
        .replace("<!--EQUITY_BUDGET_INR-->", str(int(cfg.equity_budget_inr)))
        .replace("<!--CNC_SELECTED-->", "selected" if cfg.equity_product_type.value == "CNC" else "")
        .replace("<!--MTF_SELECTED-->", "selected" if cfg.equity_product_type.value == "MTF" else "")
        .replace("<!--EQUITY_PRICE_MARKUP_PCT-->", str(cfg.equity_price_markup_pct))
        .replace("<!--FNO_CAPITAL_PER_TRADE-->", str(int(cfg.fno_capital_per_trade)))
        .replace("<!--FNO_MAX_LOTS-->", str(cfg.fno_max_lots))
        # Risk
        .replace("<!--MAX_ORDER_AMOUNT_INR-->", str(int(cfg.max_order_amount_inr)))
        .replace("<!--MIN_TRADE_FUNDS_INR-->", str(int(cfg.min_trade_funds_inr)))
        .replace("<!--MAX_DAILY_LOSS_INR-->", str(int(cfg.max_daily_loss_inr)))
        .replace("<!--MAX_DAILY_TRADES-->", str(cfg.max_daily_trades))
        .replace("<!--MAX_TOTAL_EXPOSURE_INR-->", str(int(cfg.max_total_exposure_inr)))
        .replace("<!--MAX_OPEN_POSITIONS-->", str(cfg.max_open_positions))
        # Protection & Timing
        .replace("<!--SL_PCT-->", str(cfg.sl_pct))
        .replace("<!--TARGET_PCT-->", str(cfg.target_pct))
        .replace("<!--SL_LIMIT_BUFFER_PCT-->", str(cfg.sl_limit_buffer_pct))
        .replace("<!--MAX_MESSAGE_AGE_SEC-->", str(int(cfg.max_message_age_sec)))
        .replace("<!--DEDUP_WINDOW_SEC-->", str(int(cfg.dedup_window_sec)))
        .replace("<!--MAX_QUOTE_AGE_SEC-->", str(cfg.max_quote_age_sec))
    )
    return HTMLResponse(content=html)


@app.post("/setup", response_class=HTMLResponse)
def handle_setup_submission(
    # Mode & Segments
    is_live_mode: bool = Form(False),
    equity_active: bool = Form(False),
    fno_active: bool = Form(False),
    # Operator & Telegram
    operator_name: str = Form("Udit"),
    instance_name: str = Form("Audit bot"),
    tg_api_id: str = Form(""),
    tg_api_hash: str = Form(""),
    monitored_channel_ids: str = Form(""),
    log_group_id: str = Form(""),
    test_group_id: str = Form(""),
    ignore_backlog: str = Form("true"),
    # Dhan Credentials
    dhan_client_id: str = Form(""),
    dhan_pin: str = Form(""),
    dhan_totp_secret: str = Form(""),
    dhan_access_token: str = Form(""),
    # Sizing
    equity_budget_inr: float = Form(25000.0),
    equity_product_type: str = Form("CNC"),
    equity_price_markup_pct: float = Form(0.5),
    fno_capital_per_trade: float = Form(20000.0),
    fno_max_lots: int = Form(3),
    # Risk
    max_order_amount_inr: float = Form(50000.0),
    min_trade_funds_inr: float = Form(10000.0),
    max_daily_loss_inr: float = Form(15000.0),
    max_daily_trades: Optional[int] = Form(None),
    daily_trade_limit: Optional[int] = Form(None),
    max_drawdown_pct: Optional[float] = Form(None),
    max_total_exposure_inr: float = Form(150000.0),
    max_open_positions: int = Form(5),
    # Protection & Timing
    sl_pct: float = Form(3.0),
    target_pct: float = Form(3.0),
    sl_limit_buffer_pct: float = Form(1.0),
    max_message_age_sec: float = Form(60.0),
    dedup_window_sec: float = Form(20.0),
    max_quote_age_sec: float = Form(2.0),
):
    trading_mode = "LIVE" if is_live_mode else "TEST"
    trades_count = max_daily_trades if max_daily_trades is not None else (daily_trade_limit if daily_trade_limit is not None else 20)

    # Clean channel IDs
    clean_monitored = ",".join([c.strip() for c in monitored_channel_ids.split(",") if c.strip()])

    env_content = f"""# =====================================================================
# AUDIT BOT / DHAN ALGO - OPERATOR CONFIGURATION
# Updated via Visual Web Setup Dashboard
# =====================================================================

# --- OPERATOR & BOT IDENTITY ---
OPERATOR_NAME={operator_name.strip()}
BOT_INSTANCE_NAME={instance_name.strip()}

# --- MASTER OPERATING MODE & PRODUCTS ---
TRADING_MODE={trading_mode}
EQUITY_ACTIVE={'true' if equity_active else 'false'}
FNO_ACTIVE={'true' if fno_active else 'false'}

# --- TELEGRAM USERBOT & CHANNEL INGESTION ---
TG_API_ID={tg_api_id.strip()}
TG_API_HASH={tg_api_hash.strip()}
MONITORED_CHANNEL_IDS={clean_monitored}
LOG_GROUP_ID={log_group_id.strip()}
TEST_GROUP_ID={test_group_id.strip()}
IGNORE_TELEGRAM_BACKLOG_ON_RESTART={ignore_backlog.strip()}

# --- DHANHQ v2 EXCHANGE CREDENTIALS ---
DHAN_CLIENT_ID={dhan_client_id.strip()}
DHAN_PIN={dhan_pin.strip()}
DHAN_TOTP_SECRET={dhan_totp_secret.strip()}
DHAN_ACCESS_TOKEN={dhan_access_token.strip()}
DHAN_TOKEN_FILE=~/.dhan_token

# --- CAPITAL & POSITION SIZING ---
EQUITY_PRODUCT_TYPE={equity_product_type.strip().upper()}
EQUITY_SIZING_MODE=BUDGET
EQUITY_BUDGET_INR={equity_budget_inr}
EQUITY_FIXED_QTY=10
EQUITY_PRICE_MARKUP_PCT={equity_price_markup_pct}

FNO_SIZING_MODE=BUDGET
FNO_CAPITAL_PER_TRADE={fno_capital_per_trade}
FNO_FIXED_LOTS=1
FNO_MAX_LOTS={fno_max_lots}

# --- RISK MANAGEMENT CONTROLS ---
MAX_ORDER_AMOUNT_INR={max_order_amount_inr}
MIN_TRADE_FUNDS_INR={min_trade_funds_inr}
MAX_DAILY_LOSS_INR={max_daily_loss_inr}
MAX_DAILY_TRADES={trades_count}
MAX_TOTAL_EXPOSURE_INR={max_total_exposure_inr}
MAX_OPEN_POSITIONS={max_open_positions}
INSUFFICIENT_FUNDS_POLICY=HARD_REJECT
MODE_SWITCH_POSITION_POLICY=REMAIN_UNTOUCHED

# --- PROTECTION & TIMING (FOREVER OCO) ---
SL_PCT={sl_pct}
TARGET_PCT={target_pct}
SL_LIMIT_BUFFER_PCT={sl_limit_buffer_pct}
MAX_MESSAGE_AGE_SEC={max_message_age_sec}
DEDUP_WINDOW_SEC={dedup_window_sec}
MAX_QUOTE_AGE_SEC={max_quote_age_sec}
PARTIAL_FILL_RETRY_TIMEOUT_SEC=2.0
ORDER_TIMEOUT_SEC=60.0
"""

    fd = os.open(".env", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(env_content)

    saved_info = {
        "mode": trading_mode,
        "operator": operator_name.strip(),
        "instance": instance_name.strip(),
        "dhan_id": dhan_client_id.strip(),
        "log_group": log_group_id.strip(),
    }
    return get_setup_page(saved=True, saved_info=saved_info)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    if "--port" in sys.argv:
        idx = sys.argv.index("--port")
        if idx + 1 < len(sys.argv):
            port = int(sys.argv[idx + 1])
    elif len(sys.argv) > 1 and sys.argv[1].isdigit():
        port = int(sys.argv[1])
    print(f"🚀 Starting Audit bot Setup Dashboard on http://0.0.0.0:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port)