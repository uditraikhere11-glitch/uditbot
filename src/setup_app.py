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
Uses an authenticated premium workspace, navigation and compact Save & Apply confirmations.
Safely reads from and writes to .env with chmod 0600 file permissions.
"""

import os
import sys
from html import escape as escape_html
from src.premium_ui import premium_styles, premium_nav, premium_nav_js, approved_replica_styles, approved_replica_js
from src.notifier.dhan_response import normalize_group_name
from typing import Optional
from fastapi import FastAPI, Request, Form, Depends, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

import secrets
from src.config import AppConfig, parse_env_dict
from src.broker.dhan_token import write_token_atomic
from src.engine.config_manager import is_isolated_test_environment

SETUP_CSRF_TOKEN = secrets.token_hex(16)
from src.engine.config_manager import (
    ConfigChangeTracker,
    execute_configuration_update,
    compute_config_version,
    check_service_is_active,
)
from src.notifier.telegram_notifier import TelegramNotifier
from src.engine.config_patch import ConfigurationConflict, ConfigurationRejected, REGISTRY, REQUEST_METADATA, revision



async def _submitted_webui_form(request: Request) -> dict:
    """Read only fields actually supplied, not FastAPI Form defaults."""
    form = await request.form()
    if any(len(form.getlist(key)) != 1 for key in form):
        raise HTTPException(status_code=422, detail="Duplicate form keys are not permitted")
    return dict(form)


def _setup_revision() -> str:
    try:
        with open(".env", "r", encoding="utf-8", newline="") as fh:
            return revision(fh.read())
    except FileNotFoundError:
        return revision("")
    except OSError:
        return "UNAVAILABLE"


app = FastAPI(title="Audit bot - Setup Dashboard")

# Presentation-only Telegram settings editor; existing Setup routes are unchanged.
from src.notifier.message_designer_routes import register_routes as register_message_designer_routes
register_message_designer_routes(app, SETUP_CSRF_TOKEN)




APPROVED_REFERENCE_BOARD = r'''<div id="replicaBoard" class="replica-board" aria-label="Trading configuration overview">
  <section class="replica-card replica-bot" aria-labelledby="replica-bot-heading">
    <div class="replica-card-top"><div class="replica-card-icon" aria-hidden="true">♙</div><div class="replica-card-headings"><h2 id="replica-bot-heading">Bot Configuration</h2><p>Core trading operation and signal settings</p></div></div>
    <div class="replica-rows">
      <div class="replica-row"><span class="replica-label">Trading Mode</span><div class="replica-modes replica-field" role="group" aria-label="Saved trading mode, applied on Save & Apply"><button id="replicaLive" type="button" aria-pressed="false">◉ &nbsp; LIVE</button><button id="replicaTest" type="button" class="replica-test" aria-pressed="true">◯ &nbsp; TEST</button></div></div>
      <div class="replica-row"><span class="replica-label">Trading Switch <a class="replica-link" href="/" aria-label="Manage running bot in Control Center">ⓘ</a></span><a id="replicaServiceState" href="/" class="replica-readonly-toggle" data-on="false" title="Manage the bot service in Control Center" aria-label="Open Control Center to manage the trading service"></a></div>
      <div class="replica-row"><span class="replica-label">Signal Channels</span><div class="replica-field" style="text-align:right"><a class="replica-link" href="#setup-telegram" id="replicaSources">Edit monitored channels in Advanced</a></div></div>
      <div class="replica-row"><label for="replicaPerOrder">Per Order Amount (₹)</label><div class="replica-field"><input id="replicaPerOrder" type="number" min="0.01" step="any" data-mirror="equity_budget_inr" disabled></div></div>
    </div>
  </section>
  <section class="replica-card replica-risk" aria-labelledby="replica-risk-heading">
    <div class="replica-card-top"><div class="replica-card-icon" aria-hidden="true">⬡</div><div class="replica-card-headings"><h2 id="replica-risk-heading">Risk &amp; Protection</h2><p>Position sizing and automatic exit protection</p></div></div>
    <div class="replica-rows">
      <div class="replica-row"><label for="replicaSl">Stop Loss (F&amp;O)</label><div class="replica-field replica-control-wrap"><input id="replicaSl" type="number" min="0.1" max="50" step="0.1" data-mirror="fno_sl_pct" disabled><span>%</span></div></div>
      <div class="replica-row"><label for="replicaTp">Target (F&amp;O)</label><div class="replica-field replica-control-wrap"><input id="replicaTp" type="number" min="0.1" max="100" step="0.1" data-mirror="fno_target_pct" disabled><span>%</span></div></div>
      <div class="replica-row"><label for="replicaLots">Max Lots (F&amp;O)</label><div class="replica-field"><input id="replicaLots" type="number" min="1" step="1" data-mirror="fno_max_lots" disabled></div></div>
      <div class="replica-row"><label for="replicaFnoBudget">F&amp;O Budget (₹)</label><div class="replica-field"><input id="replicaFnoBudget" type="number" min="0" step="any" data-mirror="fno_capital_per_trade" disabled></div></div>
      <div class="replica-row"><label for="replicaEquityBudget">Equity Budget (₹)</label><div class="replica-field"><input id="replicaEquityBudget" type="number" min="0.01" step="any" data-mirror="equity_budget_inr" disabled></div></div>
    </div>
  </section>
  <section class="replica-card replica-amo" aria-labelledby="replica-amo-heading">
    <div class="replica-card-top"><div class="replica-card-icon" aria-hidden="true">▣</div><div class="replica-card-headings"><h2 id="replica-amo-heading">AMO Settings</h2><p>After Market Order (AMO) configuration</p></div></div>
    <div class="replica-rows">
      <div class="replica-row"><span class="replica-label">Equity AMO</span><button id="replicaAMOEquity" class="replica-amo-switch" type="button" role="switch" aria-checked="false" data-on="false" disabled aria-label="Equity AMO: saved state unverified"></button></div>
      <div class="replica-row"><span class="replica-label">F&amp;O AMO</span><button id="replicaAMOFno" class="replica-amo-switch" type="button" role="switch" aria-checked="false" data-on="false" disabled aria-label="F&O AMO: saved state unverified"></button></div>
      <div class="replica-row"><span class="replica-label">AMO Dispatch Preference</span><div class="replica-field"><input type="text" value="Market Open (broker AMO)" readonly aria-label="AMO dispatch preference; controlled by broker" title="Native Dhan AMO dispatch is market open"></div></div>
      <div id="replicaAMOFeedback" class="replica-amo-summary" style="grid-column:1/-1">Checking saved AMO settings…</div>
    </div>
  </section>
  <section class="replica-card replica-market" aria-labelledby="replica-market-heading">
    <div class="replica-card-top"><div class="replica-card-icon" aria-hidden="true">◈</div><div class="replica-card-headings"><h2 id="replica-market-heading">Market &amp; Data</h2><p>Quotes, data freshness and scrip master</p></div></div>
    <div class="replica-rows">
      <div class="replica-row"><label for="replicaQuoteAge">Quote Staleness</label><div class="replica-field replica-control-wrap"><input id="replicaQuoteAge" type="number" min="0.1" step="0.5" data-mirror="max_quote_age_sec" disabled><span>seconds</span></div></div>
      <div class="replica-row"><label for="replicaPriceSource">Price Source Policy</label><div class="replica-field"><select id="replicaPriceSource" data-mirror="equity_cmp_execution_policy" disabled><option value="STRICT_TIMESTAMP">Strict timestamp</option><option value="DARVAS_HTTP_SUCCESS">Darvas HTTP response</option></select></div></div>
      <div class="replica-row"><label for="replicaScripCache">Scrip Master Cache</label><div class="replica-field replica-control-wrap"><input id="replicaScripCache" type="number" min="1" step="1" data-mirror="scrip_master_max_age_sec" disabled><span>seconds</span></div></div>
    </div>
  </section>
</div>'''


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en" class="premium-setup">
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
    
    /* Operational System Status Panel */
    .system-status-panel {
      background: rgba(17, 24, 39, 0.75);
      border: 1px solid #1e293b;
      border-radius: 14px;
      padding: 16px 20px;
      margin-bottom: 22px;
      box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.4);
    }
    .status-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 14px;
      padding-bottom: 8px;
      border-bottom: 1px solid #1f2937;
    }
    .status-header-title {
      font-size: 13px;
      font-weight: 700;
      color: #f1f5f9;
      display: flex;
      align-items: center;
      gap: 8px;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }
    .status-header-tag {
      font-size: 11px;
      font-weight: 600;
      color: #10b981;
      background: rgba(16, 185, 129, 0.1);
      border: 1px solid rgba(16, 185, 129, 0.3);
      padding: 3px 8px;
      border-radius: 6px;
    }
    .status-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
      gap: 12px;
    }
    .status-card {
      background: #0d1321;
      border: 1px solid #1e293b;
      border-radius: 8px;
      padding: 10px 12px;
    }
    .status-card-label {
      font-size: 10px;
      color: #94a3b8;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      margin-bottom: 4px;
    }
    .status-card-val {
      font-size: 12px;
      font-weight: 600;
      color: #f8fafc;
    }
    .status-pill {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      padding: 2px 8px;
      border-radius: 6px;
      font-size: 11px;
      font-weight: 700;
    }
    .pill-online {
      background: rgba(16, 185, 129, 0.15);
      color: #34d399;
      border: 1px solid rgba(16, 185, 129, 0.4);
    }
    .pill-offline {
      background: rgba(148, 163, 184, 0.12);
      color: #cbd5e1;
      border: 1px solid rgba(148, 163, 184, 0.3);
    }
    .pill-test {
      background: rgba(59, 130, 246, 0.15);
      color: #60a5fa;
      border: 1px solid rgba(59, 130, 246, 0.4);
    }
    .pill-live {
      background: rgba(239, 68, 68, 0.2);
      color: #f87171;
      border: 1px solid rgba(239, 68, 68, 0.5);
    }
    .pill-darvas {
      background: rgba(14, 165, 233, 0.15);
      color: #38bdf8;
      border: 1px solid rgba(14, 165, 233, 0.4);
    }
    .pill-version {
      background: rgba(168, 85, 247, 0.15);
      color: #c084fc;
      border: 1px solid rgba(168, 85, 247, 0.4);
      font-family: monospace;
    }
    .pill-danger {
      background: rgba(239, 68, 68, 0.2);
      color: #fca5a5;
      border: 1px solid #ef4444;
    }
    .pill-neutral {
      background: rgba(30, 41, 59, 0.7);
      color: #e2e8f0;
      border: 1px solid #334155;
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

    /* Save status styling lives in the shared premium design system. */
  </style>
  <!--PREMIUM_STYLES-->
  <!--APPROVED_REPLICA_STYLES-->
</head>
<body>
  <div class="container">
    <button id="premNavOpen" type="button" class="prem-nav-toggle" aria-controls="premNav" aria-expanded="false">☰ Menu</button>
    <!--PREMIUM_NAV-->
    <div class="replica-head">
      <div class="replica-brandline">
        <span class="replica-head-icon" aria-hidden="true">⚙</span>
        <div><h1 aria-label="Audit bot Setup &amp; Configuration">Setup &amp; Configuration</h1>
          <p>Configure trading, risk, AMO and notification settings</p></div>
      </div>
      <div class="replica-head-status" aria-live="polite">
        <span id="replicaModePill" class="replica-pill" data-tone="neutral">◌ UNVERIFIED</span>
        <span class="replica-separator" aria-hidden="true"></span>
        <span class="replica-status">◇ Reconciliation: Not verified</span>
        <span class="replica-separator" aria-hidden="true"></span>
        <span class="replica-status">♢ OCO Guard: Not verified</span>
        <span class="replica-separator" aria-hidden="true"></span>
        <time id="replicaClock" class="replica-clock">IST</time>
      </div>
    </div>
    <div class="replica-topline"><div id="replicaAlert" class="replica-alert" role="status" aria-live="polite" hidden>
      <span class="replica-alert-icon" aria-hidden="true">✓</span>
      <div><strong id="replicaAlertTitle">Settings</strong><small id="replicaAlertText">Update recorded.</small></div>
      <button id="replicaAlertClose" type="button" aria-label="Dismiss notification">×</button>
    </div></div>
    <!--STATUS_BANNER-->

    <form method="POST" action="/setup">
      <nav class="setup-section-jumps" aria-label="Setup sections">
        <a href="#setup-execution">Execution</a><a href="#setup-telegram">Telegram</a><a href="#setup-broker">Dhan Broker</a>
        <a href="#setup-sizing">Sizing</a><a href="#setup-risk">Risk Controls</a><a href="#setup-protection">Protection</a>
      </nav>
      <input type="hidden" name="csrf_token" value="<!--CSRF_TOKEN-->">
        <input type="hidden" name="expected_revision" value="<!--CONFIG_REVISION-->">
        <input type="hidden" name="full_setup_form" value="1">

      <!--APPROVED_REFERENCE_BOARD-->
      <div class="replica-footer">
        <button id="replicaReset" type="button" class="replica-reset">↶ &nbsp; Reset</button>
        <button id="replicaSaveOnly" type="button" class="replica-save" disabled title="Save without restart is available only when the bot is verified stopped">▣ &nbsp; Save</button>
        <button id="replicaApply" type="submit" class="btn-submit"><span>✓</span> Save &amp; Apply</button>
      </div>

      <details id="replicaAdvanced" class="replica-advanced" open>
        <summary>Advanced Configuration — Telegram, Dhan, account limits, and detailed protections</summary>
        <div class="replica-advanced-body">
                <label class="toggle-row" style="margin:12px 0;gap:12px;">
          <input type="checkbox" name="confirm_live" value="true" style="width:18px;height:18px;">
          I explicitly authorize switching TEST to LIVE. Actual broker orders may be placed.
        </label>

      <!-- 1. Master Execution Mode & Segments -->
      <div id="setup-execution" class="section-card">
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
      <div id="setup-telegram" class="section-card">
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
              <input type="password" name="tg_api_hash" value="<!--TG_API_HASH-->" placeholder="Saved in .env (leave blank to keep unchanged)">
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

        <div class="form-group" style="margin-top: 10px;">
          <label>Dhan Response Telegram Group Name or ID (optional)</label>
          <div class="input-wrap">
            <input type="text" name="dhan_response_group_name" value="<!--DHAN_RESPONSE_GROUP_NAME-->" maxlength="100" placeholder="Dhan Response or -5254821642">
          </div>
          <p class="help-text">Enter the exact Telegram group name or signed group chat ID. Dhan acknowledgements and final broker-confirmed outcomes are posted there. Leave blank to disable; the logged-in Telegram account must be a member.</p>
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

        <div class="form-group" style="margin-top: 10px;">
          <label>Telegram Bot Token (Optional - Dedicated Bot Dispatch)</label>
          <div class="input-wrap">
            <input type="text" name="tg_bot_token" value="<!--TG_BOT_TOKEN-->" placeholder="123456789:ABCdefGhIJKlmNoPQRsTUVwxyZ">
          </div>
          <p class="help-text">Send trade log cards via official Telegram Bot API instead of userbot</p>
        </div>
      </div>

      <!-- 3. DhanHQ Exchange Credentials -->
      <div id="setup-broker" class="section-card">
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
      <div id="setup-sizing" class="section-card">
        <div class="section-title">
          <svg viewBox="0 0 24 24" fill="none" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M16 8h-6a2 2 0 1 0 0 4h4a2 2 0 1 1 0 4H8"/><line x1="12" y1="6" x2="12" y2="8"/><line x1="12" y1="16" x2="12" y2="18"/></svg>
          Capital Sizing & Product Defaults
        </div>

        <div class="grid-3">
          <div class="form-group">
            <label>Equity Budget per Trade (INR)</label>
            <div class="input-wrap">
              <input type="number" step="any" min="0.01" name="equity_budget_inr" value="<!--EQUITY_BUDGET_INR-->" required>
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
          <div class="form-group" style="grid-column: span 2;">
            <label>Equity Live CMP Execution Policy</label>
            <div class="input-wrap">
              <select name="equity_cmp_execution_policy">
                <option value="DARVAS_HTTP_SUCCESS" <!--CMP_DARVAS_SEL-->>DARVAS_HTTP_SUCCESS (Accepts valid Yahoo HTTP 200 price reference)</option>
                <option value="STRICT_TIMESTAMP" <!--CMP_STRICT_SEL-->>STRICT_TIMESTAMP (Enforces strict <=2.0s source timestamp freshness)</option>
              </select>
            </div>
            <p class="help-text" style="color: #fbbf24;">⚠️ Warning: DARVAS_HTTP_SUCCESS mode accepts Yahoo HTTP 200 price references and may use delayed or stale market data when Yahoo's source timestamp lags.</p>
          </div>
        </div>

        <div class="grid-2" style="margin-top: 10px;">
          <div class="form-group">
            <label>Equity Sizing Mode</label>
            <div class="input-wrap">
              <select name="equity_sizing_mode">
                <option value="BUDGET" <!--EQ_BUDGET_SEL-->>BUDGET (Rupee Allocation)</option>
                <option value="FIXED_QTY" <!--EQ_FIXED_SEL-->>FIXED_QTY (Fixed Share Count)</option>
              </select>
            </div>
            <p class="help-text">Determine order sizing method for Cash Equity</p>
          </div>
          <div class="form-group">
            <label>Equity Fixed Quantity (Shares)</label>
            <div class="input-wrap">
              <input type="number" name="equity_fixed_qty" value="<!--EQUITY_FIXED_QTY-->" required>
            </div>
            <p class="help-text">Fixed share count used when Sizing Mode = FIXED_QTY</p>
          </div>
        </div>

        <div class="grid-2" style="margin-top: 10px;">
          <div class="form-group">
            <label>Stock Options Capital per Trade (INR)</label>
            <div class="input-wrap">
              <input type="number" step="any" name="fno_capital_per_trade" value="<!--FNO_CAPITAL_PER_TRADE-->" required>
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

        <div class="grid-2" style="margin-top: 10px;">
          <div class="form-group">
            <label>Options Sizing Mode</label>
            <div class="input-wrap">
              <select name="fno_sizing_mode">
                <option value="BUDGET" <!--FNO_BUDGET_SEL-->>BUDGET (Rupee Allocation)</option>
                <option value="FIXED_LOTS" <!--FNO_FIXED_SEL-->>FIXED_LOTS (Fixed Lot Count)</option>
              </select>
            </div>
            <p class="help-text">Determine order sizing method for Stock Options</p>
          </div>
          <div class="form-group">
            <label>Options Fixed Lots</label>
            <div class="input-wrap">
              <input type="number" name="fno_fixed_lots" value="<!--FNO_FIXED_LOTS-->" required>
            </div>
            <p class="help-text">Fixed lot count used when Options Sizing = FIXED_LOTS</p>
          </div>
        </div>

        <div class="form-group" style="margin-top: 10px;">
          <label>Nearest-Month Expiry Fallback</label>
          <div class="input-wrap">
            <select name="allow_next_month_expiry_fallback">
              <option value="true" <!--EXP_FB_TRUE-->>Enabled - Fall back to nearest monthly expiry</option>
              <option value="false" <!--EXP_FB_FALSE-->>Disabled - Require explicit month in signal</option>
            </select>
          </div>
          <p class="help-text">Resolves options when signal callout omits explicit month</p>
        </div>
      </div>

      <!-- 5. Risk Management & Circuit Breakers -->
      <div id="setup-risk" class="section-card">
        <div class="section-title">
          <svg viewBox="0 0 24 24" fill="none" stroke-width="2"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><polyline points="9 12 11 14 15 10"/></svg>
          Risk Management & Circuit Breakers
        </div>

        <div class="grid-3">
          <div class="form-group">
            <label>Max Order Amount (INR)</label>
            <div class="input-wrap">
              <input type="number" step="any" name="max_order_amount_inr" value="<!--MAX_ORDER_AMOUNT_INR-->" required>
            </div>
            <p class="help-text">Hard ceiling per single order</p>
          </div>
          <div class="form-group">
            <label>Min Required Ledger Funds (INR)</label>
            <div class="input-wrap">
              <input type="number" step="any" min="0" name="min_trade_funds_inr" value="<!--MIN_TRADE_FUNDS_INR-->" required>
            </div>
            <p class="help-text">Trading halted if funds drop below</p>
          </div>
          <div class="form-group">
            <label>Virtual Paper Balance (TEST Mode INR)</label>
            <div class="input-wrap">
              <input type="number" step="any" name="test_mode_paper_funds_inr" value="<!--TEST_MODE_PAPER_FUNDS_INR-->" required>
            </div>
            <p class="help-text">Simulated capital used for risk sizing in TEST mode</p>
          </div>
          <div class="form-group">
            <label>Max Daily Loss (INR)</label>
            <div class="input-wrap">
              <input type="number" step="any" name="max_daily_loss_inr" value="<!--MAX_DAILY_LOSS_INR-->" required>
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
              <input type="number" step="any" name="max_total_exposure_inr" value="<!--MAX_TOTAL_EXPOSURE_INR-->" required>
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

        <div class="grid-2" style="margin-top: 10px;">
          <div class="form-group">
            <label>Max Account Drawdown Limit (%)</label>
            <div class="input-wrap">
              <input type="number" step="0.5" name="max_drawdown_pct" value="<!--MAX_DRAWDOWN_PCT-->" required>
            </div>
            <p class="help-text">Account-level circuit breaker threshold</p>
          </div>
          <div class="form-group">
            <label>Insufficient Funds Policy</label>
            <div class="input-wrap">
              <select name="insufficient_funds_policy">
                <option value="HARD_REJECT" <!--HR_SELECTED-->>HARD_REJECT (Block Order)</option>
                <option value="SCALE_DOWN" <!--SD_SELECTED-->>SCALE_DOWN (Adjust Quantity)</option>
              </select>
            </div>
            <p class="help-text">Behavior when required capital exceeds ledger balance</p>
          </div>
        </div>
      </div>

      <!-- 6. Protection & Timing Parameters -->
      <div id="setup-protection" class="section-card">
        <div class="section-title">
          <svg viewBox="0 0 24 24" fill="none" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
          Protection & Timing Rules (Forever OCO)
        </div>

        <div class="grid-3">
          <div class="form-group">
            <label>Equity Stop Loss (%)</label>
            <div class="input-wrap">
              <input type="number" min="0.1" max="50" step="0.1" name="equity_sl_pct" value="<!--EQUITY_SL_PCT-->" required>
            </div>
            <p class="help-text">Cash Equity only: default 1% below executed fill</p>
          </div>
          <div class="form-group">
            <label>Equity Target (%)</label>
            <div class="input-wrap">
              <input type="number" min="0.1" max="100" step="0.1" name="equity_target_pct" value="<!--EQUITY_TARGET_PCT-->" required>
            </div>
            <p class="help-text">Cash Equity only: default 1% above executed fill</p>
          </div>
          <div class="form-group">
            <label>F&amp;O Stop Loss (%)</label>
            <div class="input-wrap">
              <input type="number" min="0.1" max="50" step="0.1" name="fno_sl_pct" value="<!--FNO_SL_PCT-->" required>
            </div>
            <p class="help-text">Stock options only: default 3% below executed fill</p>
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
            <label>F&amp;O Target (%)</label>
            <div class="input-wrap">
              <input type="number" min="0.1" max="100" step="0.1" name="fno_target_pct" value="<!--FNO_TARGET_PCT-->" required>
            </div>
            <p class="help-text">Stock options only: default 3% above executed fill</p>
          </div>
        </div>
        <!-- Backwards-compatible form keys, not used for segment protection -->
        <input type="hidden" name="sl_pct" value="<!--SL_PCT-->">
        <input type="hidden" name="target_pct" value="<!--TARGET_PCT-->">
        <p class="help-text">Provider Telegram SL / targets are informational only. Protection uses these independent values after execution.</p>

        <div class="grid-3" style="margin-top: 10px;">
          <div class="form-group">
            <label>Max Signal Message Age (Sec)</label>
            <div class="input-wrap">
              <input type="number" step="any" name="max_message_age_sec" value="<!--MAX_MESSAGE_AGE_SEC-->" required>
            </div>
            <p class="help-text">Rejects stale historical signals</p>
          </div>
          <div class="form-group">
            <label>Dedup Window (Sec)</label>
            <div class="input-wrap">
              <input type="number" step="any" name="dedup_window_sec" value="<!--DEDUP_WINDOW_SEC-->" required>
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

        <div class="grid-3" style="margin-top: 10px;">
          <div class="form-group">
            <label>Entry Order Fill Timeout (Sec)</label>
            <div class="input-wrap">
              <input type="number" step="any" name="order_timeout_sec" value="<!--ORDER_TIMEOUT_SEC-->" required>
            </div>
            <p class="help-text">Auto-cancels entry limit order if unfilled</p>
          </div>
          <div class="form-group">
            <label>Partial-Fill Retry Delay (Sec)</label>
            <div class="input-wrap">
              <input type="number" step="0.5" name="partial_fill_retry_timeout_sec" value="<!--PARTIAL_FILL_RETRY_TIMEOUT_SEC-->" required>
            </div>
            <p class="help-text">Wait buffer before single controlled retry</p>
          </div>
          <div class="form-group">
            <label>Scrip Master Cache Lifetime (Sec)</label>
            <div class="input-wrap">
              <input type="number" step="any" name="scrip_master_max_age_sec" value="<!--SCRIP_MASTER_MAX_AGE_SEC-->" required>
            </div>
            <p class="help-text">Dhan catalog cache duration (default 86400s)</p>
          </div>
        </div>
      </div>

        </div>
      </details>
      <div class="replica-extras">
        <div id="replicaPending" class="replica-info" role="status">All displayed values come from saved configuration</div>
        <details class="replica-diagnostics"><summary>Runtime diagnostics and existing status details</summary>
          <!--SYSTEM_STATUS_BAR-->
        </details>
      </div>
    </form>
  </div>

  <div id="simpleSaveStatus" role="status" aria-live="polite" hidden><span id="toastMessage"></span><button type="button" id="toastDismiss" aria-label="Dismiss notification">×</button></div>
<script>
document.addEventListener('DOMContentLoaded', function () {
  /*PREMIUM_NAV_JS*/
  /*APPROVED_REPLICA_JS*/
  const form = document.querySelector('form[action="/setup"]');
  if (!form) return;
  const button = form.querySelector('button[type="submit"]');
  const status = document.getElementById('simpleSaveStatus');
  const toastMessage = document.getElementById('toastMessage');
  const toastDismiss = document.getElementById('toastDismiss');
  let toastTimer;
  function showToast(value,tone,autoDismissMs) {
    clearTimeout(toastTimer);
    status.className = tone;
    toastMessage.textContent = String(value);
    status.hidden = false;
    if (autoDismissMs) toastTimer = setTimeout(() => { status.hidden = true; }, autoDismissMs);
  }
  toastDismiss.addEventListener('click', () => { clearTimeout(toastTimer); status.hidden = true; });
  const fallbackToast = document.querySelector('.setup-post-confirmation');
  if (fallbackToast) setTimeout(() => { fallbackToast.hidden = true; }, 5200);
  form.addEventListener('submit', async function (event) {
    event.preventDefault();
    if (button.disabled) return;
    const mode = form.querySelector('input[name="is_live_mode"]');
    if (mode && mode.checked) {
      if (!window.confirm('Save LIVE settings? Restarting a running bot can interrupt protection supervision. Continue?')) return;
      const livePermission = form.querySelector('input[name="confirm_live"]');
      if (livePermission) livePermission.checked = true;
    }
    button.disabled = true;
    showToast('Saving settings and applying changes…','neutral',0);
    try {
      const response = await fetch('/setup/save-apply', {
        method: 'POST',
        credentials: 'same-origin',
        body: new FormData(form)
      });
      const result = await response.json();
      if (!response.ok || !result.ok) throw Error(result.message || result.error || 'Save failed');
      const rev = form.querySelector('input[name="expected_revision"]');
      if (rev && result.revision) rev.value = result.revision;
      const verified = result.status === 'RESTARTED' && result.restart_performed === true;
      showToast(result.message || 'Save & Apply completed.', verified ? 'success' : 'neutral', 5200);
    } catch (error) {
      showToast(String(error.message || error) +
        ' Check service status and refresh before trying another change.', 'error', 0);
    } finally {
      button.disabled = false;
    }
  });
});
</script>
</body>
</html>
"""


def _mask_account_id(acct: str) -> str:
    if not acct:
        return "Not Configured"
    acct_str = str(acct).strip()
    if len(acct_str) <= 4:
        return acct_str
    return f"***{acct_str[-4:]}"


@app.get("/", response_class=HTMLResponse)
def get_setup_page(saved: bool = False, saved_info: Optional[dict] = None):
    # Render the authoritative editable .env and schema defaults only.
    # Process-level os.environ can contain stale WebUI secrets/settings
    # unrelated to the current file, which must never be copied back into
    # .env by an ordinary full form submission.
    raw_cfg_values = parse_env_dict(".env")
    cfg = AppConfig.from_env(env=raw_cfg_values, env_file="")

    # Build real-time system status bar
    from src.engine.config_manager import check_service_is_active, ConfigChangeTracker, ServiceOperatingState
    tracker = ConfigChangeTracker(data_dir=getattr(cfg, "data_dir", "data"))
    svc_op_state, _ = tracker.resolve_service_operating_state()
    is_svc_active = (svc_op_state == ServiceOperatingState.RUNNING)
    act_info = tracker.get_active_running_version() or {}
    act_v = act_info.get("short_version", "v-UNKNOWN")
    stop_path = os.path.join(getattr(cfg, "data_dir", "data"), "emergency_stop.json")
    is_stopped = False
    if os.path.exists(stop_path):
        try:
            with open(stop_path, "r", encoding="utf-8") as f:
                is_stopped = bool(json.load(f).get("is_stopped", False))
        except Exception:
            is_stopped = True

    if svc_op_state == ServiceOperatingState.RUNNING:
        svc_badge = '<span class="status-pill pill-online">🟢 Online (Running)</span>'
    elif svc_op_state == ServiceOperatingState.STOPPED_BY_OWNER:
        svc_badge = '<span class="status-pill pill-offline">⚪ Stopped by Owner</span>'
    elif svc_op_state == ServiceOperatingState.FAILED:
        svc_badge = '<span class="status-pill pill-danger">❌ Stopped Unexpectedly</span>'
    elif svc_op_state == ServiceOperatingState.UNKNOWN:
        svc_badge = '<span class="status-pill pill-offline">⚠️ Status Unavailable (UNKNOWN)</span>'
    else:
        svc_badge = '<span class="status-pill pill-offline">⚪ Offline (Stopped)</span>'
    mode_badge = f'<span class="status-pill {"pill-live" if cfg.trading_mode.value == "LIVE" else "pill-test"}">{"🔴 Real Trading (Live Money)" if cfg.trading_mode.value == "LIVE" else "🧪 Paper Trading (Safe / Zero Risk)"}</span>'
    stop_badge = '<span class="status-pill pill-danger">🛑 Halted (Emergency Stop)</span>' if is_stopped else '<span class="status-pill pill-online">🟢 Normal (Trading Allowed)</span>'
    cmp_pol = getattr(cfg, "equity_cmp_execution_policy", "STRICT_TIMESTAMP")
    cmp_label = "📈 Darvas Live Market Price" if cmp_pol == "DARVAS_HTTP_SUCCESS" else "⏱️ Strict 2s Market Price"
    cmp_badge = f'<span class="status-pill pill-darvas">{cmp_label}</span>'
    
    clean_v_id = escape_html(str(act_v).replace("v-", ""), quote=True)
    # A running process is not proof that it consumed recently saved settings.
    # Verify the actual systemd PID and acknowledged configuration hash.
    runtime_matches_saved = False
    if is_svc_active:
        try:
            runtime_matches_saved, _, _ = tracker.is_active_version_confirmed(
                expected_hashes={compute_config_version(cfg)},
                expected_mode=cfg.trading_mode.value,
                require_systemd_active=True,
            )
        except Exception:
            runtime_matches_saved = False
    if runtime_matches_saved:
        ver_badge = f'<span class="status-pill pill-online">🟢 Active Configuration Verified</span><span style="font-size: 10px; opacity: 0.6; display: block; font-weight: normal; margin-top: 3px;">Sync Ref: {clean_v_id}</span>'
    elif is_svc_active:
        ver_badge = f'<span class="status-pill pill-offline">⚠️ Saved — Running Version Not Verified</span><span style="font-size: 10px; opacity: 0.6; display: block; font-weight: normal; margin-top: 3px;">Sync Ref: {clean_v_id}</span>'
    else:
        ver_badge = f'<span class="status-pill pill-offline">⚪ Saved (Start Bot to Apply)</span><span style="font-size: 10px; opacity: 0.6; display: block; font-weight: normal; margin-top: 3px;">Sync Ref: {clean_v_id}</span>'

    dhan_masked = escape_html(_mask_account_id(cfg.dhan_client_id), quote=True)
    dhan_badge = f'<span class="status-pill pill-neutral">Connected ({dhan_masked})</span>' if cfg.dhan_client_id else '<span class="status-pill pill-offline">Not Configured</span>'

    status_bar = f"""
    <div class="system-status-panel">
      <div class="status-header">
        <div class="status-header-title">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#38bdf8" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/></svg>
          AuditBot Real-Time System Status
        </div>
        <div class="status-header-tag">Safety Guardrails Active</div>
      </div>
      <div class="status-grid">
        <div class="status-card">
          <div class="status-card-label">Bot Service</div>
          <div class="status-card-val">{svc_badge}</div>
        </div>
        <div class="status-card">
          <div class="status-card-label">Trading Mode</div>
          <div class="status-card-val">{mode_badge}</div>
        </div>
        <div class="status-card">
          <div class="status-card-label">Settings Status</div>
          <div class="status-card-val">{ver_badge}</div>
        </div>
        <div class="status-card">
          <div class="status-card-label">Price Source</div>
          <div class="status-card-val">{cmp_badge}</div>
        </div>
        <div class="status-card">
          <div class="status-card-label">Safety Circuit Breaker</div>
          <div class="status-card-val">{stop_badge}</div>
        </div>
        <div class="status-card">
          <div class="status-card-label">Dhan Broker Account</div>
          <div class="status-card-val">{dhan_badge}</div>
        </div>
      </div>
    </div>
    """

    banner = ""
    if saved:
        info = saved_info or {}
        mode_val = escape_html(str(info.get("mode", cfg.trading_mode.value)), quote=True)
        badge_cls = "badge-live" if mode_val == "LIVE" else "badge-test"
        
        saved_state = str(info.get("state", "UNKNOWN"))
        saved_message = escape_html(str(info.get("reason") or "Configuration request recorded."), quote=True)
        saved_label = escape_html(saved_state.replace("_", " "), quote=True)
        tone = "success" if saved_state == "RESTARTED" and info.get("service_restarted") else "neutral"
        banner = f"""<div class="setup-post-confirmation {tone}" role="status" aria-live="polite"><strong>Audit bot Configuration Request Recorded</strong> · {saved_label} — {saved_message}</div>"""

    channels_str = ", ".join(str(cid) for cid in cfg.monitored_channel_ids) if cfg.monitored_channel_ids else ""

    html = (
        HTML_TEMPLATE.replace("<!--PREMIUM_STYLES-->", premium_styles())
        .replace("<!--PREMIUM_NAV-->", premium_nav("setup"))
        .replace("<!--APPROVED_REPLICA_STYLES-->", approved_replica_styles())
        .replace("<!--APPROVED_REFERENCE_BOARD-->", APPROVED_REFERENCE_BOARD)
        .replace("/*APPROVED_REPLICA_JS*/", approved_replica_js())
        .replace("/*PREMIUM_NAV_JS*/", premium_nav_js())
        .replace("<!--STATUS_BANNER-->", banner)
        .replace("<!--SYSTEM_STATUS_BAR-->", status_bar)
        # Mode & Products
        .replace("<!--LIVE_CHECKED-->", "checked" if cfg.trading_mode.value == "LIVE" else "")
        .replace("<!--EQUITY_CHECKED-->", "checked" if cfg.equity_active else "")
        .replace("<!--FNO_CHECKED-->", "checked" if cfg.fno_active else "")
        # Operator & Bot Name
        .replace("<!--OPERATOR_NAME-->", escape_html(str(getattr(cfg, "operator_name", os.environ.get("OPERATOR_NAME", "Udit"))), quote=True))
        .replace("<!--INSTANCE_NAME-->", escape_html(str(getattr(cfg, "instance_name", os.environ.get("BOT_INSTANCE_NAME", "Audit bot"))), quote=True))
        .replace("<!--CSRF_TOKEN-->", SETUP_CSRF_TOKEN)
        .replace("<!--CONFIG_REVISION-->", _setup_revision())
        # Telegram (Secret values strictly excluded from HTML responses)
        .replace("<!--TG_API_ID-->", escape_html(str(raw_cfg_values.get("TG_API_ID", "")), quote=True))
        .replace("<!--TG_API_HASH-->", "")
        .replace("<!--MONITORED_CHANNEL_IDS-->", channels_str)
        .replace("<!--LOG_GROUP_ID-->", escape_html(str(raw_cfg_values.get("LOG_GROUP_ID", "")), quote=True))
        .replace("<!--DHAN_RESPONSE_GROUP_NAME-->", escape_html(cfg.dhan_response_group_name, quote=True))
        .replace("<!--TEST_GROUP_ID-->", escape_html(str(raw_cfg_values.get("TEST_GROUP_ID", "")), quote=True))
        # Dhan (Secret values strictly excluded from HTML responses)
        .replace("<!--DHAN_CLIENT_ID-->", escape_html(str(cfg.dhan_client_id or ""), quote=True))
        .replace("<!--DHAN_PIN-->", "")
        .replace("<!--DHAN_TOTP_SECRET-->", "")
        .replace("<!--DHAN_ACCESS_TOKEN-->", "")
        .replace("<!--CMP_DARVAS_SEL-->", "selected" if getattr(cfg, "equity_cmp_execution_policy", "STRICT_TIMESTAMP") == "DARVAS_HTTP_SUCCESS" else "")
        .replace("<!--CMP_STRICT_SEL-->", "selected" if getattr(cfg, "equity_cmp_execution_policy", "STRICT_TIMESTAMP") != "DARVAS_HTTP_SUCCESS" else "")
        # Sizing
        .replace("<!--EQUITY_BUDGET_INR-->", str(cfg.equity_budget_inr))
        .replace("<!--CNC_SELECTED-->", "selected" if cfg.equity_product_type.value == "CNC" else "")
        .replace("<!--MTF_SELECTED-->", "selected" if cfg.equity_product_type.value == "MTF" else "")
        .replace("<!--EQUITY_PRICE_MARKUP_PCT-->", str(cfg.equity_price_markup_pct))
        .replace("<!--FNO_CAPITAL_PER_TRADE-->", str(cfg.fno_capital_per_trade))
        .replace("<!--FNO_MAX_LOTS-->", str(cfg.fno_max_lots))
        # Risk
        .replace("<!--MAX_ORDER_AMOUNT_INR-->", str(cfg.max_order_amount_inr))
        .replace("<!--MIN_TRADE_FUNDS_INR-->", str(cfg.min_trade_funds_inr))
        .replace("<!--TEST_MODE_PAPER_FUNDS_INR-->", str(getattr(cfg, "test_mode_paper_funds_inr", 100000.0)))
        .replace("<!--MAX_DAILY_LOSS_INR-->", str(cfg.max_daily_loss_inr))
        .replace("<!--MAX_DAILY_TRADES-->", str(cfg.max_daily_trades))
        .replace("<!--MAX_TOTAL_EXPOSURE_INR-->", str(cfg.max_total_exposure_inr))
        .replace("<!--MAX_OPEN_POSITIONS-->", str(cfg.max_open_positions))
        # Protection & Timing
        .replace("<!--EQUITY_SL_PCT-->", str(cfg.equity_sl_pct))
        .replace("<!--EQUITY_TARGET_PCT-->", str(cfg.equity_target_pct))
        .replace("<!--FNO_SL_PCT-->", str(cfg.fno_sl_pct))
        .replace("<!--FNO_TARGET_PCT-->", str(cfg.fno_target_pct))
        .replace("<!--SL_PCT-->", str(cfg.sl_pct))
        .replace("<!--TARGET_PCT-->", str(cfg.target_pct))
        .replace("<!--SL_LIMIT_BUFFER_PCT-->", str(cfg.sl_limit_buffer_pct))
        .replace("<!--MAX_MESSAGE_AGE_SEC-->", str(cfg.max_message_age_sec))
        .replace("<!--DEDUP_WINDOW_SEC-->", str(cfg.dedup_window_sec))
        .replace("<!--MAX_QUOTE_AGE_SEC-->", str(cfg.max_quote_age_sec))
        # Additional Engine Parameters
        .replace("<!--TG_BOT_TOKEN-->", "")
        .replace("<!--EQ_BUDGET_SEL-->", "selected" if getattr(cfg, "equity_sizing_mode", "BUDGET") == "BUDGET" else "")
        .replace("<!--EQ_FIXED_SEL-->", "selected" if getattr(cfg, "equity_sizing_mode", "BUDGET") == "FIXED_QTY" else "")
        .replace("<!--EQUITY_FIXED_QTY-->", str(getattr(cfg, "equity_fixed_qty", 10)))
        .replace("<!--FNO_BUDGET_SEL-->", "selected" if getattr(cfg, "fno_sizing_mode", "BUDGET") == "BUDGET" else "")
        .replace("<!--FNO_FIXED_SEL-->", "selected" if getattr(cfg, "fno_sizing_mode", "BUDGET") == "FIXED_LOTS" else "")
        .replace("<!--FNO_FIXED_LOTS-->", str(getattr(cfg, "fno_fixed_lots", 1)))
        .replace("<!--EXP_FB_TRUE-->", "selected" if getattr(cfg, "allow_next_month_expiry_fallback", True) else "")
        .replace("<!--EXP_FB_FALSE-->", "selected" if not getattr(cfg, "allow_next_month_expiry_fallback", True) else "")
        .replace("<!--MAX_DRAWDOWN_PCT-->", str(getattr(cfg, "max_drawdown_pct", 5.0)))
        .replace("<!--HR_SELECTED-->", "selected" if getattr(cfg, "insufficient_funds_policy", "HARD_REJECT") == "HARD_REJECT" else "")
        .replace("<!--SD_SELECTED-->", "selected" if getattr(cfg, "insufficient_funds_policy", "HARD_REJECT") == "SCALE_DOWN" else "")
        .replace("<!--ORDER_TIMEOUT_SEC-->", str(getattr(cfg, "order_timeout_sec", 60.0)))
        .replace("<!--PARTIAL_FILL_RETRY_TIMEOUT_SEC-->", str(getattr(cfg, "partial_fill_retry_timeout_sec", 2.0)))
        .replace("<!--SCRIP_MASTER_MAX_AGE_SEC-->", str(getattr(cfg, "scrip_master_max_age_sec", 86400.0)))
    )
    return HTMLResponse(content=html)


def _extract_val(field_val, fallback):
    if hasattr(field_val, "default"):
        return field_val.default
    return field_val if field_val is not None else fallback


def build_env_and_params_dict(
    is_live_mode=False,
    equity_active=False,
    fno_active=False,
    operator_name="Udit",
    instance_name="Audit bot",
    tg_api_id="",
    tg_api_hash="",
    monitored_channel_ids="",
    log_group_id="",
    dhan_response_group_name="",
    test_group_id="",
    ignore_backlog="true",
    dhan_client_id="",
    dhan_pin="",
    dhan_totp_secret="",
    dhan_access_token="",
    equity_budget_inr=25000.0,
    equity_product_type="CNC",
    equity_price_markup_pct=0.5,
    equity_cmp_execution_policy=None,
    fno_capital_per_trade=20000.0,
    fno_max_lots=3,
    max_order_amount_inr=50000.0,
    min_trade_funds_inr=10000.0,
    test_mode_paper_funds_inr=100000.0,
    max_daily_loss_inr=15000.0,
    max_daily_trades=None,
    daily_trade_limit=None,
    max_drawdown_pct=None,
    max_total_exposure_inr=150000.0,
    max_open_positions=5,
    equity_sl_pct=1.0,
    equity_target_pct=1.0,
    fno_sl_pct=3.0,
    fno_target_pct=3.0,
    sl_pct=3.0,
    target_pct=3.0,
    sl_limit_buffer_pct=1.0,
    max_message_age_sec=60.0,
    dedup_window_sec=20.0,
    max_quote_age_sec=2.0,
    tg_bot_token="",
    equity_sizing_mode="BUDGET",
    equity_fixed_qty=10,
    fno_sizing_mode="BUDGET",
    fno_fixed_lots=1,
    allow_next_month_expiry_fallback="true",
    insufficient_funds_policy="HARD_REJECT",
    order_timeout_sec=60.0,
    partial_fill_retry_timeout_sec=2.0,
    scrip_master_max_age_sec=86400.0,
    env_path=".env",
    **extra
):
    existing_env = parse_env_dict(env_path) if os.path.exists(env_path) else {}
    trading_mode = "LIVE" if is_live_mode else "TEST"
    _existing_trades_str = str(existing_env.get("MAX_DAILY_TRADES", "10")).strip()
    _default_trades = int(_existing_trades_str) if _existing_trades_str.isdigit() else 10
    trades_count = max_daily_trades if max_daily_trades is not None else (daily_trade_limit if daily_trade_limit is not None else _default_trades)

    try:
        paper_funds_val = float(_extract_val(test_mode_paper_funds_inr, 100000.0))
    except (ValueError, TypeError):
        paper_funds_val = 100000.0

    # Preserve existing secrets when submitted blank
    raw_tg_hash = str(_extract_val(tg_api_hash, "")).strip()
    clean_tg_hash = raw_tg_hash if raw_tg_hash else existing_env.get("TG_API_HASH", "")

    raw_tg_bot = str(_extract_val(tg_bot_token, "")).strip()
    clean_tg_bot = raw_tg_bot if raw_tg_bot else existing_env.get("TG_BOT_TOKEN", "")

    raw_dhan_pin = str(_extract_val(dhan_pin, "")).strip()
    clean_dhan_pin = raw_dhan_pin if raw_dhan_pin else existing_env.get("DHAN_PIN", "")

    raw_dhan_totp = str(_extract_val(dhan_totp_secret, "")).strip()
    clean_dhan_totp = raw_dhan_totp if raw_dhan_totp else existing_env.get("DHAN_TOTP_SECRET", "")

    # Single authoritative token source: ~/.dhan_token is managed exclusively by the TOTP coordinator.
    # Ordinary WebUI Save must NEVER write DHAN_ACCESS_TOKEN to .env or overwrite the shared token file.
    raw_dhan_token = str(_extract_val(dhan_access_token, "")).strip()
    token_file_setting = existing_env.get("DHAN_TOKEN_FILE", "~/.dhan_token")

    raw_cmp_pol = _extract_val(equity_cmp_execution_policy, None)
    clean_cmp_policy = (
        str(raw_cmp_pol).upper().strip()
        if raw_cmp_pol
        else existing_env.get("EQUITY_CMP_EXECUTION_POLICY", "STRICT_TIMESTAMP")
    )

    clean_eq_mode = str(_extract_val(equity_sizing_mode, "BUDGET")).upper().strip()
    try:
        clean_eq_qty = int(_extract_val(equity_fixed_qty, 10))
    except (ValueError, TypeError):
        clean_eq_qty = 10
    clean_fno_mode = str(_extract_val(fno_sizing_mode, "BUDGET")).upper().strip()
    try:
        clean_fno_lots = int(_extract_val(fno_fixed_lots, 1))
    except (ValueError, TypeError):
        clean_fno_lots = 1
    clean_exp_fb = "true" if str(_extract_val(allow_next_month_expiry_fallback, "true")).lower() in ("true", "1", "yes") else "false"
    clean_insufficient = str(_extract_val(insufficient_funds_policy, "HARD_REJECT")).upper().strip()
    try:
        clean_drawdown = float(_extract_val(max_drawdown_pct, 5.0))
    except (ValueError, TypeError):
        clean_drawdown = 5.0
    try:
        clean_order_timeout = float(_extract_val(order_timeout_sec, 60.0))
    except (ValueError, TypeError):
        clean_order_timeout = 60.0
    try:
        clean_retry_timeout = float(_extract_val(partial_fill_retry_timeout_sec, 2.0))
    except (ValueError, TypeError):
        clean_retry_timeout = 2.0
    try:
        clean_scrip_max_age = float(_extract_val(scrip_master_max_age_sec, 86400.0))
    except (ValueError, TypeError):
        clean_scrip_max_age = 86400.0

    clean_monitored = ",".join([c.strip() for c in str(_extract_val(monitored_channel_ids, "")).split(",") if c.strip()])
    clean_dhan_response_group_name = normalize_group_name(_extract_val(dhan_response_group_name, ""))

    managed_keys = {
        "OPERATOR_NAME", "BOT_INSTANCE_NAME", "TRADING_MODE", "EQUITY_ACTIVE", "FNO_ACTIVE",
        "TG_API_ID", "TG_API_HASH", "TG_BOT_TOKEN", "MONITORED_CHANNEL_IDS", "LOG_GROUP_ID", "DHAN_RESPONSE_GROUP_NAME",
        "TEST_GROUP_ID", "IGNORE_TELEGRAM_BACKLOG_ON_RESTART", "DHAN_CLIENT_ID", "DHAN_PIN",
        "DHAN_TOTP_SECRET", "DHAN_ACCESS_TOKEN", "DHAN_TOKEN_FILE", "EQUITY_PRODUCT_TYPE",
        "EQUITY_SIZING_MODE", "EQUITY_BUDGET_INR", "EQUITY_FIXED_QTY", "EQUITY_PRICE_MARKUP_PCT",
        "EQUITY_CMP_EXECUTION_POLICY", "FNO_SIZING_MODE", "FNO_CAPITAL_PER_TRADE", "FNO_FIXED_LOTS",
        "FNO_MAX_LOTS", "ALLOW_NEXT_MONTH_EXPIRY_FALLBACK", "MAX_ORDER_AMOUNT_INR",
        "MIN_TRADE_FUNDS_INR", "TEST_MODE_PAPER_FUNDS_INR", "MAX_DAILY_LOSS_INR",
        "MAX_DRAWDOWN_PCT", "MAX_DAILY_TRADES", "MAX_TOTAL_EXPOSURE_INR", "MAX_OPEN_POSITIONS",
        "INSUFFICIENT_FUNDS_POLICY", "MODE_SWITCH_POSITION_POLICY",
        "EQUITY_SL_PCT", "EQUITY_TARGET_PCT", "FNO_SL_PCT", "FNO_TARGET_PCT",
        "SL_PCT", "TARGET_PCT",
        "SL_LIMIT_BUFFER_PCT", "MAX_MESSAGE_AGE_SEC", "DEDUP_WINDOW_SEC", "MAX_QUOTE_AGE_SEC",
        "ORDER_TIMEOUT_SEC", "PARTIAL_FILL_RETRY_TIMEOUT_SEC", "SCRIP_MASTER_MAX_AGE_SEC",
    }

    unknown_lines = []
    for k, v in existing_env.items():
        if k not in managed_keys:
            unknown_lines.append(f"{k}={v}")

    preserved_block = ""
    if unknown_lines:
        preserved_block = "\n# --- PRESERVED CUSTOM KEYS ---\n" + "\n".join(unknown_lines) + "\n"

    env_content = f"""# =====================================================================
# AUDIT BOT / DHAN ALGO - OPERATOR CONFIGURATION
# Updated via Visual Web Setup Dashboard
# =====================================================================

# --- OPERATOR & BOT IDENTITY ---
OPERATOR_NAME={str(_extract_val(operator_name, 'Udit')).strip()}
BOT_INSTANCE_NAME={str(_extract_val(instance_name, 'Audit bot')).strip()}

# --- MASTER OPERATING MODE & PRODUCTS ---
TRADING_MODE={trading_mode}
EQUITY_ACTIVE={'true' if _extract_val(equity_active, False) else 'false'}
FNO_ACTIVE={'true' if _extract_val(fno_active, False) else 'false'}

# --- TELEGRAM USERBOT & CHANNEL INGESTION ---
TG_API_ID={str(_extract_val(tg_api_id, '')).strip()}
TG_API_HASH={clean_tg_hash}
TG_BOT_TOKEN={clean_tg_bot}
MONITORED_CHANNEL_IDS={clean_monitored}
LOG_GROUP_ID={str(_extract_val(log_group_id, '')).strip()}
DHAN_RESPONSE_GROUP_NAME={clean_dhan_response_group_name}
TEST_GROUP_ID={str(_extract_val(test_group_id, '')).strip()}
IGNORE_TELEGRAM_BACKLOG_ON_RESTART={str(_extract_val(ignore_backlog, 'true')).strip()}

# --- DHANHQ v2 EXCHANGE CREDENTIALS ---
DHAN_CLIENT_ID={str(_extract_val(dhan_client_id, '')).strip()}
DHAN_PIN={clean_dhan_pin}
DHAN_TOTP_SECRET={clean_dhan_totp}
DHAN_TOKEN_FILE={token_file_setting}

# --- CAPITAL & POSITION SIZING ---
EQUITY_PRODUCT_TYPE={str(_extract_val(equity_product_type, 'CNC')).strip().upper()}
EQUITY_SIZING_MODE={clean_eq_mode}
EQUITY_BUDGET_INR={_extract_val(equity_budget_inr, 25000.0)}
EQUITY_FIXED_QTY={clean_eq_qty}
EQUITY_PRICE_MARKUP_PCT={_extract_val(equity_price_markup_pct, 0.5)}
EQUITY_CMP_EXECUTION_POLICY={clean_cmp_policy}

FNO_SIZING_MODE={clean_fno_mode}
FNO_CAPITAL_PER_TRADE={_extract_val(fno_capital_per_trade, 20000.0)}
FNO_FIXED_LOTS={clean_fno_lots}
FNO_MAX_LOTS={_extract_val(fno_max_lots, 3)}
ALLOW_NEXT_MONTH_EXPIRY_FALLBACK={clean_exp_fb}

# --- RISK MANAGEMENT CONTROLS ---
MAX_ORDER_AMOUNT_INR={_extract_val(max_order_amount_inr, 50000.0)}
MIN_TRADE_FUNDS_INR={_extract_val(min_trade_funds_inr, 10000.0)}
TEST_MODE_PAPER_FUNDS_INR={paper_funds_val}
MAX_DAILY_LOSS_INR={_extract_val(max_daily_loss_inr, 15000.0)}
MAX_DRAWDOWN_PCT={clean_drawdown}
MAX_DAILY_TRADES={trades_count}
MAX_TOTAL_EXPOSURE_INR={_extract_val(max_total_exposure_inr, 150000.0)}
MAX_OPEN_POSITIONS={_extract_val(max_open_positions, 5)}
INSUFFICIENT_FUNDS_POLICY={clean_insufficient}
MODE_SWITCH_POSITION_POLICY=REMAIN_UNTOUCHED

# --- SEGMENT-SPECIFIC PROTECTION (FOREVER OCO / SUPER ORDER) ---
EQUITY_SL_PCT={_extract_val(equity_sl_pct, 1.0)}
EQUITY_TARGET_PCT={_extract_val(equity_target_pct, 1.0)}
FNO_SL_PCT={_extract_val(fno_sl_pct, 3.0)}
FNO_TARGET_PCT={_extract_val(fno_target_pct, 3.0)}
# Legacy-only fields below are not applied to new order protection.
SL_PCT={_extract_val(sl_pct, 3.0)}
TARGET_PCT={_extract_val(target_pct, 3.0)}
SL_LIMIT_BUFFER_PCT={_extract_val(sl_limit_buffer_pct, 1.0)}
MAX_MESSAGE_AGE_SEC={_extract_val(max_message_age_sec, 60.0)}
DEDUP_WINDOW_SEC={_extract_val(dedup_window_sec, 20.0)}
MAX_QUOTE_AGE_SEC={_extract_val(max_quote_age_sec, 2.0)}
ORDER_TIMEOUT_SEC={clean_order_timeout}
PARTIAL_FILL_RETRY_TIMEOUT_SEC={clean_retry_timeout}
SCRIP_MASTER_MAX_AGE_SEC={clean_scrip_max_age}
{preserved_block}"""
    params_dict = {
        "TRADING_MODE": trading_mode,
        "OPERATOR_NAME": str(_extract_val(operator_name, 'Udit')).strip(),
        "BOT_INSTANCE_NAME": str(_extract_val(instance_name, 'Audit bot')).strip(),
        "EQUITY_ACTIVE": 'true' if _extract_val(equity_active, False) else 'false',
        "FNO_ACTIVE": 'true' if _extract_val(fno_active, False) else 'false',
        "TG_API_ID": str(_extract_val(tg_api_id, '')).strip(),
        "TG_API_HASH": clean_tg_hash,
        "TG_BOT_TOKEN": clean_tg_bot,
        "DHAN_PIN": clean_dhan_pin,
        "DHAN_TOTP_SECRET": clean_dhan_totp,
        "MONITORED_CHANNEL_IDS": clean_monitored,
        "LOG_GROUP_ID": str(_extract_val(log_group_id, '')).strip(),
        "DHAN_RESPONSE_GROUP_NAME": clean_dhan_response_group_name,
        "TEST_GROUP_ID": str(_extract_val(test_group_id, '')).strip(),
        "IGNORE_TELEGRAM_BACKLOG_ON_RESTART": str(_extract_val(ignore_backlog, 'true')).strip(),
        "DHAN_CLIENT_ID": str(_extract_val(dhan_client_id, '')).strip(),
        "EQUITY_PRODUCT_TYPE": str(_extract_val(equity_product_type, 'CNC')).strip().upper(),
        "EQUITY_SIZING_MODE": clean_eq_mode,
        "EQUITY_BUDGET_INR": float(_extract_val(equity_budget_inr, 25000.0)),
        "EQUITY_FIXED_QTY": clean_eq_qty,
        "EQUITY_PRICE_MARKUP_PCT": float(_extract_val(equity_price_markup_pct, 0.5)),
        "EQUITY_CMP_EXECUTION_POLICY": clean_cmp_policy,
        "FNO_SIZING_MODE": clean_fno_mode,
        "FNO_CAPITAL_PER_TRADE": float(_extract_val(fno_capital_per_trade, 20000.0)),
        "FNO_FIXED_LOTS": clean_fno_lots,
        "FNO_MAX_LOTS": int(_extract_val(fno_max_lots, 3)),
        "ALLOW_NEXT_MONTH_EXPIRY_FALLBACK": clean_exp_fb,
        "MAX_ORDER_AMOUNT_INR": float(_extract_val(max_order_amount_inr, 50000.0)),
        "MIN_TRADE_FUNDS_INR": float(_extract_val(min_trade_funds_inr, 10000.0)),
        "TEST_MODE_PAPER_FUNDS_INR": paper_funds_val,
        "MAX_DAILY_LOSS_INR": float(_extract_val(max_daily_loss_inr, 15000.0)),
        "MAX_DRAWDOWN_PCT": clean_drawdown,
        "MAX_DAILY_TRADES": int(_extract_val(trades_count, 20)),
        "MAX_TOTAL_EXPOSURE_INR": float(_extract_val(max_total_exposure_inr, 150000.0)),
        "MAX_OPEN_POSITIONS": int(_extract_val(max_open_positions, 5)),
        "INSUFFICIENT_FUNDS_POLICY": clean_insufficient,
        "MODE_SWITCH_POSITION_POLICY": "REMAIN_UNTOUCHED",
        "EQUITY_SL_PCT": float(_extract_val(equity_sl_pct, 1.0)),
        "EQUITY_TARGET_PCT": float(_extract_val(equity_target_pct, 1.0)),
        "FNO_SL_PCT": float(_extract_val(fno_sl_pct, 3.0)),
        "FNO_TARGET_PCT": float(_extract_val(fno_target_pct, 3.0)),
        "SL_PCT": float(_extract_val(sl_pct, 3.0)),
        "TARGET_PCT": float(_extract_val(target_pct, 3.0)),
        "SL_LIMIT_BUFFER_PCT": float(_extract_val(sl_limit_buffer_pct, 1.0)),
        "MAX_MESSAGE_AGE_SEC": float(_extract_val(max_message_age_sec, 60.0)),
        "DEDUP_WINDOW_SEC": float(_extract_val(dedup_window_sec, 20.0)),
        "MAX_QUOTE_AGE_SEC": float(_extract_val(max_quote_age_sec, 2.0)),
        "ORDER_TIMEOUT_SEC": clean_order_timeout,
        "PARTIAL_FILL_RETRY_TIMEOUT_SEC": clean_retry_timeout,
        "SCRIP_MASTER_MAX_AGE_SEC": clean_scrip_max_age,
        "is_live_mode": bool(is_live_mode),
    }
    return env_content, params_dict


@app.post("/setup/save-apply")
async def simple_save_apply(request: Request):
    """One synchronous Save & Apply action; no tracker modal or async worker."""
    from src.engine.simple_save_apply import apply_settings
    try:
        form = await request.form()
        if any(len(form.getlist(key)) != 1 for key in form):
            return JSONResponse(status_code=422, content={"ok": False, "error": "Duplicate form field"})
        submitted = dict(form)
        if submitted.get("csrf_token") != SETUP_CSRF_TOKEN:
            return JSONResponse(status_code=403, content={"ok": False, "error": "Invalid CSRF token"})
        unknown = set(submitted) - set(REGISTRY) - REQUEST_METADATA - {"dhan_access_token", "daily_trade_limit"}
        if unknown:
            return JSONResponse(status_code=422, content={"ok": False, "error": "Unsupported settings fields"})
        editable = {k: v for k, v in submitted.items() if k in REGISTRY or k in ("dhan_access_token", "daily_trade_limit")}
        if submitted.get("full_setup_form") == "1":
            for name in ("is_live_mode", "equity_active", "fno_active"):
                editable[name] = name in submitted
        result = apply_settings(
            editable, env_path=os.path.abspath(".env"),
            expected_revision=str(submitted.get("expected_revision") or ""),
            confirm_live=str(submitted.get("confirm_live") or "").lower() in ("1", "true", "on", "yes"),
        )
        return JSONResponse(content=result.as_dict())
    except ConfigurationConflict as exc:
        return JSONResponse(status_code=409, content={"ok": False, "error": str(exc)})
    except (ConfigurationRejected, ValueError) as exc:
        return JSONResponse(status_code=422, content={"ok": False, "error": str(exc)})


@app.get("/setup/change-status/{change_id}")
def get_change_status(change_id: str):
    """Section F: Strictly read-only polling endpoint for real-time modal progress."""
    cfg = AppConfig.from_env()
    tracker = ConfigChangeTracker(data_dir=getattr(cfg, "data_dir", "data"))
    rec = tracker.get_change_record(change_id)
    if not rec:
        rec = tracker.get_change_record()
        if not rec or rec.change_id != change_id:
            return JSONResponse(status_code=404, content={"error": f"Change {change_id} not found"})
    status = rec.to_dict()
    # Read-only projection: a notification-only SAVE becomes VERIFIED only
    # after the actual authenticated listener acknowledges the requested
    # destination, matches systemd MainPID/start time, and confirms the exact
    # active configuration version. No broker GET or state mutation here.
    if rec.changed_fields == ["DHAN_RESPONSE_GROUP_NAME"]:
        from src.notifier.response_activation import read_active_destination
        ack = read_active_destination(tracker.data_dir)
        saved_name = parse_env_dict(".env").get("DHAN_RESPONSE_GROUP_NAME", "")
        running = tracker.get_active_running_version(require_live_pid=True)
        if (ack and running
                and ack.get("requested_name") == saved_name
                and ack.get("pid") == running.get("pid")
                and ack.get("process_starttime") is not None
                and ack.get("process_starttime") == running.get("process_starttime")
                and ack.get("config_version_hash") == running.get("version_hash")
                and isinstance(ack.get("acknowledged_at"), (int, float))
                and isinstance(running.get("acknowledged_at"), (int, float))
                and ack["acknowledged_at"] >= running["acknowledged_at"]
                and ack["acknowledged_at"] >= float(rec.saved_at or rec.requested_at or 0)):
            if ack.get("target_verified") is True and rec.proposed_version_hash == running.get("version_hash"):
                confirmed, _, _ = tracker.is_active_version_confirmed(
                    expected_hashes={rec.proposed_version_hash},
                    expected_mode=rec.requested_mode,
                    require_systemd_active=True,
                )
                if confirmed:
                    status.update({
                        "state": "APPLIED_VERIFIED",
                        "operation_state": "APPLIED_VERIFIED",
                        "state_label": "Dhan Response destination active and verified",
                        "notification_destination_state": "ACTIVE_VERIFIED",
                        "runtime_verified": True,
                        "progress_percent": 100,
                        "reason": "The connected Telegram listener confirmed the saved destination. No trading restart was issued by the configuration transaction.",
                    })
            elif ack.get("fallback_active"):
                status.update({
                    "notification_destination_state": "PREVIOUS_DESTINATION_ACTIVE",
                    "state_label": "New destination unresolved; previous verified destination retained",
                    "reason": "Requested Dhan Response group could not be verified. An independently verified previous group remains active; review the requested destination.",
                })
            elif not ack.get("target_verified"):
                status.update({
                    "notification_destination_state": "DESTINATION_UNVERIFIED",
                    "state_label": "Requested Telegram destination unavailable",
                    "reason": "The connected Telegram listener could not verify the requested destination. Notification mirroring is unavailable until corrected.",
                })
    return JSONResponse(content=status)


@app.post("/setup/service-control")
async def handle_service_control(request: Request):
    """B10 & B12: Authenticated, CSRF-protected Start / Stop / Restart / Status endpoint for the WebUI."""
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        raw_data = await request.json()
    else:
        form = await request.form()
        raw_data = dict(form)

    csrf_val = raw_data.get("csrf_token")
    if not csrf_val or str(csrf_val) != SETUP_CSRF_TOKEN:
        return JSONResponse(
            status_code=403,
            content={"ok": False, "error": "Missing or invalid CSRF token", "service_state": "UNKNOWN"},
        )

    action = str(raw_data.get("action") or "").strip().lower()
    if action not in ("start", "stop", "restart", "status"):
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": f"Invalid service action '{action}' (only start/stop/restart/status allowed)"},
        )

    confirm_live = str(raw_data.get("confirm_live", "")).strip().lower() in ("1", "true", "yes", "on") or (raw_data.get("confirm_live") is True)
    reason = str(raw_data.get("reason") or f"Operator requested {action} from WebUI").strip()

    cfg = AppConfig.from_env()
    env_path = os.path.abspath(".env")
    tracker = ConfigChangeTracker(data_dir=getattr(cfg, "data_dir", "data"))
    ro_broker = tracker.get_readonly_broker(env_path=env_path) if action != "status" else None
    res = tracker.control_service(
        action=action,
        reason=reason,
        confirm_live=confirm_live,
        env_path=env_path,
        broker=ro_broker,
    )
    status_code = 200 if (res.get("ok") or res.get("deferred") or res.get("requires_live_confirmation")) else 400
    return JSONResponse(status_code=status_code, content=res)


@app.get("/setup/active-version")
def get_active_version():
    """CV13-06G: Returns currently running configuration version and latest change record."""
    cfg = AppConfig.from_env()
    tracker = ConfigChangeTracker(data_dir=getattr(cfg, "data_dir", "data"))
    info = tracker.get_active_running_version() or {}
    svc_state, _ = tracker.resolve_service_operating_state()
    is_active = (svc_state.value == "RUNNING")
    latest_rec = tracker.get_change_record()
    return JSONResponse(content={
        "version_hash": info.get("version_hash", "UNKNOWN"),
        "short_version": info.get("short_version", "v-UNKNOWN"),
        "trading_mode": info.get("trading_mode", "UNKNOWN"),
        "service_running": is_active,
        "service_state": svc_state.value,
        "acknowledged_at": info.get("acknowledged_at"),
        "latest_change": latest_rec.to_dict() if latest_rec else None,
    })


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
    dhan_response_group_name: str = Form(""),
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
    equity_cmp_execution_policy: Optional[str] = Form(None),
    fno_capital_per_trade: float = Form(20000.0),
    fno_max_lots: int = Form(3),
    # Risk
    max_order_amount_inr: float = Form(50000.0),
    min_trade_funds_inr: float = Form(10000.0),
    test_mode_paper_funds_inr: float = Form(100000.0),
    max_daily_loss_inr: float = Form(15000.0),
    max_daily_trades: Optional[int] = Form(None),
    daily_trade_limit: Optional[int] = Form(None),
    max_drawdown_pct: Optional[float] = Form(None),
    max_total_exposure_inr: float = Form(150000.0),
    max_open_positions: int = Form(5),
    # Protection & Timing
    equity_sl_pct: float = Form(1.0),
    equity_target_pct: float = Form(1.0),
    fno_sl_pct: float = Form(3.0),
    fno_target_pct: float = Form(3.0),
    sl_pct: float = Form(3.0),
    target_pct: float = Form(3.0),
    sl_limit_buffer_pct: float = Form(1.0),
    max_message_age_sec: float = Form(60.0),
    dedup_window_sec: float = Form(20.0),
    max_quote_age_sec: float = Form(2.0),
    csrf_token: Optional[str] = Form(None),
    expected_revision: Optional[str] = Form(None),
    request_id: Optional[str] = Form(None),
    confirm_live: Optional[str] = Form(None),
    submitted: dict = Depends(_submitted_webui_form),
    tg_bot_token: str = Form(""),
    equity_sizing_mode: str = Form("BUDGET"),
    equity_fixed_qty: int = Form(10),
    fno_sizing_mode: str = Form("BUDGET"),
    fno_fixed_lots: int = Form(1),
    allow_next_month_expiry_fallback: str = Form("true"),
    insufficient_funds_policy: str = Form("HARD_REJECT"),
    order_timeout_sec: float = Form(60.0),
    partial_fill_retry_timeout_sec: float = Form(2.0),
    scrip_master_max_age_sec: float = Form(86400.0),
):
    raw_csrf = _extract_val(csrf_token, None)
    if raw_csrf != SETUP_CSRF_TOKEN:
        return HTMLResponse(status_code=403, content="Missing or invalid CSRF token")
    expected = _extract_val(expected_revision, None)
    if not expected:
        return HTMLResponse(status_code=409, content="Setup page revision missing. Refresh the page before saving.")
    unknown = set(submitted) - set(REGISTRY) - REQUEST_METADATA - {"dhan_access_token", "daily_trade_limit"}
    if unknown:
        return HTMLResponse(status_code=422, content="Unsupported WebUI form fields: " + escape_html(", ".join(sorted(unknown))))
    editable = {k: v for k, v in submitted.items()
                if k in REGISTRY or k in ("dhan_access_token", "daily_trade_limit")}
    if submitted.get("full_setup_form") == "1":
        for key in ("is_live_mode", "equity_active", "fno_active"):
            editable[key] = key in submitted
    if submitted.get("confirm_live") is not None:
        editable["confirm_live"] = submitted["confirm_live"]
    from src.engine.simple_save_apply import apply_settings
    try:
        record = apply_settings(
            editable, env_path=".env", expected_revision=str(expected),
            confirm_live=str(submitted.get("confirm_live") or "").lower() in ("1", "true", "on", "yes"),
        )
    except ConfigurationConflict as exc:
        return HTMLResponse(status_code=409, content=escape_html(str(exc)))
    except (ConfigurationRejected, ValueError) as exc:
        return HTMLResponse(status_code=422, content=escape_html(str(exc)))
    new_cfg = AppConfig.from_env(env=parse_env_dict(".env"), env_file="")
    return get_setup_page(saved=True, saved_info={
        "mode": new_cfg.trading_mode.value,
        "operator": new_cfg.operator_name,
        "instance": new_cfg.instance_name,
        "dhan_id": new_cfg.dhan_client_id,
        "log_group": new_cfg.log_group_id,
        "service_restarted": record.restart_performed,
        "state": record.status,
        "reason": record.message,
        "version_hash": record.revision[:8],
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    host = os.environ.get("WEBUI_HOST", "127.0.0.1")
    if "--port" in sys.argv:
        idx = sys.argv.index("--port")
        if idx + 1 < len(sys.argv):
            port = int(sys.argv[idx + 1])
    elif len(sys.argv) > 1 and sys.argv[1].isdigit():
        port = int(sys.argv[1])
    if "--host" in sys.argv:
        idx = sys.argv.index("--host")
        if idx + 1 < len(sys.argv):
            host = sys.argv[idx + 1]
    print(f"🚀 Starting Audit bot Setup Dashboard on http://{host}:{port}")
    uvicorn.run(app, host=host, port=port)
