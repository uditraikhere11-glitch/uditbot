"""Proposed AuditBot Control Center presentation, for isolated integration review.

This module is pure HTML/JS rendering. Importing it never changes a service,
credential, configuration, broker order, or running trading process.

BACKEND CONTRACT (implemented by the authenticated Control Center facade):
  GET  /control/status
  GET  /control/health
  GET  /control/amo/orders
  POST /control/amo/equity
  POST /control/amo/fno
  POST /control/amo/fno/cancel
  POST /setup/service-control  (already exists)

F&O AMO sends an entry-only LIMIT order directly to Dhan's documented /orders
API after hours. Dhan's order ID confirms acknowledgement, NOT execution.
Local queued entries from older builds are never automatically dispatched.
"""

from html import escape
from src.premium_ui import premium_styles, premium_nav, premium_nav_js, premium_instance_name


def render_control_center(csrf_token: str) -> str:
    """Return HTML for the Control Center without executing any side effects."""
    safe_token = escape(str(csrf_token), quote=True)
    safe_brand = escape(premium_instance_name(), quote=True)
    return (_HTML.replace("__BOT_BRAND__", safe_brand)
            .replace("__CSRF_TOKEN__", safe_token)
            .replace("<!--PREMIUM_STYLES-->", premium_styles())
            .replace("<!--PREMIUM_NAV-->", premium_nav("control"))
            .replace("/*PREMIUM_NAV_JS*/", premium_nav_js()))


_HTML = r'''<!doctype html>
<html lang="en" class="premium-control">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="control-csrf" content="__CSRF_TOKEN__">
<title>__BOT_BRAND__ — Control Center</title>
<style>
:root{font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color-scheme:dark;
  --bg:#0b0f19;--panel:#111726;--muted:#94a3b8;--edge:#26334d;--text:#f1f5f9;
  --green:#34d399;--red:#fb7185;--amber:#fbbf24;--blue:#60a5fa}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text)}
main{max-width:960px;margin:25px auto;padding:0 18px 45px}
header{display:flex;align-items:center;justify-content:space-between;gap:14px;margin-bottom:22px}
.logo{display:flex;align-items:center;gap:13px}.logo-icon{display:grid;place-items:center;
  height:48px;width:48px;background:#1d4ed8;border-radius:13px;font-size:25px}
h1{margin:0;font-size:24px;letter-spacing:-.7px}h2{font-size:16px;margin:0 0 16px}
.subtitle,small,.muted{color:var(--muted)}.subtitle{font-size:13px;margin-top:4px}
a.setup,button{border:1px solid var(--edge);border-radius:10px;color:var(--text);
  background:#1c2739;font-size:14px;font-weight:600;padding:12px 16px;text-decoration:none;cursor:pointer}
button:disabled{opacity:.43;cursor:not-allowed}button:focus-visible,a:focus-visible,input:focus-visible{outline:2px solid var(--blue);outline-offset:3px}
button:hover:not(:disabled),a.setup:hover{background:#27364f}
section{background:var(--panel);border:1px solid var(--edge);border-radius:17px;padding:22px;margin-bottom:15px}
.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}
.tile{border:1px solid var(--edge);background:#172033;border-radius:12px;padding:16px;min-height:113px}
.label{font-size:12px;color:var(--muted);margin-bottom:9px}.value{font-size:20px;font-weight:700;overflow-wrap:anywhere}
.detail{font-size:12px;color:var(--muted);margin-top:6px;line-height:1.45}
.status-line{display:flex;align-items:center;gap:11px;min-height:26px}
.status-beacon{display:none;position:relative;flex:0 0 11px;width:11px;height:11px;border-radius:50%;
  background:radial-gradient(circle at 32% 28%,#dcffef 0%,#71ecb4 33%,#1ebf7d 76%);
  box-shadow:0 0 0 3px rgba(52,211,153,.12),0 0 12px rgba(52,211,153,.32)}
.status-beacon.is-active{display:inline-block;animation:verified-beacon-glow 2.8s ease-in-out infinite}
.status-beacon.is-active::after{content:"";position:absolute;top:1px;left:2px;width:4px;height:3px;border-radius:50%;background:#ffffffd9}
@keyframes verified-beacon-glow{
  0%,100%{box-shadow:0 0 0 3px rgba(52,211,153,.12),0 0 12px rgba(52,211,153,.32)}
  50%{box-shadow:0 0 0 4px rgba(52,211,153,.16),0 0 18px rgba(52,211,153,.62)}
}
@media(prefers-reduced-motion:reduce){.status-beacon.is-active{animation:none}}
.buttons{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-top:15px}
.btn-start{background:#064e3b;color:#d1fae5;border-color:#047857}.btn-stop{background:#571e29;color:#ffe4e6;border-color:#9f1239}
.row{display:flex;justify-content:space-between;align-items:center;gap:16px;padding:15px 0;border-top:1px solid var(--edge)}
.row:first-of-type{border-top:0;padding-top:0}.row-title{font-weight:650;font-size:15px}.row-sub{font-size:12px;color:var(--muted);margin-top:4px}
.toggle{height:24px;width:44px;accent-color:#34d399;cursor:pointer}.toggle:disabled{cursor:not-allowed;opacity:.55}
#info,#health-state,#amo-feedback{margin-top:14px;font-size:13px;line-height:1.45;color:var(--muted)}
.status-note{padding:12px 14px;background:#172033;border:1px solid var(--edge);border-radius:10px}
#checks,#orders{display:grid;gap:8px;margin-top:12px}.check,.order{padding:11px 12px;border-radius:10px;background:#172033;border:1px solid var(--edge);
  font-size:13px;display:flex;justify-content:space-between;gap:16px}.check .muted,.order .muted{max-width:68%;text-align:right}
.good{color:var(--green)}.bad{color:var(--red)}.warn{color:var(--amber)}.section-header{display:flex;justify-content:space-between;align-items:center;gap:12px}
.counts{font-size:13px;color:var(--muted)}.mode{font-size:13px;color:var(--muted)}
@media(max-width:560px){main{padding:0 12px}section{padding:17px}.buttons{grid-template-columns:1fr}header{align-items:start}h1{font-size:21px}.tile{padding:12px}.value{font-size:17px}}
</style>
<!--PREMIUM_STYLES-->
</head>
<body><!--PREMIUM_NAV--><main>
<header>
  <div class="logo"><div class="logo-icon" aria-hidden="true">🤖</div><div>
    <h1>__BOT_BRAND__</h1><div class="subtitle">Control Center · Verified status, never assumed</div>
  </div></div>
  <div style="display:flex;gap:8px;align-items:center"><button id="premNavOpen" class="prem-nav-toggle" type="button" aria-controls="premNav" aria-expanded="false">☰ Menu</button><a class="setup" href="/setup" aria-label="Open full Setup page">⚙ Setup</a><form action="/logout" method="post"><input type="hidden" name="csrf_token" value="__CSRF_TOKEN__"><button type="submit">Logout</button></form></div>
</header>
<nav class="v17-view-nav" aria-label="Trading workspace views">
  <a href="#dashboard" data-v17-view="dashboard">⌂ Dashboard</a>
  <a href="#control" data-v17-view="control" aria-current="page">◉ Trading Controls</a>
  <a href="/setup">⚙ Setup</a>
  <a href="/setup/telegram-designer">✉ Telegram</a>
  <a href="#amo" data-v17-view="amo">▣ AMO Orders</a>
  <a href="#health" data-v17-view="health">♡ System Health</a>
</nav>
<section id="v17-dashboard" class="v17-view-panel v17-dashboard" aria-label="Trading dashboard" hidden>
  <div class="v17-page-heading"><h2>Trading Dashboard</h2><p>Verified service and configuration overview. Financial metrics are not provided by this read-only route.</p></div>
  <div class="v17-summary-grid">
    <div class="v17-summary-card"><span class="v17-summary-icon" aria-hidden="true">◉</span><span class="v17-summary-label">Bot service</span><strong id="v17-db-service">VERIFYING</strong><small>From authenticated service status</small></div>
    <div class="v17-summary-card"><span class="v17-summary-icon" aria-hidden="true">▣</span><span class="v17-summary-label">Trading mode</span><strong id="v17-db-mode">UNVERIFIED</strong><small>Running mode only when verified</small></div>
    <div class="v17-summary-card"><span class="v17-summary-icon" aria-hidden="true">▤</span><span class="v17-summary-label">Equity AMO</span><strong id="v17-db-equity">UNVERIFIED</strong><small>Saved setting, not a broker fill</small></div>
    <div class="v17-summary-card"><span class="v17-summary-icon" aria-hidden="true">◇</span><span class="v17-summary-label">F&amp;O AMO</span><strong id="v17-db-fno">UNVERIFIED</strong><small>Entry only; no automatic protection</small></div>
    <div class="v17-summary-card"><span class="v17-summary-icon" aria-hidden="true">✓</span><span class="v17-summary-label">Runtime evidence</span><strong id="v17-db-verified">PENDING</strong><small>Never inferred from saved settings</small></div>
  </div>
  <h2 class="v17-quick-title">Quick Access</h2>
  <div class="v17-quick-grid">
    <a class="v17-quick-tile" href="#control"><span aria-hidden="true">▷</span><strong>Trading Controls</strong><small>Start, Stop and verify service</small><b aria-hidden="true">›</b></a>
    <a class="v17-quick-tile" href="/setup"><span aria-hidden="true">⚙</span><strong>Configuration</strong><small>Review the native Setup form</small><b aria-hidden="true">›</b></a>
    <a class="v17-quick-tile" href="#health"><span aria-hidden="true">♡</span><strong>System Health</strong><small>Read-only safety checks</small><b aria-hidden="true">›</b></a>
    <a class="v17-quick-tile" href="#amo"><span aria-hidden="true">▣</span><strong>AMO Orders</strong><small>Inspect settings and journal</small><b aria-hidden="true">›</b></a>
    <a class="v17-quick-tile" href="/setup/telegram-designer"><span aria-hidden="true">✉</span><strong>Telegram Designer</strong><small>Edit supported message templates</small><b aria-hidden="true">›</b></a>
  </div>
</section>
<section id="v17-control" class="v17-view-panel" aria-label="Bot operating state">
  <div class="grid">
    <div class="tile"><div class="label">Bot service</div>
      <div class="status-line"><span id="service-beacon" class="status-beacon" aria-hidden="true"></span><div id="service" class="value">Checking…</div></div>
      <div id="service-detail" class="detail">Verifying VM service state</div></div>
    <div class="tile"><div class="label">Trading mode</div>
      <div class="status-line"><span id="mode-beacon" class="status-beacon" aria-hidden="true"></span><div id="mode" class="value">UNKNOWN</div></div>
      <div id="mode-detail" class="detail">Running configuration pending verification</div></div>
  </div>
  <div class="buttons">
    <button id="start" class="btn-start" type="button" disabled>▶ Start Bot</button>
    <button id="stop" class="btn-stop" type="button" disabled>■ Stop Bot</button>
    <button id="health" type="button">♡ Health Check</button>
  </div>
  <div id="info" class="status-note" role="status" aria-live="polite">Read-only status is loading. No action has been taken.</div>

</section>
<section id="v17-amo-settings" class="v17-view-panel" aria-label="After market order controls" hidden>
  <h2>After Market Order Settings</h2>
  <div class="row"><div><div class="row-title">Equity AMO</div>
    <div class="row-sub">NSE cash equity · CNC LIMIT · dispatch at OPEN</div></div>
    <input id="equity-amo" class="toggle" type="checkbox" role="switch" aria-label="Enable Equity AMO" disabled>
  </div>
  <div class="row"><div><div class="row-title">F&amp;O AMO</div>
    <div class="row-sub">NSE stock options · broker AMO at OPEN · entry only (no SL/TP)</div></div>
    <input id="fno-amo" class="toggle" type="checkbox" role="switch" aria-label="Enable F&O AMO" disabled>
  </div>
  <div class="status-note">AMO preference: <b>Market Open</b>. Both Equity and F&amp;O send orders to Dhan immediately after hours. Dhan order ID confirms receipt, not a fill. F&amp;O AMO has NO automatic SL/target; review any filled position manually. Legacy local F&amp;O entries are never dispatched. Switching OFF does not cancel submitted Dhan orders.</div>
  <div id="amo-feedback" role="status" aria-live="polite">AMO settings have not been changed.</div>
</section>
<section id="amo-orders" class="v17-view-panel" aria-label="AMO order reconciliation" hidden>
  <div class="section-header"><h2>AMO Order Status</h2><button id="refresh-orders" type="button">↻ Refresh</button></div>
  <div id="order-counts" class="counts">Waiting for verified order information…</div>
  <div id="orders" aria-live="polite"></div>
</section>
<section id="system-health" class="v17-view-panel" aria-label="Health check results" hidden>
  <div class="section-header"><h2>System Health</h2><div class="v17-health-actions"><small>Read-only checks</small><button id="v17-health-run" type="button">♡ Run Health Check</button></div></div>
  <div id="health-state" role="status" aria-live="polite">Select Health Check to check service, broker, token, Telegram and safety gates.</div>
  <div id="checks"></div>
</section>
</main>
<script>
'use strict';
/*PREMIUM_NAV_JS*/
const $=id=>document.getElementById(id);
const csrf=document.querySelector('meta[name="control-csrf"]').content;
/* View-only navigation: preserve the original controls and authenticated routes. */
const v17ViewNames = new Set(['dashboard','control','amo','health']);
function v17ShowView() {
  const requested=window.location.hash.slice(1).toLowerCase();
  const active=v17ViewNames.has(requested)?requested:'control';
  for (const id of ['dashboard','control','amo-settings','amo-orders','system-health']) {
    const el=$(id==='dashboard'?'v17-dashboard':id==='control'?'v17-control':id==='amo-settings'?'v17-amo-settings':id);
    if (!el) continue;
    el.hidden = active==='amo' ? !(id==='amo-settings'||id==='amo-orders')
      : active==='health' ? id!=='system-health' : id!==active;
  }
  for(const a of document.querySelectorAll('[data-v17-view]')) {
    if(a.dataset.v17View===active)a.setAttribute('aria-current','page');
    else a.removeAttribute('aria-current');
  }
}
window.addEventListener('hashchange',v17ShowView);
v17ShowView();

let current=null;
const setText=(id,value)=>{$(id).textContent=String(value??'UNKNOWN')};
function safeStatus(status){
  return (typeof status==='string'&&status.trim())?status.trim():'UNKNOWN';
}
async function requestJSON(url,options={}){
  const response=await fetch(url,{credentials:'same-origin',cache:'no-store',...options});
  // A session-expiry redirect may return the HTML login page with HTTP 200.
  // Never interpret that as an authenticated control response.
  if(response.redirected && /\/login(?:[?#]|$)/.test(String(response.url||''))){
    throw new Error('WebUI session expired; sign in again before retrying.');
  }
  let body;
  try{body=await response.json()}
  catch(_err){throw new Error('Control response was not JSON; check your login session and retry.');}
  if(!body || typeof body!=='object' || Array.isArray(body)){
    throw new Error('Control response was invalid; no action verified.');
  }
  if(!response.ok){throw new Error(String(body.error||body.message||'HTTP '+response.status))}
  return body;
}
async function refreshStatus(){
  try{
    const s=await requestJSON('/control/status');current=s;
    const state=safeStatus(s.service_state);
    setText('service',state.replace(/_/g,' '));
    // The glow is a verified-status indicator, not a substitute for Health Check.
    const verifiedRunning=state==='RUNNING'&&s.runtime_verified===true;
    $('service-beacon').classList.toggle('is-active',verifiedRunning);
    setText('service-detail',s.service_detail||'Verified state from trading_bot.service');
    const mode=(s.runtime_verified===true)?safeStatus(s.running_mode):'UNVERIFIED';
    setText('mode',mode);
    $('mode-beacon').classList.toggle('is-active',verifiedRunning&&mode==='LIVE');
    setText('mode-detail',s.runtime_verified===true?'Active running configuration verified':'Saved mode: '+safeStatus(s.saved_mode));
    $('start').disabled=!['STOPPED_BY_OWNER','STOPPED_OTHER','FAILED'].includes(state)||s.transition_gate_active===true||s.tracker_v2_apply_blocked===true;
    $('stop').disabled=state!=='RUNNING';
    $('equity-amo').checked=s.equity_amo_saved===true;
    $('equity-amo').disabled=s.equity_amo_can_edit!==true;
    $('fno-amo').checked=s.fno_amo_saved===true;
    $('fno-amo').disabled=s.fno_amo_can_edit!==true;
    setText('amo-feedback',
      'Equity AMO saved: '+(s.equity_amo_saved?'ON':'OFF')+
      ' · F&O AMO saved: '+(s.fno_amo_saved?'ON':'OFF')+
      ' · F&O runtime: '+(s.fno_amo_active===true?'ON':s.fno_amo_active===false?'OFF':'UNVERIFIED')+
      ' · F&O route: broker native AMO /orders (entry only)');
    setText('v17-db-service',state.replace(/_/g,' '));
    setText('v17-db-mode',mode);
    setText('v17-db-equity',s.equity_amo_saved===true?'SAVED ON':s.equity_amo_saved===false?'SAVED OFF':'UNKNOWN');
    setText('v17-db-fno',s.fno_amo_saved===true?'SAVED ON':s.fno_amo_saved===false?'SAVED OFF':'UNKNOWN');
    setText('v17-db-verified',s.runtime_verified===true&&state==='RUNNING'?'VERIFIED':'UNVERIFIED');
    setText('info',s.warning||('Service: '+state+' · saved: '+safeStatus(s.saved_mode)+' · running: '+mode));
  }catch(err){current=null;$('start').disabled=true;$('stop').disabled=true;
    $('service-beacon').classList.remove('is-active');$('mode-beacon').classList.remove('is-active');
    $('equity-amo').disabled=true;$('fno-amo').disabled=true;setText('service','UNKNOWN');setText('mode','UNVERIFIED');
    for(const id of ['v17-db-service','v17-db-mode','v17-db-equity','v17-db-fno','v17-db-verified'])setText(id,'UNVERIFIED');
    setText('info','Cannot verify bot state: '+err.message+' (controls disabled)')}
}
async function serviceAction(action){
  if(!current)return;
  const live=(current.saved_mode==='LIVE');
  if(action==='start'&&live&&!confirm('Start UDITBOT in LIVE mode? Real Dhan orders may be placed.'))return;
  if(action==='stop'&&!confirm('Request a safe bot stop? Existing live orders may require reconciliation.'))return;
  $('start').disabled=true;$('stop').disabled=true;setText('info','Requesting '+action+'; awaiting verification…');
  let resultMessage='';
  try{
    const r=await requestJSON('/setup/service-control',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({action,confirm_live:action==='start'&&live,csrf_token:csrf,reason:'Owner requested '+action+' from Control Center'})});
    resultMessage=r.message||r.error||('Request '+action+' received.');
    if(r.requires_live_confirmation){
      resultMessage='LIVE action requires explicit confirmation; no action performed.';
    }else if(r.deferred===true){
      resultMessage='Stop deferred: '+(r.message||r.blocked_reason||'live order safety gate');
    }
  }catch(err){resultMessage='Action was not verified: '+err.message}
  // Always reacquire the authoritative state, including rejected/confirmation
  // responses. The earlier early-return could leave Start and Stop disabled.
  await refreshStatus();
  // If the follow-up status probe fails, retain the fail-closed warning.
  // A mutation acknowledgement alone does not verify the runtime state.
  if(current && resultMessage)setText('info',resultMessage);
}
$('start').addEventListener('click',()=>serviceAction('start'));
$('stop').addEventListener('click',()=>serviceAction('stop'));
$('v17-health-run').addEventListener('click',()=>{ $('health').click(); });
$('health').addEventListener('click',async()=>{
  $('health').disabled=true;$('v17-health-run').disabled=true;setText('health-state','Running read-only checks…');$('checks').replaceChildren();
  try{
    const r=await requestJSON('/control/health');
    setText('health-state',r.summary||'Health checks returned');
    for(const check of (Array.isArray(r.checks)?r.checks:[])){
      const el=document.createElement('div');el.className='check';
      const name=document.createElement('strong');name.textContent=String(check.name||'Check');
      const detail=document.createElement('span');detail.className='muted';
      detail.textContent=String(check.status||'UNKNOWN')+' · '+String(check.detail||'');
      el.append(name,detail);$('checks').append(el);
    }
  }catch(err){setText('health-state','Health check unavailable: '+err.message)}
  $('health').disabled=false;$('v17-health-run').disabled=false;
});
$('equity-amo').addEventListener('change',async()=>{
  const requested=$('equity-amo').checked;
  if(!current){$('equity-amo').checked=!requested;return}
  $('equity-amo').disabled=true;setText('amo-feedback','Saving Equity AMO settings…');
  try{
    const r=await requestJSON('/control/amo/equity',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({enabled:requested,confirm_live:current.saved_mode==='LIVE'&&confirm('Restart LIVE bot to apply this AMO setting?'),csrf_token:csrf,expected_revision:current.config_file_revision,request_id:(window.crypto&&window.crypto.randomUUID?window.crypto.randomUUID():'AMO-'+Date.now()+'-'+Math.random().toString(36).slice(2))})});
    setText('amo-feedback',r.message);
    if (r.state !== 'UNCHANGED') window.alert(r.message);
  }catch(err){setText('amo-feedback','Equity AMO change/status verification failed: '+err.message)}
  await refreshStatus();
});
$('fno-amo').addEventListener('change',async()=>{
  const requested=$('fno-amo').checked;
  if(!current){$('fno-amo').checked=!requested;return}
  if(requested&&!confirm('Enable broker-native F&O AMO? Eligible NSE stock option orders will be sent to Dhan after hours. Dhan will return a real Order ID if acknowledged. NO automatic SL or target will protect a filled position.')) {
    $('fno-amo').checked=false;return;
  }
  $('fno-amo').disabled=true;setText('amo-feedback','Saving F&O AMO settings…');
  try{
    const r=await requestJSON('/control/amo/fno',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({enabled:requested,confirm_live:current.saved_mode==='LIVE'&&confirm('Restart LIVE bot to apply this AMO setting?'),csrf_token:csrf,expected_revision:current.config_file_revision,request_id:(window.crypto&&window.crypto.randomUUID?window.crypto.randomUUID():'AMO-'+Date.now()+'-'+Math.random().toString(36).slice(2))})});
    setText('amo-feedback',r.message);
    if (r.state !== 'UNCHANGED') window.alert(r.message);
  }catch(err){setText('amo-feedback','F&O AMO change not verified: '+err.message)}
  await refreshStatus();
});
async function refreshOrders(){
  $('refresh-orders').disabled=true;$('orders').replaceChildren();
  try{
    const r=await requestJSON('/control/amo/orders');
    setText('order-counts',r.summary||'Broker and journal AMO records');
    for(const order of (Array.isArray(r.orders)?r.orders:[])){
      const el=document.createElement('div');el.className='order';
      const left=document.createElement('span');left.textContent=String(order.symbol||'Unknown instrument');
      const right=document.createElement('span');right.className='muted';
      right.textContent=String(order.status||'UNKNOWN')+' · '+String(order.protection||'Protection unverified');
      el.append(left,right);$('orders').append(el);
    }
    for(const item of (Array.isArray(r.fno_local_queue)?r.fno_local_queue:[])){
      const el=document.createElement('div');el.className='order';
      const label=document.createElement('span');
      label.textContent='F&O '+String(item.symbol||'Unknown')+' · '+String(item.state||'UNKNOWN')+
        ' · LEGACY local record · '+(item.state==='QUEUED'?'NOT DISPATCHED — review or cancel':'Never auto-dispatched by this patch');
      el.append(label);
      if(item.state==='QUEUED'){
        const button=document.createElement('button');button.type='button';
        button.textContent='Cancel legacy staged';
        button.addEventListener('click',async()=>{
          if(!confirm('Cancel this legacy local record? This does not cancel any Dhan orders.'))return;
          button.disabled=true;
          try{
            const out=await requestJSON('/control/amo/fno/cancel',{method:'POST',
              headers:{'Content-Type':'application/json'},
              body:JSON.stringify({item_id:String(item.item_id),csrf_token:csrf})});
            setText('order-counts',String(out.message||'Cancellation recorded'));
          }catch(err){setText('order-counts','F&O local cancellation unverified: '+err.message)}
          await refreshOrders();
        });
        el.append(button);
      }
      $('orders').append(el);
    }
  }catch(err){setText('order-counts','Cannot verify AMO orders: '+err.message)}
  $('refresh-orders').disabled=false;
}
$('refresh-orders').addEventListener('click',refreshOrders);
refreshStatus();refreshOrders();
setInterval(refreshStatus,20000);
</script>
</body></html>'''
