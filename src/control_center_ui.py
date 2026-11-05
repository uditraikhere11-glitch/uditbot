"""Proposed AuditBot Control Center presentation, for isolated integration review.

This module is pure HTML/JS rendering. Importing it never changes a service,
credential, configuration, broker order, or running trading process.

BACKEND CONTRACT (endpoints are NOT implemented by this proposal):
  GET  /control/status
  GET  /control/health
  GET  /control/amo/orders
  POST /control/amo/equity
  POST /setup/service-control  (already exists)

F&O AMO intentionally remains noninteractive until the protected Dhan API route
is independently verified and approved.
"""

from html import escape


def render_control_center(csrf_token: str) -> str:
    """Return HTML for the Control Center without executing any side effects."""
    safe_token = escape(str(csrf_token), quote=True)
    return _HTML.replace("__CSRF_TOKEN__", safe_token)


_HTML = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="control-csrf" content="__CSRF_TOKEN__">
<title>UDITBOT — Control Center</title>
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
</head>
<body><main>
<header>
  <div class="logo"><div class="logo-icon" aria-hidden="true">🤖</div><div>
    <h1>UDITBOT</h1><div class="subtitle">Control Center · Verified status, never assumed</div>
  </div></div>
  <a class="setup" href="/setup" aria-label="Open full Setup page">⚙ Setup</a>
</header>
<section aria-label="Bot operating state">
  <div class="grid">
    <div class="tile"><div class="label">Bot service</div><div id="service" class="value">Checking…</div>
      <div id="service-detail" class="detail">Verifying VM service state</div></div>
    <div class="tile"><div class="label">Trading mode</div><div id="mode" class="value">UNKNOWN</div>
      <div id="mode-detail" class="detail">Running configuration pending verification</div></div>
  </div>
  <div class="buttons">
    <button id="start" class="btn-start" type="button" disabled>▶ Start Bot</button>
    <button id="stop" class="btn-stop" type="button" disabled>■ Stop Bot</button>
    <button id="health" type="button">♡ Health Check</button>
  </div>
  <div id="info" class="status-note" role="status" aria-live="polite">Read-only status is loading. No action has been taken.</div>
</section>
<section aria-label="After market order controls">
  <h2>After Market Order Settings</h2>
  <div class="row"><div><div class="row-title">Equity AMO</div>
    <div class="row-sub">NSE cash equity · CNC LIMIT · dispatch at OPEN</div></div>
    <input id="equity-amo" class="toggle" type="checkbox" role="switch" aria-label="Enable Equity AMO" disabled>
  </div>
  <div class="row"><div><div class="row-title">F&amp;O AMO</div>
    <div class="row-sub">Stock options · disabled until protected Dhan API route is verified</div></div>
    <input id="fno-amo" class="toggle" type="checkbox" role="switch" aria-label="Enable F&O AMO" disabled>
  </div>
  <div class="status-note">AMO dispatch preference: <b>Market Open</b>. Enabling a switch never changes Emergency STOP or LIVE/TEST mode.</div>
  <div id="amo-feedback" role="status" aria-live="polite">AMO settings have not been changed.</div>
</section>
<section aria-label="AMO order reconciliation">
  <div class="section-header"><h2>AMO Order Status</h2><button id="refresh-orders" type="button">↻ Refresh</button></div>
  <div id="order-counts" class="counts">Waiting for verified order information…</div>
  <div id="orders" aria-live="polite"></div>
</section>
<section aria-label="Health check results">
  <div class="section-header"><h2>System Health</h2><small>Read-only checks</small></div>
  <div id="health-state" role="status" aria-live="polite">Select Health Check to check service, broker, token, Telegram and safety gates.</div>
  <div id="checks"></div>
</section>
</main>
<script>
'use strict';
const $=id=>document.getElementById(id);
const csrf=document.querySelector('meta[name="control-csrf"]').content;
let current=null;
const setText=(id,value)=>{$(id).textContent=String(value??'UNKNOWN')};
function safeStatus(status){
  return (typeof status==='string'&&status.trim())?status.trim():'UNKNOWN';
}
async function requestJSON(url,options={}){
  const response=await fetch(url,{credentials:'same-origin',cache:'no-store',...options});
  const body=await response.json();
  if(!response.ok){throw new Error(String(body.error||body.message||'HTTP '+response.status))}
  return body;
}
async function refreshStatus(){
  try{
    const s=await requestJSON('/control/status');current=s;
    const state=safeStatus(s.service_state);
    setText('service',state.replace(/_/g,' '));
    setText('service-detail',s.service_detail||'Verified state from trading_bot.service');
    const mode=(s.runtime_verified===true)?safeStatus(s.running_mode):'UNVERIFIED';
    setText('mode',mode);
    setText('mode-detail',s.runtime_verified===true?'Active running configuration verified':'Saved mode: '+safeStatus(s.saved_mode));
    $('start').disabled=!['STOPPED_BY_OWNER','STOPPED_OTHER','FAILED'].includes(state)||s.transition_gate_active===true;
    $('stop').disabled=state!=='RUNNING';
    $('equity-amo').checked=s.equity_amo_saved===true;
    $('equity-amo').disabled=s.equity_amo_can_edit!==true;
    setText('amo-feedback',s.equity_amo_message||('Equity AMO saved: '+(s.equity_amo_saved?'ON':'OFF')+
      ' · active: '+(s.equity_amo_active===true?'ON':s.equity_amo_active===false?'OFF':'UNVERIFIED')));
    setText('info',s.warning||('Service: '+state+' · saved: '+safeStatus(s.saved_mode)+' · running: '+mode));
  }catch(err){current=null;$('start').disabled=true;$('stop').disabled=true;
    $('equity-amo').disabled=true;setText('service','UNKNOWN');setText('mode','UNVERIFIED');
    setText('info','Cannot verify bot state: '+err.message+' (controls disabled)')}
}
async function serviceAction(action){
  if(!current)return;
  const live=(current.saved_mode==='LIVE');
  if(action==='start'&&live&&!confirm('Start UDITBOT in LIVE mode? Real Dhan orders may be placed.'))return;
  if(action==='stop'&&!confirm('Request a safe bot stop? Existing live orders may require reconciliation.'))return;
  $('start').disabled=true;$('stop').disabled=true;setText('info','Requesting '+action+'; awaiting verification…');
  try{
    const r=await requestJSON('/setup/service-control',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({action,confirm_live:action==='start'&&live,csrf_token:csrf,reason:'Owner requested '+action+' from Control Center'})});
    setText('info',r.message||r.error||('Request '+action+' received.'));
    if(r.requires_live_confirmation){
      setText('info','LIVE action requires explicit confirmation; no action performed.');
      return;
    }
    if(r.deferred===true)setText('info','Stop deferred: '+(r.message||r.blocked_reason||'live order safety gate'));
  }catch(err){setText('info','Action was not verified: '+err.message)}
  await refreshStatus();
}
$('start').addEventListener('click',()=>serviceAction('start'));
$('stop').addEventListener('click',()=>serviceAction('stop'));
$('health').addEventListener('click',async()=>{
  $('health').disabled=true;setText('health-state','Running read-only checks…');$('checks').replaceChildren();
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
  $('health').disabled=false;
});
$('equity-amo').addEventListener('change',async()=>{
  const requested=$('equity-amo').checked;
  if(!current){$('equity-amo').checked=!requested;return}
  $('equity-amo').disabled=true;setText('amo-feedback','Saving Equity AMO request through safe configuration coordinator…');
  try{
    const r=await requestJSON('/control/amo/equity',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({enabled:requested,csrf_token:csrf})});
    setText('amo-feedback',r.message||('Request received: '+String(r.state||'PENDING')));
    if(r.change_id){
      const requestedChangeId=encodeURIComponent(String(r.change_id));
      // Poll the authoritative coordinator result; a queued request is not applied.
      for(let i=0;i<15;i++){
        await new Promise(resolve=>setTimeout(resolve,1500));
        const rec=await requestJSON('/setup/change-status/'+requestedChangeId);
        const st=String(rec.state||'UNKNOWN');
        setText('amo-feedback','Equity AMO configuration: '+st+' · '+String(rec.reason||''));
        if(['APPLIED_VERIFIED','SAVED_FOR_NEXT_START','REJECTED','APPLY_FAILED'].includes(st))break;
      }
    }
  }catch(err){setText('amo-feedback','Equity AMO change/status verification failed: '+err.message)}
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
  }catch(err){setText('order-counts','Cannot verify AMO orders: '+err.message)}
  $('refresh-orders').disabled=false;
}
$('refresh-orders').addEventListener('click',refreshOrders);
refreshStatus();refreshOrders();
setInterval(refreshStatus,20000);
</script>
</body></html>'''