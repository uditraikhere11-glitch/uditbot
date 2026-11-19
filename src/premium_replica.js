/* Screenshot-matched Setup presentation. Moves existing named controls, never clones or changes their values. */
(function () {
  'use strict';
  const form = document.querySelector('form[action="/setup"]');
  if (!form || !document.getElementById('replica-grid')) return;
  const $ = id => document.getElementById(id);
  const sourceSections = [...form.querySelectorAll(':scope > .section-card')];
  const detail = $('replica-advanced');
  const advancedContent = $('replica-advanced-content');
  const hostMap = [
    ['is_live_mode', 'rp-mode'],
    ['equity_active', 'rp-equity-switch'],
    ['fno_active', 'rp-fno-switch'],
    ['monitored_channel_ids', 'rp-sources'],
    ['max_order_amount_inr', 'rp-order-cap'],
    ['equity_sl_pct', 'rp-equity-sl'],
    ['equity_target_pct', 'rp-equity-tp'],
    ['fno_max_lots', 'rp-fno-lots'],
    ['fno_capital_per_trade', 'rp-fno-budget'],
    ['equity_budget_inr', 'rp-equity-budget'],
    ['max_quote_age_sec', 'rp-quote-age'],
    ['equity_cmp_execution_policy', 'rp-price-policy'],
    ['scrip_master_max_age_sec', 'rp-scrip-refresh']
  ];
  for (const [name, target] of hostMap) {
    const input = form.elements.namedItem(name);
    const host = $(target);
    if (!input || !host) return; // Fail closed: keep original form layout intact if schema changes.
  }
  for (const [name, target] of hostMap) {
    const input = form.elements.namedItem(name);
    const field = input.closest('.form-group, .toggle-row');
    const host = $(target);
    if (field.classList.contains('form-group')) {
      while (field.firstChild) host.appendChild(field.firstChild);
      field.remove();
    } else {
      host.appendChild(field);
      field.classList.add('rp-moved-field');
    }
    host.querySelectorAll('.help-text, .toggle-info p').forEach(el => el.classList.add('rp-help'));
  }
  // Move the remaining real sections into disclosure, not hidden duplicate inputs.
  for (const section of sourceSections) advancedContent.appendChild(section);
  const niceLabels={
    monitored_channel_ids:'Alert Sources (channel IDs)',
    max_order_amount_inr:'Per-Order Limit (₹)',
    equity_sl_pct:'Stop Loss (Equity %)',
    equity_target_pct:'Target (Equity %)',
    fno_max_lots:'Max Lots (F&O)',
    fno_capital_per_trade:'F&O Budget (₹)',
    equity_budget_inr:'Equity Budget (₹)',
    max_quote_age_sec:'Quote Staleness (sec)',
    equity_cmp_execution_policy:'Price Source Policy',
    scrip_master_max_age_sec:'Scrip Master Refresh (sec)'
  };
  for (const [name,target] of hostMap){
    const label=$(target).querySelector('label');
    if(label&&niceLabels[name]) label.textContent=niceLabels[name];
  }
  const submit=form.querySelector('.btn-submit');
  $('rp-actions').appendChild(submit);
  form.addEventListener('reset',()=>setTimeout(syncMode,0));
  const eqLabel=$('rp-equity-switch').querySelector('.toggle-info h4');
  const fnoLabel=$('rp-fno-switch').querySelector('.toggle-info h4');
  if(eqLabel) eqLabel.textContent='Equity';
  if(fnoLabel) fnoLabel.textContent='F&O';
  const modeBox = form.elements.namedItem('is_live_mode');
  modeBox.tabIndex=-1;
  const modeText = $('rp-mode-label');
  const liveButton = $('rp-live-button'), testButton = $('rp-test-button');
  function syncMode() {
    const live = modeBox.checked;
    modeText.textContent = live ? 'LIVE' : 'TEST';
    liveButton.setAttribute('aria-pressed', String(live));
    testButton.setAttribute('aria-pressed', String(!live));
    liveButton.classList.toggle('selected', live);
    testButton.classList.toggle('selected', !live);
  }
  liveButton.addEventListener('click', () => { modeBox.checked = true; modeBox.dispatchEvent(new Event('change',{bubbles:true})); });
  testButton.addEventListener('click', () => { modeBox.checked = false; modeBox.dispatchEvent(new Event('change',{bubbles:true})); });
  modeBox.addEventListener('change',syncMode);syncMode();
  // Opening the disclosure on invalid hidden fields lets the browser focus them.
  form.addEventListener('invalid',e => {if (advancedContent.contains(e.target)) detail.open = true;},true);
  const topStatus = $('rp-top-service');
  const topMode = $('rp-top-mode');
  const topRecon = $('rp-top-recon');
  const topOco = $('rp-top-oco');
  const amoEq = $('rp-amo-equity'),amoFno=$('rp-amo-fno'),amoFeedback=$('rp-amo-feedback');
  let verifiedStatus = null;
  async function json(url,options={}) {
    const r = await fetch(url,{credentials:'same-origin',cache:'no-store',...options});
    let data;try {data=await r.json();} catch(_e){throw new Error('Non-JSON response (check Control Center login)');}
    if (!r.ok) throw new Error(String(data.message||data.error||'HTTP '+r.status));
    return data;
  }
  function setState(node,value,color) {
    node.textContent=value;
    node.dataset.state=color;
  }
  async function refresh() {
    amoEq.disabled=true;amoFno.disabled=true;
    try {
      const data=await json('/control/status');
      verifiedStatus=data;
      const running=data.service_state==='RUNNING' && data.runtime_verified===true;
      setState(topStatus,running?'Bot Running':data.service_state==='RUNNING'?'Service running · unverified':'Service: '+String(data.service_state||'Unknown').replaceAll('_',' '),running?'good':'neutral');
      setState(topMode,running?'Running: '+String(data.running_mode||'UNKNOWN'):'Saved: '+String(data.saved_mode||'UNKNOWN'),running?'good':'neutral');
      // The status endpoint does not independently attest these states.
      setState(topRecon,'Reconciliation: not verified','neutral');
      setState(topOco,'OCO Guard: not verified','neutral');
      amoEq.checked=data.equity_amo_saved===true;
      amoFno.checked=data.fno_amo_saved===true;
      amoEq.disabled=data.equity_amo_can_edit!==true;
      amoFno.disabled=data.fno_amo_can_edit!==true;
      amoFeedback.textContent='AMO values loaded from Control Center. Changes may require a verified restart.';
    } catch (err) {
      verifiedStatus=null;
      setState(topStatus,'Service: unknown','neutral');
      setState(topRecon,'Reconciliation: unknown','neutral');
      setState(topOco,'OCO Guard: unknown','neutral');
      amoFeedback.textContent='AMO control unavailable: '+err.message+' · Open Control Center for diagnosis.';
    }
  }
  async function changeAmo(which,checked) {
    if(!verifiedStatus) return;
    const target=which==='equity'?amoEq:amoFno;
    if(which==='fno' && checked && !confirm('F&O AMO may send orders after hours. A broker-acknowledged entry has NO automatic SL/target. Enable?')){
      target.checked=false;return;
    }
    const isLive=verifiedStatus.saved_mode==='LIVE';
    const confirmLive=isLive && confirm('Restart a LIVE trading service to apply AMO? Existing protection supervision may be interrupted.');
    target.disabled=true;
    amoFeedback.textContent='Submitting AMO change and verifying…';
    try {
      const payload={enabled:checked,confirm_live:confirmLive,
        csrf_token:form.elements.namedItem('csrf_token').value,
        expected_revision:verifiedStatus.config_file_revision,
        request_id:(window.crypto&&window.crypto.randomUUID?window.crypto.randomUUID():'UI-'+Date.now()+'-'+Math.random().toString(36).slice(2))};
      const reply=await json('/control/amo/'+which,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
      amoFeedback.textContent=String(reply.message||'Operation completed; verifying saved values.');
    } catch(err) {amoFeedback.textContent='AMO change not verified: '+err.message;}
    await refresh();
  }
  amoEq.addEventListener('change',()=>changeAmo('equity',amoEq.checked));
  amoFno.addEventListener('change',()=>changeAmo('fno',amoFno.checked));
  document.documentElement.classList.add('replica-ready');
  refresh();
  // No automatic service start, order submission, or config write.
}());
