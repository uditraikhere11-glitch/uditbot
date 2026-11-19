/* Screenshot-aligned Setup UI. Reuses the ONE original form; no hidden duplicate
   setting names, no broker calls, and no optimistic health claims. */
(function () {
  'use strict';
  document.addEventListener('DOMContentLoaded', () => {
    const form = document.querySelector('form[action="/setup"]');
    const overview = document.getElementById('dashOverview');
    const advanced = document.getElementById('dashAdvanced');
    const advancedBody = document.getElementById('dashAdvancedBody');
    const nav = document.getElementById('premNav');
    if (!form || !overview || !advanced || !advancedBody || !nav) return;
    const byId = id => document.getElementById(id);
    const single = name => {
      const fields = form.querySelectorAll('[name="' + name + '"]');
      if (fields.length !== 1) throw new Error('Missing or duplicate setting ' + name);
      return fields[0];
    };
    const moveNames = [
      'equity_sl_pct', 'equity_target_pct', 'fno_max_lots', 'fno_capital_per_trade',
      'equity_budget_inr', 'max_quote_age_sec', 'equity_cmp_execution_policy',
      'scrip_master_max_age_sec'
    ];
    const moves = [];
    try {
      for (const name of moveNames) {
        const el = single(name);
        const group = el.closest('.form-group');
        const slot = overview.querySelector('[data-move="' + name + '"]');
        if (!group || !slot || !group.closest('.section-card')) throw new Error('Layout contract: ' + name);
        moves.push({group, slot, label: slot.getAttribute('data-label')});
      }
      for (const name of ['is_live_mode','equity_active','fno_active','monitored_channel_ids']) single(name);
      for (const name of ['equity_active','fno_active']) {
        if (!single(name).closest('label.switch')) throw new Error('Missing checkbox switch ' + name);
      }
      if (form.querySelectorAll(':scope > .section-card').length !== 6) {
        throw new Error('Legacy Setup section count changed');
      }
      if (form.querySelectorAll('[name]').length < 25) throw new Error('Form contract incomplete');
    } catch (err) {
      // Fail safe: original full Setup form and its ordinary Save & Apply remain visible.
      console.error('Premium Setup unavailable; legacy form retained', err);
      return;
    }

    // Show the approved four-card layout only after every source control exists.
    for (const {group, slot, label} of moves) {
      slot.append(group); // MOVE, not clone: only one submitted field survives.
      const labelEl = group.querySelector('label');
      if (labelEl) labelEl.textContent = label;
    }
    for (const [name,id] of [['equity_active','dashEquitySlot'],['fno_active','dashFnoSlot']]) {
      const input = single(name);
      const switchLabel = input.closest('label.switch');
      const sourceRow = switchLabel && switchLabel.closest('.toggle-row');
      if (sourceRow) sourceRow.classList.add('dash-relocated');
      if (!switchLabel) throw new Error('Missing safe checkbox label for ' + name);
      byId(id).append(switchLabel);
    }
    const mode = single('is_live_mode');
    const modeLive = byId('dashLiveButton'), modeTest = byId('dashTestButton');
    const channels = single('monitored_channel_ids');
    const setModeButtons = () => {
      modeLive.setAttribute('aria-pressed', String(mode.checked));
      modeTest.setAttribute('aria-pressed', String(!mode.checked));
    };
    modeLive.addEventListener('click', () => {mode.checked = true; mode.dispatchEvent(new Event('change',{bubbles:true}));});
    modeTest.addEventListener('click', () => {mode.checked = false; mode.dispatchEvent(new Event('change',{bubbles:true}));});
    mode.addEventListener('change',setModeButtons); setModeButtons();
    const updateChannels = () => {
      const count = channels.value.split(',').map(x=>x.trim()).filter(Boolean).length;
      byId('dashChannels').textContent = count ? count + ' configured channel' + (count===1?'':'s') : 'None configured';
    };
    channels.addEventListener('input',updateChannels); updateChannels();

    // Advanced: move the same original sections, without omitting password fields.
    const oldSections = Array.from(form.querySelectorAll(':scope > .section-card'));
    const authLabel = form.querySelector(':scope > label.toggle-row');
    if (authLabel) {
      authLabel.classList.add('dash-live-confirm');
      advancedBody.append(authLabel);
    }
    const systemStatus = byId('dashOriginalSystemStatus');
    if (systemStatus) {systemStatus.classList.add('dash-status-advanced');advancedBody.append(systemStatus);}
    oldSections.forEach(section => advancedBody.append(section));
    overview.hidden = false;
    advanced.hidden = false;
    document.documentElement.classList.add('dash-ready');
    // Form validation must reveal the field, never focus a closed details pane.
    form.addEventListener('invalid', e => {
      if (advanced.contains(e.target)) advanced.open = true;
    }, true);
    byId('dashReset').disabled = false;

    // Statuses always start UNKNOWN until the authenticated read-only service proves them.
    const sidebarStatus = document.createElement('div');
    sidebarStatus.className = 'dash-sidebar-status';
    sidebarStatus.innerHTML = '<p class="dash-sidebar-title"><span id="dashSideDot" class="dash-dot"></span><span id="dashSideTitle">Checking service</span></p><p class="dash-sidebar-detail">Mode: <b id="dashSideMode">UNVERIFIED</b></p><p class="dash-sidebar-detail">Runtime: <b id="dashSideRuntime">Awaiting verification</b></p><p class="dash-sidebar-detail">Protections: <b>Verify in Control Center</b></p>';
    nav.append(sidebarStatus);
    const clock = byId('dashClock');
    const updateTime = () => {clock.textContent = new Intl.DateTimeFormat('en-IN',{timeZone:'Asia/Kolkata',hour:'2-digit',minute:'2-digit',hour12:false}).format(new Date())+' IST';};
    updateTime();
    // Time is decorative; no trading state is inferred from the clock.
    const timeHandle = setInterval(updateTime, 60000);
    window.addEventListener('pagehide', () => clearInterval(timeHandle), {once:true});
    const safeSet = (id,value) => {byId(id).textContent = String(value);};
    const amoEquity=byId('dashEquityAmo'),amoFno=byId('dashFnoAmo');
    const saveStopped=byId('dashSaveStopped');
    let current=null;
    const setUnavailable = reason => {
      current=null;amoEquity.disabled=true;amoFno.disabled=true;saveStopped.disabled=true;
      safeSet('dashAmoHelp',reason || 'Cannot verify AMO settings; edit disabled.');
      safeSet('dashModeStatus','Runtime unverified');
      safeSet('dashSideTitle','Service unverified');
      safeSet('dashSideMode','UNKNOWN');
      safeSet('dashSideRuntime','Not verified');
      byId('dashSideDot').classList.remove('verified');
    };
    async function statusJSON(){
      const r=await fetch('/control/status',{credentials:'same-origin',cache:'no-store'});
      if(!r.ok)throw new Error('HTTP '+r.status);
      const data=await r.json();
      if(!data||typeof data!=='object'||!data.service_state)throw new Error('Status response unavailable');
      return data;
    }
    async function refreshStatus(){
      try {
        const s=await statusJSON();current=s;
        const verified = s.runtime_verified===true && s.service_state==='RUNNING';
        const actualMode=verified?(s.running_mode==='LIVE'?'LIVE':s.running_mode==='TEST'?'TEST':'UNKNOWN'):'UNVERIFIED';
        const pill=byId('dashModeStatus');
        pill.className='dash-header-pill' + (verified && actualMode==='LIVE'?' verified-live':verified && actualMode==='TEST'?' verified-test':'');
        pill.textContent=verified?actualMode:(s.saved_mode==='LIVE'?'Saved LIVE · unverified':'Runtime unverified');
        safeSet('dashSideTitle',verified?'Bot Online':s.service_state==='RUNNING'?'Running · unverified':s.service_state.replaceAll('_',' '));
        safeSet('dashSideMode',actualMode);
        safeSet('dashSideRuntime',verified?'Verified running mode':'Check Control Center');
        byId('dashSideDot').classList.toggle('verified',verified);
        amoEquity.checked = s.equity_amo_saved === true;
        amoFno.checked = s.fno_amo_saved === true;
        amoEquity.disabled=s.equity_amo_can_edit!==true;
        amoFno.disabled=s.fno_amo_can_edit!==true;
        const active = s.equity_amo_active===true || s.fno_amo_active===true;
        const statusNote = verified ? 'Runtime verified' : 'Saved settings only · runtime unverified';
        safeSet('dashAmoHelp',statusNote + ' · F&O AMO has no automatic SL/target.');
        const stopped=['STOPPED_BY_OWNER','STOPPED_OTHER'].includes(s.service_state);
        saveStopped.disabled= !(stopped && s.transition_gate_active===false && s.tracker_v2_apply_blocked!==true);
        if(s.warning)safeSet('dashAmoHelp',s.warning);
      } catch(err){setUnavailable('AMO status unavailable: '+err.message+'. Use Control Center.');}
    }
    function newRequestId(){
      return window.crypto&&window.crypto.randomUUID?window.crypto.randomUUID():'AMO-'+Date.now()+'-'+Math.random().toString(36).slice(2);
    }
    async function toggleAmo(which,requested){
      const control=which==='equity'?amoEquity:amoFno;
      const allowed=current && current[which==='equity'?'equity_amo_can_edit':'fno_amo_can_edit']===true;
      if(!allowed){await refreshStatus();return;}
      if(which==='fno'&&requested&&!confirm('Enable broker-native F&O AMO? Filled orders have NO automatic SL or target.')){await refreshStatus();return;}
      const live=current.saved_mode==='LIVE';
      if(live&&!confirm('Changing AMO may restart the LIVE bot and interrupt protection supervision. Proceed only after checking open orders. Continue?')){await refreshStatus();return;}
      control.disabled=true;
      safeSet('dashAmoHelp','Saving '+(which==='equity'?'Equity':'F&O')+' AMO; verifying outcome…');
      try {
        const request={enabled:requested,confirm_live:live,csrf_token:single('csrf_token').value,expected_revision:current.config_file_revision,request_id:newRequestId()};
        const r=await fetch('/control/amo/'+which,{method:'POST',headers:{'Content-Type':'application/json'},credentials:'same-origin',body:JSON.stringify(request)});
        const payload=await r.json();
        if(!r.ok||payload.ok===false)throw new Error(payload.message||payload.error||'AMO save rejected');
        // Never indicate success until the read-only endpoint rechecks the status.
        await refreshStatus();
        safeSet('dashAmoHelp',(payload.message||'AMO request completed.')+' · Verify runtime state above.');
      } catch(err){await refreshStatus();safeSet('dashAmoHelp','AMO not verified: '+err.message);}
    }
    amoEquity.addEventListener('change',()=>toggleAmo('equity',amoEquity.checked));
    amoFno.addEventListener('change',()=>toggleAmo('fno',amoFno.checked));
    refreshStatus();

    // Reset means discard edits, NOT overwrite persisted defaults.
    const reset=byId('dashReset');
    reset.addEventListener('click',()=>{
      if(!confirm('Discard unsaved Setup changes? No broker orders or service settings will be changed.'))return;
      form.reset();setModeButtons();updateChannels();refreshStatus();
      const confirmInput=form.querySelector('input[name="confirm_live"]');
      if(confirmInput)confirmInput.checked=false;
    });
    saveStopped.addEventListener('click',async()=>{
      // Read state again immediately before using the existing Save & Apply API.
      saveStopped.disabled=true;
      try{
        const s=await statusJSON();
        if(!['STOPPED_BY_OWNER','STOPPED_OTHER'].includes(s.service_state)||s.transition_gate_active!==false||s.tracker_v2_apply_blocked===true){
          safeSet('dashAmoHelp','Service no longer verified stopped; Save only blocked.');
          return;
        }
        if(!confirm('Save settings while the trading bot is stopped? The server will independently check service state.'))return;
        form.requestSubmit(form.querySelector('button.btn-submit[type="submit"]'));
      }catch(err){safeSet('dashAmoHelp','Save-only verification failed: '+err.message);}
      finally{await refreshStatus();}
    });
    // After successful Save & Apply the original revision is updated by existing code;
    // reload verified status rather than assuming the bot restarted.
    form.addEventListener('setup:saved', () => {
      for (const el of form.querySelectorAll('input,select,textarea')) {
        if (el.matches('[type=checkbox],[type=radio]')) el.defaultChecked = el.checked;
        else if (el.tagName === 'SELECT') for (const opt of el.options) opt.defaultSelected = opt.selected;
        else if (el.type !== 'password' && el.type !== 'hidden') el.defaultValue = el.value;
      }
      refreshStatus();
    });
    if(location.hash==='#dashAdvanced')advanced.open=true;
  });
}());
