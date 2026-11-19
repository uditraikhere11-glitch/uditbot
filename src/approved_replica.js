/* Approved screenshot layout for the Setup page.
 * Presentation-only: saved settings are still submitted through the existing
 * /setup/save-apply endpoint. AMO controls use the existing authenticated API.
 * Never infer reconciliation or OCO health from a running service.
 */
(function () {
  'use strict';
  const form = document.querySelector('form[action="/setup"]');
  const board = document.getElementById('replicaBoard');
  if (!form || !board) return;
  const get = id => document.getElementById(id);
  const name = key => form.elements.namedItem(key);
  const dataFields = ['equity_budget_inr', 'fno_capital_per_trade', 'fno_max_lots',
    'fno_sl_pct', 'fno_target_pct', 'max_quote_age_sec',
    'equity_cmp_execution_policy', 'scrip_master_max_age_sec'];
  let initial = null, brokerState = null, refreshSerial = 0;
  const proxyFields = Array.from(board.querySelectorAll('[data-mirror]'));
  const buttonLive = get('replicaLive'), buttonTest = get('replicaTest');
  const originalMode = name('is_live_mode');
  const alertBox = get('replicaAlert');
  const alertText = get('replicaAlertText');
  const alertTitle = get('replicaAlertTitle');
  let alertTimer;
  function snapshot() {
    const saved = {};
    for (const item of form.elements) {
      if (!item.name || ['csrf_token', 'expected_revision', 'full_setup_form', 'confirm_live'].includes(item.name)) continue;
      if (item.type === 'checkbox') saved[item.name] = Boolean(item.checked);
      else if (item.type !== 'submit' && item.type !== 'button') saved[item.name] = String(item.value);
    }
    return saved;
  }
  function dirty() {
    const live = snapshot();
    return Object.keys(initial || {}).some(key => live[key] !== initial[key]);
  }
  function mark(value, tone) {
    const el = get('replicaPending');
    el.textContent = value;
    el.dataset.tone = tone || 'neutral';
  }
  function showAlert(title, message, tone, autoDismiss) {
    window.clearTimeout(alertTimer);
    alertBox.hidden = false;
    alertTitle.textContent = String(title);
    alertText.textContent = String(message);
    alertBox.dataset.tone = tone || 'neutral';
    if (autoDismiss) alertTimer = window.setTimeout(() => { alertBox.hidden = true; }, 5200);
  }
  function refreshMirrors() {
    proxyFields.forEach(input => {
      const original = name(input.dataset.mirror);
      if (original && input.value !== String(original.value)) input.value = String(original.value);
    });
    const live = Boolean(originalMode && originalMode.checked);
    buttonLive.setAttribute('aria-pressed', String(live));
    buttonTest.setAttribute('aria-pressed', String(!live));
    mark(dirty() ? 'Changes not saved — review before applying' : 'All displayed values come from your saved configuration', dirty() ? 'warn' : 'neutral');
  }
  proxyFields.forEach(input => {
    const original = name(input.dataset.mirror);
    if (!original) { input.disabled = true; return; }
    input.value = String(original.value);
    input.disabled = false;
    input.addEventListener('input', () => {
      original.value = input.value;
      original.dispatchEvent(new Event('input', {bubbles:true}));
      refreshMirrors();
    });
    input.addEventListener('change', () => original.dispatchEvent(new Event('change', {bubbles:true})));
    original.addEventListener('change', refreshMirrors);
  });
  function setMode(live) {
    if (!originalMode) return;
    originalMode.checked = Boolean(live);
    originalMode.dispatchEvent(new Event('change', {bubbles:true}));
    refreshMirrors();
  }
  buttonLive.addEventListener('click', () => setMode(true));
  buttonTest.addEventListener('click', () => setMode(false));
  Array.from(form.querySelectorAll('input,select')).forEach(el => {
    if (!el.dataset.mirror && el.type !== 'submit' && el.type !== 'button') {
      el.addEventListener('change', refreshMirrors);
    }
  });
  get('replicaReset').addEventListener('click', () => {
    if (!initial) return;
    for (const [key, value] of Object.entries(initial)) {
      const el = name(key);
      if (!el) continue;
      if (el.type === 'checkbox') el.checked = value;
      else el.value = value;
    }
    const liveAuthorization = name('confirm_live');
    if (liveAuthorization) liveAuthorization.checked = false;
    refreshMirrors();
    showAlert('Unsaved edits reset', 'Restored values from when this page was opened. Broker settings and trading state are unchanged.', 'neutral', true);
  });
  // The optional Save button is enabled only when the verified service is
  // STOPPED. It intentionally uses the same safe Save & Apply endpoint.
  const savedOnly = get('replicaSaveOnly');
  savedOnly.addEventListener('click', () => {
    if (!brokerState || !['STOPPED_BY_OWNER','STOPPED_OTHER'].includes(brokerState.service_state)) return;
    form.requestSubmit(get('replicaApply'));
  });
  // Use the existing form handler, including CSRF, revision, and LIVE confirmation.
  form.addEventListener('submit', () => {
    if (form.checkValidity()) {
      showAlert('Applying configuration', 'Waiting for the backend to verify the save and, when applicable, the trading service restart.', 'neutral', false);
    }
  });
  // Existing Save & Apply code updates the form revision after success. Observe
  // only the rendered status toast; never assume success from HTTP alone.
  const saveStatus = get('simpleSaveStatus');
  if (saveStatus && window.MutationObserver) {
    const observer = new MutationObserver(() => {
      if (saveStatus.hidden) return;
      const text = get('toastMessage') && get('toastMessage').textContent;
      if (!text) return;
      if (saveStatus.classList.contains('success')) {
        showAlert('Settings updated', text, 'success', true);
        initial = snapshot();
        mark('All displayed values come from your saved configuration', 'neutral');
        refreshStatus();
      } else if (saveStatus.classList.contains('error')) {
        showAlert('Settings not verified', text, 'danger', false);
      } else if (text.includes('Settings saved') || text.includes('already')) {
        showAlert('Configuration recorded', text, 'neutral', true);
        initial = snapshot();
        refreshStatus();
      }
    });
    observer.observe(saveStatus, {attributes:true,attributeFilter:['hidden','class'],subtree:true,characterData:true,childList:true});
  }
  get('replicaAlertClose').addEventListener('click', () => { alertBox.hidden = true; window.clearTimeout(alertTimer); });
  const advanced = get('replicaAdvanced');
  advanced.open = true;  // Display the unchanged working Setup controls by default.
  get('replicaSources').addEventListener('click', event => {
    event.preventDefault();
    advanced.open = true;
    get('setup-telegram').scrollIntoView({behavior:'smooth', block:'start'});
  });
  form.addEventListener('invalid', () => { advanced.open = true; }, true);
  initial = snapshot();
  refreshMirrors();
  // The top status bar reports only what the already-authenticated Control
  // Center can prove; neither reconciliation nor OCO is guessed from RUNNING.
  async function refreshStatus() {
    const serial = ++refreshSerial;
    try {
      const response = await fetch('/control/status', {credentials:'same-origin',cache:'no-store'});
      if (!response.ok) throw new Error('HTTP ' + response.status);
      const state = await response.json();
      if (serial !== refreshSerial) return;
      brokerState = state;
      const running = state.service_state === 'RUNNING' && state.runtime_verified === true;
      const stopped = ['STOPPED_BY_OWNER','STOPPED_OTHER'].includes(state.service_state);
      const status = get('replicaModePill');
      const mode = running ? state.running_mode : state.saved_mode;
      status.textContent = (running ? '● ' : '◌ ') + (mode === 'LIVE' ? 'LIVE' : mode === 'TEST' ? 'TEST' : 'UNVERIFIED');
      status.dataset.tone = running && mode === 'LIVE' ? 'success' : 'neutral';
      const service = get('replicaServiceState');
      service.dataset.on = String(running);
      service.setAttribute('aria-label', running ? 'Open Control Center; trading service is running' : 'Open Control Center; trading service stopped or unverified');
      get('replicaServiceText').textContent = running ? 'Bot Online' : (stopped ? 'Bot Stopped' : 'Status Unverified');
      get('replicaNavState').textContent = running ? 'Running · ' + mode : stopped ? 'Stopped by owner' : 'Not verified';
      const editable = state.tracker_v2_apply_blocked !== true && state.transition_gate_active === false;
      savedOnly.disabled = !(stopped && editable);
      savedOnly.title = savedOnly.disabled ? 'Save without restart is available only when service state is verified stopped and safety gates permit changes' : 'Save while bot is stopped; no trading restart';
      const token = get('replicaAMOEquity'), fno = get('replicaAMOFno');
      token.dataset.on = String(state.equity_amo_saved === true);
      fno.dataset.on = String(state.fno_amo_saved === true);
      token.disabled = state.equity_amo_can_edit !== true;
      fno.disabled = state.fno_amo_can_edit !== true;
      token.setAttribute('aria-checked', String(state.equity_amo_saved === true));
      fno.setAttribute('aria-checked', String(state.fno_amo_saved === true));
      get('replicaAMOFeedback').textContent = (state.runtime_verified ? 'Saved AMO settings · runtime verified' : 'AMO saved settings · runtime unverified');
      get('replicaNavMode').textContent = mode === 'LIVE' ? 'LIVE' : mode === 'TEST' ? 'TEST' : 'UNVERIFIED';
    } catch (err) {
      brokerState = null;
      savedOnly.disabled = true;
      get('replicaModePill').textContent = '◌ UNVERIFIED';
      get('replicaModePill').dataset.tone = 'neutral';
      get('replicaServiceState').dataset.on = 'false';
      get('replicaServiceText').textContent = 'Status Unverified';
      get('replicaNavState').textContent = 'Not verified';
      for(const id of ['replicaAMOEquity','replicaAMOFno']) get(id).disabled = true;
      get('replicaAMOFeedback').textContent = 'Status unavailable; AMO controls disabled';
    }
  }
  async function updateAMO(segment) {
    if (!brokerState || dirty()) {
      showAlert('Save current edits first', 'AMO settings use a separate verified operation. Save or reset unsaved edits before switching AMO.', 'danger', false);
      return;
    }
    const isFno = segment === 'fno';
    const control = get(isFno ? 'replicaAMOFno' : 'replicaAMOEquity');
    if (control.disabled) return;
    const current = control.dataset.on === 'true';
    const enabled = !current;
    if (isFno && enabled && !confirm('Enable F&O AMO? The broker can acknowledge an after-hours entry without a fill. Filled F&O AMO entries do NOT receive an automatic SL/target. Continue?')) return;
    const live = brokerState.saved_mode === 'LIVE';
    if (live && !confirm('Restart LIVE UditBot to apply this AMO setting? Existing protections must pass the safe restart gate.')) return;
    control.disabled = true;
    try {
      const csrf = name('csrf_token').value;
      const requestId = window.crypto && window.crypto.randomUUID ? window.crypto.randomUUID() : 'UI-' + Date.now() + '-' + Math.random().toString(36).slice(2);
      const response = await fetch(isFno ? '/control/amo/fno' : '/control/amo/equity',{
        method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({enabled,confirm_live:live,csrf_token:csrf,expected_revision:brokerState.config_file_revision,request_id:requestId})
      });
      const result = await response.json();
      if (!response.ok || result.ok === false) throw new Error(result.message || result.error || 'Change not verified');
      showAlert('AMO change recorded',result.message || 'Check the Control Center for runtime verification','neutral',true);
      await refreshStatus();
      if (brokerState && brokerState.config_file_revision) {
        name('expected_revision').value = brokerState.config_file_revision;
      }
    } catch (error) {
      showAlert('AMO change not verified',String(error.message||error),'danger',false);
      await refreshStatus();
    }
  }
  get('replicaAMOEquity').addEventListener('click', () => updateAMO('equity'));
  get('replicaAMOFno').addEventListener('click', () => updateAMO('fno'));
  function updateClock() {
    try {
      get('replicaClock').textContent = new Intl.DateTimeFormat('en-GB', {
        timeZone:'Asia/Kolkata',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23'
      }).format(new Date()) + ' IST';
    } catch (_) { get('replicaClock').textContent = 'IST'; }
  }
  updateClock();window.setInterval(updateClock,1000);
  refreshStatus();
  window.setInterval(refreshStatus,20000);
  document.documentElement.classList.add('approved-replica-ready');
})();
