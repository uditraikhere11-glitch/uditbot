/* Unified shell for non-Setup WebUI pages. Read only; never calls broker APIs,
 * mutates trading services, or asserts watcher/reconciliation health.
 */
(function () {
  'use strict';
  const get = id => document.getElementById(id);
  const modePill = get('replicaModePill');
  if (!modePill) return;
  const clock = get('replicaClock');
  let requestSerial = 0;
  function clockTick() {
    try {
      clock.textContent = new Intl.DateTimeFormat('en-GB', {
        timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit',
        second: '2-digit', hourCycle: 'h23'
      }).format(new Date()) + ' IST';
    } catch (_) { clock.textContent = 'IST'; }
  }
  function setText(id, value) {
    const node = get(id);
    if (node) node.textContent = value;
  }
  function setStatus(state) {
    const running = state.service_state === 'RUNNING' && state.runtime_verified === true;
    const stopped = ['STOPPED_BY_OWNER','STOPPED_OTHER'].includes(state.service_state);
    const mode = running ? state.running_mode : state.saved_mode;
    const validMode = mode === 'LIVE' || mode === 'TEST' ? mode : 'UNVERIFIED';
    modePill.textContent = (running ? '● ' : '◌ ') + validMode;
    modePill.dataset.tone = running && validMode === 'LIVE' ? 'success' : 'neutral';
    setText('replicaServiceText', running ? 'Bot Online' : stopped ? 'Bot Stopped' : 'Status Unverified');
    setText('replicaNavMode', validMode);
    setText('replicaNavState', running ? 'Running · ' + validMode : stopped ? 'Stopped by owner' : 'Not verified');
  }
  function unavailable() {
    modePill.textContent = '◌ UNVERIFIED';
    modePill.dataset.tone = 'neutral';
    setText('replicaServiceText','Status Unverified');
    setText('replicaNavMode','UNVERIFIED');
    setText('replicaNavState','Not verified');
  }
  async function refresh() {
    const serial = ++requestSerial;
    try {
      const response = await fetch('/control/status', {credentials:'same-origin', cache:'no-store'});
      if (!response.ok) throw new Error('HTTP ' + response.status);
      const state = await response.json();
      if (serial !== requestSerial) return;
      setStatus(state);
    } catch (_) { if (serial === requestSerial) unavailable(); }
  }
  clockTick(); window.setInterval(clockTick,1000);
  refresh(); window.setInterval(refresh,20000);
})();
