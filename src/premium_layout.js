/* Six original cards; visual positioning only. No input, form submission, or state access. */
(function () {
  'use strict';
  const form = document.querySelector('html.premium-setup form[action="/setup"]');
  if (!form) return;
  const left = ['setup-execution', 'setup-risk', 'setup-protection'];
  const right = ['setup-telegram', 'setup-broker', 'setup-sizing'];
  const cards = [...left, ...right].map(id => document.getElementById(id));
  if (cards.some(node => !node || node.parentElement !== form)) return;
  const button = form.querySelector(':scope > .btn-submit');
  if (!button) return;
  const wide = window.matchMedia('(min-width:1280px)');
  let scheduled = false;
  let busy = false;
  function clean() {
    form.classList.remove('prem-balanced-columns');
    cards.forEach(c => { c.style.gridRow = ''; c.style.gridColumn = ''; });
    button.style.gridRow = ''; button.style.gridColumn = '';
  }
  function layout() {
    scheduled = false;
    if (busy) return;
    busy = true;
    if (!wide.matches) { clean(); busy = false; return; }
    // Measure intrinsic card height at its actual half-column width. 
    clean();
    form.classList.add('prem-balanced-columns');
    form.style.setProperty('--prem-grid-step', '6px');
    const heights = new Map();
    for (const card of cards) {
      card.style.gridRow = 'auto';
      card.style.gridColumn = 'auto';
      heights.set(card.id, Math.ceil(card.scrollHeight + 2));
    }
    const step = 6, space = 18;
    let leftRow = 3, rightRow = 3;
    for (const id of left) {
      const card = document.getElementById(id);
      const span = Math.ceil((heights.get(id) + space) / step);
      card.style.gridColumn = '1'; card.style.gridRow = `${leftRow} / span ${span}`;
      leftRow += span;
    }
    for (const id of right) {
      const card = document.getElementById(id);
      const span = Math.ceil((heights.get(id) + space) / step);
      card.style.gridColumn = '2'; card.style.gridRow = `${rightRow} / span ${span}`;
      rightRow += span;
    }
    button.style.gridRow = String(Math.max(leftRow,rightRow)+1);
    button.style.gridColumn = '1 / -1';
    busy = false;
  }
  function queue() {
    if (scheduled) return;
    scheduled = true;
    window.requestAnimationFrame(layout);
  }
  window.addEventListener('resize',queue,{passive:true});
  if (document.fonts && document.fonts.ready) document.fonts.ready.then(queue);
  window.addEventListener('load',queue,{once:true});
  queue();
})();
