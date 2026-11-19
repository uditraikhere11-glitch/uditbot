/* Responsive navigation: a visible desktop rail; an accessible mobile drawer. */
(function () {
  const opener = document.getElementById('premNavOpen');
  const closer = document.getElementById('premNavClose');
  const panel = document.getElementById('premNav');
  const backdrop = document.getElementById('premNavBackdrop');
  if (!opener || !closer || !panel || !backdrop) return;
  const desktop = window.matchMedia('(min-width: 1120px)');
  let mobileOpen = false;
  function sync() {
    const wide = desktop.matches;
    panel.classList.toggle('open', mobileOpen && !wide);
    panel.setAttribute('aria-hidden', String(!wide && !mobileOpen));
    panel.inert = !wide && !mobileOpen;
    opener.setAttribute('aria-expanded', String(wide || mobileOpen));
    backdrop.hidden = wide || !mobileOpen;
    document.body.classList.toggle('prem-drawer-open', !wide && mobileOpen);
  }
  function toggle(value) {
    mobileOpen = value;
    sync();
    (value ? closer : opener).focus();
  }
  opener.addEventListener('click', () => toggle(true));
  closer.addEventListener('click', () => toggle(false));
  backdrop.addEventListener('click', () => toggle(false));
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && mobileOpen && !desktop.matches) toggle(false);
  });
  window.addEventListener('resize', sync);
  sync();
}());