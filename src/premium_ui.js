/* V17 preview-inspired presentation only. Native forms/routes remain authoritative. */
(function () {
  'use strict';
  const root=document.documentElement;
  const top=document.querySelector('.v17p-topbar');
  const bottom=document.querySelector('.v17p-bottom-tabs');
  // Setup historically embeds shared navigation within its native .container.
  // Relocate only presentation nodes, never a native form or field.
  if(top && top.parentElement!==document.body) document.body.insertBefore(top,document.body.firstChild);
  if(bottom && bottom.parentElement!==document.body) document.body.appendChild(bottom);
  root.classList.add('v17p-preview-shell');

  const opener=document.getElementById('premNavOpen');
  const account=document.getElementById('v17pAccountOpen');
  const closer=document.getElementById('premNavClose');
  const panel=document.getElementById('premNav');
  const backdrop=document.getElementById('premNavBackdrop');
  let menuOpen=false;
  function syncMenu() {
    if(!panel||!backdrop)return;
    panel.classList.toggle('open',menuOpen);
    panel.style.setProperty('visibility',menuOpen?'visible':'hidden','important');
    panel.style.setProperty('transform',menuOpen?'translateX(0)':'translateX(-110%)','important');
    panel.setAttribute('aria-hidden',String(!menuOpen));
    panel.inert=!menuOpen;
    if(opener)opener.setAttribute('aria-expanded',String(menuOpen));
    if(account)account.setAttribute('aria-expanded',String(menuOpen));
    backdrop.hidden=!menuOpen;
    document.body.classList.toggle('prem-drawer-open',menuOpen);
  }
  function toggleMenu(open) {
    menuOpen=Boolean(open);syncMenu();
    const target=menuOpen?closer:(account||opener);
    if(target)target.focus();
  }
  if(opener)opener.addEventListener('click',()=>toggleMenu(true));
  if(account)account.addEventListener('click',()=>toggleMenu(!menuOpen));
  if(closer)closer.addEventListener('click',()=>toggleMenu(false));
  if(backdrop)backdrop.addEventListener('click',()=>toggleMenu(false));
  if(panel)panel.addEventListener('click',e=>{if(e.target.closest('a'))toggleMenu(false)});
  document.addEventListener('keydown',e=>{if(e.key==='Escape'&&menuOpen)toggleMenu(false)});
  // Move the existing CSRF-protected Logout form into the account drawer.
  const logout=document.querySelector('form[action="/logout"]');
  if(panel&&logout){logout.classList.add('v17p-logout');panel.appendChild(logout);}
  syncMenu();

  function syncTabs(){
    const path=location.pathname;
    let active='dashboard';
    if(root.classList.contains('premium-designer')||path.includes('/telegram-designer'))active='telegram';
    else if(root.classList.contains('premium-setup')||path.startsWith('/setup'))active='setup';
    else {
      const hash=location.hash.slice(1).toLowerCase();
      active=['dashboard','control','amo','health'].includes(hash)?hash:'dashboard';
    }
    document.querySelectorAll('[data-v17p-dest]').forEach(a=>{
      if(a.getAttribute('data-v17p-dest')===active)a.setAttribute('aria-current','page');
      else a.removeAttribute('aria-current');
    });
  }
  window.addEventListener('hashchange',syncTabs);syncTabs();

  // Purely visual preference; user can switch dark/light at any time.
  const key='auditbot-v17-appearance';
  let light=true;
  try {light=window.localStorage.getItem(key)!=='dark';}catch(_){/* private origin */}
  function applyTheme(){
    root.classList.toggle('v17-light',light);
    root.classList.toggle('v17p-dark',!light);
    const control=document.getElementById('premiumThemeToggle');
    if(control){control.textContent=light?'☼':'☾';control.setAttribute('aria-pressed',String(!light));}
  }
  const themeBtn=document.getElementById('premiumThemeToggle');
  if(themeBtn)themeBtn.addEventListener('click',()=>{
    light=!light;
    try{window.localStorage.setItem(key,light?'light':'dark');}catch(_){/* still works */}
    applyTheme();
  });
  applyTheme();

  // Header status is never taken from a saved LIVE preference or demo data.
  const chip=document.getElementById('v17p-runtime-chip');
  if(chip && !root.classList.contains('premium-control')){
    const fail=()=>{chip.textContent='UNVERIFIED';chip.className='v17p-runtime-chip';};
    fail();
    (async()=>{
      try{
        const r=await fetch('/control/status',{credentials:'same-origin',cache:'no-store'});
        if(!r.ok||r.redirected)throw Error('Status unavailable');
        const data=await r.json();
        if(!data||typeof data!=='object'||Array.isArray(data))throw Error('Invalid status');
        const running=data.service_state==='RUNNING'&&data.runtime_verified===true;
        if(running && (data.running_mode==='LIVE'||data.running_mode==='TEST')){
          chip.textContent=data.running_mode;
          chip.classList.add(data.running_mode==='LIVE'?'v17p-chip-live':'v17p-chip-test');
        }else if(['STOPPED_BY_OWNER','STOPPED_OTHER','FAILED'].includes(data.service_state)){
          chip.textContent='STOPPED';chip.classList.add('v17p-chip-stopped');
        }else fail();
      }catch(_){fail();}
    })();
  }

  // The four landing cards are navigation to existing native Setup sections.
  // No input, switch, form submission or handler is cloned or replaced.
  if(root.classList.contains('premium-setup')){
    const nativeForm=document.querySelector('form[action="/setup"]');
    const header=document.querySelector('.premium-setup .container>.header');
    if(nativeForm&&header){
      const landing=document.createElement('section');
      landing.className='v17p-setup-landing';
      landing.setAttribute('aria-label','Configuration categories');
      landing.innerHTML=`
        <div class="v17p-setup-grid">
          <button type="button" class="v17p-setup-card" data-target="setup-execution"><span class="v17p-category-icon lilac" aria-hidden="true">⚙</span><span><b>Trading Parameters</b><small>Order size, lots, SL/Target, limits</small></span><span aria-hidden="true">›</span></button>
          <button type="button" class="v17p-setup-card" data-target="setup-telegram"><span class="v17p-category-icon violet" aria-hidden="true">♧</span><span><b>Signal &amp; Parsing</b><small>Telegram sources, channels, signal settings</small></span><span aria-hidden="true">›</span></button>
          <a class="v17p-setup-card" href="/#amo"><span class="v17p-category-icon amber" aria-hidden="true">▣</span><span><b>AMO &amp; Orders</b><small>Equity/F&amp;O AMO, order controls</small></span><span aria-hidden="true">›</span></a>
          <button type="button" class="v17p-setup-card" data-target="setup-risk"><span class="v17p-category-icon sea" aria-hidden="true">⬡</span><span><b>System &amp; Risk</b><small>Governor, protection, safety controls</small></span><span aria-hidden="true">›</span></button>
        </div><button class="v17p-view-all" id="v17pViewAllSettings" type="button">View all settings and Save &amp; Apply</button>
      `;
      header.insertAdjacentElement('afterend',landing);
      root.classList.add('v17p-setup-collapsed');
      function openNative(id){
        root.classList.remove('v17p-setup-collapsed');
        landing.hidden=true;
        const el=document.getElementById(id)||nativeForm;
        el.scrollIntoView({block:'start',behavior:'auto'});
      }
      landing.querySelectorAll('button[data-target]').forEach(b=>b.addEventListener('click',()=>openNative(b.dataset.target)));
      landing.querySelector('#v17pViewAllSettings').addEventListener('click',()=>openNative('setup-execution'));
      // Follow existing native anchored section links even after collapse.
      if(location.hash.startsWith('#setup-'))openNative(location.hash.slice(1));
    }
  }
  if(root.classList.contains('premium-designer')){
    const title=document.querySelector('main .top h1');
    if(title) title.textContent='Telegram Designer';
    const back=document.querySelector('main .top a[href="/"]');
    if(back){back.href='/setup';back.textContent='← Setup';}
  }
}());
