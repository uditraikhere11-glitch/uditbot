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
  let light=false; // Approved dark appearance is the first-visit default.
  try {light=window.localStorage.getItem(key)==='light';}catch(_){/* private origin */}
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

  // Six accurately named categories, mapped 1:1 to the existing six backend
  // sections and the *one* native Save & Apply form. AMO is managed on /#amo.
  // Never clone, mutate, or re-parent any native form input.
  if(root.classList.contains('premium-setup')){
    const nativeForm=document.querySelector('form[action="/setup"]');
    const header=document.querySelector('.premium-setup .container>.header');
    const ids=['setup-execution','setup-telegram','setup-broker','setup-sizing','setup-risk','setup-protection'];
    const sections=ids.map(id=>document.getElementById(id));
    const tabs=nativeForm?.querySelector('.setup-section-jumps');
    const liveConsent=nativeForm?.querySelector('input[name="confirm_live"]')?.closest('label');
    if(nativeForm&&header&&tabs&&sections.every(Boolean)){
      const landing=document.createElement('section');
      landing.className='v17p-setup-landing';
      landing.setAttribute('aria-label','Configuration categories');
      const cards=[
        ['setup-execution','⚙','lilac','Trading Mode & Segments','LIVE/TEST mode, cash and options'],
        ['setup-telegram','♧','violet','Signal & Telegram','Operator, channels and signal ingestion'],
        ['setup-broker','◈','sea','Dhan Credentials','Client ID, PIN, TOTP and token'],
        ['setup-sizing','▤','amber','Sizing & Pricing','Budgets, lots, price policy and quantity'],
        ['setup-risk','⬡','sea','Risk & Limits','Exposure, funds, daily and drawdown limits'],
        ['setup-protection','♡','lilac','Protection & Timing','SL, targets, watcher and timing rules']
      ];
      const grid=document.createElement('div');grid.className='v17p-setup-grid';
      for(const [id,glyph,tone,title,detail] of cards){
        const button=document.createElement('button');
        button.type='button';button.className='v17p-setup-card';
        button.dataset.target=id;
        const icon=document.createElement('span');icon.className='v17p-category-icon '+tone;
        icon.setAttribute('aria-hidden','true');icon.textContent=glyph;
        const content=document.createElement('span');
        const bold=document.createElement('b');bold.textContent=title;
        const small=document.createElement('small');small.textContent=detail;
        const arrow=document.createElement('span');arrow.textContent='›';arrow.setAttribute('aria-hidden','true');
        content.append(bold,small);button.append(icon,content,arrow);grid.appendChild(button);
      }
      const actions=document.createElement('div');actions.className='v17p-setup-actions';
      const all=document.createElement('button');all.type='button';all.id='v17pViewAllSettings';
      all.className='v17p-view-all';all.textContent='View all settings and Save & Apply';
      const amo=document.createElement('a');amo.href='/#amo';amo.className='v17p-setup-amo-link';
      amo.textContent='AMO & Orders → Open dedicated page';
      actions.append(all,amo);landing.append(grid,actions);
      header.insertAdjacentElement('afterend',landing);

      const toolbar=document.createElement('div');toolbar.className='v17p-setup-toolbar';
      toolbar.hidden=true;
      const back=document.createElement('button');back.type='button';back.className='v17p-back-setup';
      back.textContent='← All categories';
      const label=document.createElement('strong');label.id='v17p-setup-current-title';
      toolbar.append(back,label);landing.insertAdjacentElement('afterend',toolbar);
      const labels=Object.fromEntries(cards.map(([id,_glyph,_tone,title])=>[id,title]));

      function showSetup(view,{focus=false}={}){
        const selected=ids.includes(view)?view:(view==='setup-all'?'setup-all':'home');
        const home=selected==='home';
        root.classList.toggle('v17p-setup-collapsed',home);
        landing.hidden=!home;toolbar.hidden=home;
        label.textContent=selected==='setup-all'?'All Setup settings':(labels[selected]||'');
        if(liveConsent)liveConsent.hidden=!home && selected!=='setup-all' && selected!=='setup-execution';
        for(const section of sections){
          section.hidden=!home && selected!=='setup-all' && section.id!==selected;
          // Hide sections individually without disabling inputs: this is one
          // full native form, so unedited controls retain their saved values.
        }
        tabs.querySelectorAll('a[href^="#setup-"]').forEach(a=>{
          const active=selected===a.getAttribute('href').slice(1);
          if(active)a.setAttribute('aria-current','location');
          else a.removeAttribute('aria-current');
        });
        if(focus && !home){
          toolbar.scrollIntoView({block:'start',behavior:'auto'});
          // Only move focus to a native heading or toolbar, not to input values.
        }
      }
      function currentView(){
        const hash=window.location.hash.replace(/^#/,'');
        return ids.includes(hash)||hash==='setup-all'?hash:'home';
      }
      function navigate(view){
        const fragment=view==='home'?'':view;
        const next=fragment?'#'+fragment:'#setup-home';
        history.pushState(null,'',next);
        showSetup(view,{focus:view!=='home'});
        if(view==='home')window.scrollTo({top:0,behavior:'instant'});
      }
      landing.querySelectorAll('button[data-target]').forEach(b=>
        b.addEventListener('click',()=>navigate(b.dataset.target)));
      all.addEventListener('click',()=>navigate('setup-all'));
      back.addEventListener('click',()=>navigate('home'));
      tabs.addEventListener('click',event=>{
        const a=event.target.closest('a[href^="#setup-"]');
        if(!a||!tabs.contains(a))return;
        const id=a.getAttribute('href').slice(1);
        if(!ids.includes(id))return;
        event.preventDefault();navigate(id);
      });
      window.addEventListener('popstate',()=>showSetup(currentView()));
      window.addEventListener('hashchange',()=>showSetup(currentView()));
      showSetup(currentView());
    }
  }
  if(root.classList.contains('premium-designer')){
    const title=document.querySelector('main .top h1');
    if(title) title.textContent='Telegram Designer';
    const back=document.querySelector('main .top a[href="/"]');
    if(back){back.href='/setup';back.textContent='← Setup';}
  }
}());
