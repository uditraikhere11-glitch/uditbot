"""Standalone offline checks for preview navigation, trust boundaries and restart safety."""
from __future__ import annotations
import os
from pathlib import Path
from playwright.sync_api import sync_playwright
from src.control_center_ui import render_control_center

JS=r'''
window.__posts=[];
window.__health={summary:'PASS 2 WARN 1',checks:[
 {name:'Trading service',status:'PASS',detail:'systemd RUNNING'},
 {name:'Owner service intent',status:'WARN',detail:'Running service conflicts with saved Owner STOP/observed state'},
 {name:'Dhan broker authentication',status:'UNKNOWN',detail:'No authenticated profile call'},
 {name:'Order protection watcher',status:'UNKNOWN',detail:'Not verified'}]};
window.__status={service_state:'RUNNING',saved_mode:'LIVE',running_mode:'LIVE',runtime_verified:true,
 transition_gate_active:false,tracker_v2_apply_blocked:false,equity_amo_saved:false,fno_amo_saved:false,
 equity_amo_can_edit:false,fno_amo_can_edit:false,service_detail:'Test status only'};
window.__badStatus=false;
window.fetch=async (url, opts={})=>{
  const path=String(url), method=String(opts.method||'GET').toUpperCase();
  if(method==='POST'){
    if(path!=='/setup/service-control')throw Error('Unexpected mutation request '+path);
    const payload=JSON.parse(opts.body);
    window.__posts.push(payload);
    if(payload.csrf_token!=='fake-testing-csrf')throw Error('Incorrect CSRF');
    return {ok:true,status:200,redirected:false,json:async()=>({ok:true,message:'Synthetic action accepted; not a real trading service'})};
  }
  if(path==='/control/status'){
    if(window.__badStatus)return {ok:true,status:200,redirected:false,json:async()=>{throw Error('Invalid response')}};
    return {ok:true,status:200,redirected:false,json:async()=>window.__status};
  }
  if(path==='/control/health')return {ok:true,status:200,redirected:false,json:async()=>window.__health};
  if(path==='/control/amo/orders')return {ok:true,status:200,redirected:false,json:async()=>({summary:'Synthetic',orders:[],fno_local_queue:[]})};
  throw Error('Unexpected offline request '+path);
};
'''

def run():
  with sync_playwright() as play:
    browser=play.chromium.launch(executable_path=os.getenv('V17_CHROMIUM','/usr/bin/chromium'),headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
    count=0
    try:
      for width in (320,390,768,1440):
        ctx=browser.new_context(viewport={'width':width,'height':844 if width<1000 else 900},is_mobile=width<600,has_touch=width<600)
        page=ctx.new_page()
        problems=[]
        page.on('pageerror',lambda exc:problems.append(str(exc)))
        page.on('dialog',lambda d:d.accept())
        page.goto('about:blank')
        page.evaluate("(()=>{"+JS+";return true;})()")
        page.set_content(render_control_center('fake-testing-csrf'),wait_until='load')
        page.wait_for_function("document.getElementById('service').textContent==='RUNNING'")
        page.wait_for_function("document.getElementById('v17p-owner-warning').hidden===false")
        assert page.locator('#v17p-runtime-chip').inner_text()=='LIVE'
        assert page.locator('#restart').is_disabled(),'Restart must remain disabled on Owner intent WARN'
        assert page.locator('#v17p-protection').inner_text()=='UNVERIFIED'
        assert page.locator('#v17p-gate').inner_text()=='UNVERIFIED'
        assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1'),width
        count+=1
        # Drawer must work at phone and desktop sizes, with accessible close.
        page.locator('#v17pAccountOpen').click()
        assert page.locator('#premNav').get_attribute('aria-hidden')=='false'
        page.wait_for_timeout(150)
        assert page.locator('#premNavClose').is_visible()
        page.locator('#premNavClose').click()
        assert page.locator('#premNav').get_attribute('aria-hidden')=='true'
        count+=1
        # Health in verified owner-safe state permits guarded restart; original backend route only.
        page.evaluate('''window.__health={summary:'PASS',checks:[
         {name:'Owner service intent',status:'PASS',detail:'Intent matches running state'},
         {name:'Order protection watcher',status:'UNKNOWN',detail:'Cannot verify broker protection'}]};
         reflectDashboardHealth(window.__health);''')
        assert page.locator('#restart').is_enabled(),'Owner intent PASS should permit restart UI if runtime verified'
        page.evaluate('window.location.hash="#control"')
        page.locator('#restart').click()
        page.wait_for_function('window.__posts.length===1')
        assert page.evaluate("window.__posts[0].action==='restart' && window.__posts[0].confirm_live===true")
        count+=1
        # A later failed status response must fail-closed and never leave LIVE badge displayed.
        page.evaluate('window.__badStatus=true;refreshStatus()')
        page.wait_for_function("document.getElementById('v17p-runtime-chip').textContent==='UNVERIFIED'")
        assert page.locator('#start').is_disabled() and page.locator('#stop').is_disabled() and page.locator('#restart').is_disabled()
        count+=1
        assert not problems,problems
        ctx.close()
      # Escaped arbitrary long operator display labels never overflow mobile width.
      os.environ['V17_TEST_BRAND']='x'
      old=os.getcwd()
      from tempfile import TemporaryDirectory
      with TemporaryDirectory() as d:
        os.chdir(d)
        Path('.env').write_text('OPERATOR_NAME='+('Q'*1000)+'\n',encoding='utf-8')
        for width in (320,390):
          ctx=browser.new_context(viewport={'width':width,'height':844},is_mobile=True)
          page=ctx.new_page();page.goto('about:blank');page.evaluate("(()=>{"+JS+";return true;})()")
          page.set_content(render_control_center('fake-testing-csrf'),wait_until='load')
          assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1'),('long name',width)
          assert page.locator('.v17p-brand').count()==1
          count+=1
          ctx.close()
        os.chdir(old)
    finally:browser.close()
  assert count==18,count
  print('PASS: 18/18 offline browser interactions across 320/390/768/1440; safe restart, owner warning, drawer, fail-closed chip, long labels')

if __name__=='__main__':run()
