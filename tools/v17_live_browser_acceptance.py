"""Offline iPhone/desktop V17 runtime UI integration against mocked read-only endpoints.

Never connects to a real WebUI, broker or systemd. Tests dynamic status, guarded
mode/restart, Reports & Logs, and false/expired statuses on both viewports.
"""
import os
from playwright.sync_api import sync_playwright
from src.control_center_ui import render_control_center

MOCK = r'''
window.__calls=[];
window.__ownerWarn=false;
window.__statusFail=false;
window.__posts=[];
window.fetch=async function(url,opts={}) {
 const path=String(url);
 window.__calls.push(path);
 const good=(data)=>({ok:true,status:200,json:async()=>data});
 if(path==='/control/status') {
   if(window.__statusFail)throw Error('Simulated status loss');
   return good({service_state:'RUNNING',service_detail:'Verified systemd process',saved_mode:'LIVE',
     running_mode:'LIVE',runtime_verified:true,mode_can_edit:!window.__ownerWarn,
     owner_intent_status:window.__ownerWarn?'WARN':'PASS',
     reconciliation:'HEALTHY',protection_watcher:'ACTIVE',
     new_entries_gate:window.__ownerWarn?'REVIEW_REQUIRED':'OPEN',
     transition_gate_active:false,tracker_v2_apply_blocked:false,config_file_revision:'a'.repeat(64),
     equity_amo_saved:true,fno_amo_saved:false,equity_amo_can_edit:false,fno_amo_can_edit:false});
 }
 if(path==='/control/broker-status')return good({status:'VERIFIED',token_expiration_ist:'22 Nov 20:00 IST'});
 if(path==='/control/health')return good({summary:'PASS: 10; WARN: '+(window.__ownerWarn?1:0),checks:[
   {name:'Owner service intent',status:window.__ownerWarn?'WARN':'PASS',detail:'Local owner intent'},
   {name:'Dhan broker authentication',status:'PASS',detail:'Verified broker profile'},
   {name:'Dhan token file',status:'PASS',detail:'Private token'},
   {name:'Reconciliation',status:'PASS',detail:'Latest producer snapshot'},
   {name:'Order protection watcher',status:'PASS',detail:'Watcher loop active'},
   {name:'New entries gate',status:window.__ownerWarn?'WARN':'PASS',detail:'Producer gate'}]});
 if(path==='/control/reports')return good({available:true,
   summary:{orders:10,signals:4,orders_today:2,signals_today:1,trades_today:1,unresolved_intents:0,unprotected_records:0},
   recent_orders:[{symbol:'SBIN',status:'TRADED',mode:'LIVE',at:1795320000}],
   events:[{event:'DAILY_HEALTH_CHECK',at:1795320000}]});
 if(path==='/control/amo/orders')return good({orders:[],summary:'No AMO records'});
 if(path==='/control/mode'||path==='/setup/service-control') {
   const body=JSON.parse(opts.body||'{}');window.__posts.push({path,body});
   if(path==='/control/mode')return good({ok:true,status:'SAVED_FOR_NEXT_START',message:'Saved for next start',restart_performed:false});
   return good({ok:true,service_state:'RUNNING',runtime_verified:true,running_mode:'LIVE',message:'Restarted verified'});
 }
 throw Error('Unexpected request '+path);
};
'''


def run():
    browser_path=os.environ.get('V17_CHROMIUM','/usr/bin/chromium')
    if not os.path.isfile(browser_path):
        raise SystemExit('Chromium missing')
    with sync_playwright() as p:
        browser=p.chromium.launch(headless=True,executable_path=browser_path,
                                  args=['--no-sandbox','--disable-dev-shm-usage'])
        for vw,vh in [(390,844),(1440,900)]:
            ctx=browser.new_context(viewport={'width':vw,'height':vh},is_mobile=vw<500)
            page=ctx.new_page()
            errors=[];page.on('pageerror',lambda e: errors.append(str(e)))
            page.on('dialog',lambda dialog:dialog.accept())
            page.evaluate('(()=>{'+MOCK+';return true})()')
            page.set_content(render_control_center('mock-csrf'),wait_until='load')
            page.wait_for_function("document.querySelector('#v17p-reconciliation').textContent==='HEALTHY'")
            page.wait_for_function("document.querySelector('#v17p-dhan').textContent.includes('Verified')")
            assert page.locator('#v17p-token').inner_text().startswith('Token expiry:'), 'Expired UI overwritten'
            assert page.locator('#v17p-gate').inner_text()=='OPEN'
            assert not page.evaluate('document.documentElement.scrollWidth>innerWidth+1')
            page.evaluate("location.hash='#control'")
            page.wait_for_function("document.querySelector('#restart').disabled === false")
            assert page.locator('#v17p-mode-test').is_enabled()
            assert page.locator('#v17p-mode-live').is_disabled()
            assert page.locator('#health').count()==0
            page.locator('#v17p-mode-test').click()
            page.wait_for_function("__posts.some(x=>x.path==='/control/mode')")
            assert page.evaluate("__posts.filter(x=>x.path==='/control/mode')[0].body.mode")=='TEST'
            assert page.evaluate("__posts.filter(x=>x.path==='/control/mode')[0].body.confirm_live") is False
            page.locator('#restart').click()
            page.wait_for_function("__posts.some(x=>x.path==='/setup/service-control')")
            assert page.evaluate("__posts.filter(x=>x.path==='/setup/service-control')[0].body.action")=='restart'
            page.evaluate("location.hash='#reports'")
            page.wait_for_function("document.querySelector('#v17-report-orders').textContent.includes('SBIN')")
            assert page.locator('#v17-report-counts').inner_text().find('Orders today')>=0
            assert page.locator('#v17-report-events').inner_text().find('DAILY_HEALTH_CHECK')>=0
            page.evaluate("window.__ownerWarn=true;location.hash='#dashboard';refreshStatus()")
            page.wait_for_function("document.querySelector('#v17p-gate').textContent==='REVIEW_REQUIRED'")
            page.evaluate("location.hash='#control'")
            page.wait_for_function("document.querySelector('#restart').disabled === true")
            assert page.locator('#v17p-mode-test').is_disabled()
            page.evaluate("window.__statusFail=true;refreshStatus()")
            page.wait_for_function("document.querySelector('#v17-db-service').textContent==='UNVERIFIED'")
            assert page.locator('#restart').is_disabled()
            assert not errors,errors
            assert not page.evaluate('document.documentElement.scrollWidth>innerWidth+1')
            print(f'{vw}x{vh}: broker, snapshot, mode coordinator, restart, reports, stop warning and fail-close PASS')
            ctx.close()
        browser.close()
    print('PASS: 2 viewports x 8 runtime/operation checks; no real requests')

if __name__=='__main__':run()
