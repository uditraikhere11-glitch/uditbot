"""V17 offline native-HTML browser validation, with deliberate synthetic statuses.

No HTTP/broker/systemd/Telegram calls and no mutation requests are allowed.
The design source is the user's saved V17 six-screen mobile preview.
"""
from __future__ import annotations
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from fastapi.testclient import TestClient
from playwright.sync_api import sync_playwright
from src.control_center_entry import app

OUT=Path(os.environ.get('V17_PREVIEW_EVIDENCE','v17-preview-evidence')).resolve()
MOCK={
 '/control/status': {
  'service_state':'RUNNING','service_detail':'Synthetic browser fixture — verified running process',
  'saved_mode':'LIVE','running_mode':'LIVE','runtime_verified':True,
  'transition_gate_active':False,'tracker_v2_apply_blocked':False,
  'equity_amo_saved':True,'fno_amo_saved':False,
  'equity_amo_can_edit':False,'fno_amo_can_edit':False,'fno_amo_active':False,
  'config_file_revision':'0'*64
 },
 '/control/health':{
  'summary':'Read-only health checks completed: PASS: 6, WARN: 1, FAIL: 0, UNKNOWN: 5',
  'checks':[
   {'name':'Trading service','status':'PASS','detail':'systemd state: RUNNING'},
   {'name':'Active mode/version','status':'PASS','detail':'Verified'},
   {'name':'Owner service intent','status':'WARN','detail':'Running service conflicts with saved Owner STOP/observed state'},
   {'name':'Emergency STOP','status':'PASS','detail':'Durable STOP flag clear (broker binding unverified)'},
   {'name':'Dhan broker authentication','status':'UNKNOWN','detail':'Read-only authenticated profile not performed'},
   {'name':'Dhan token file','status':'PASS','detail':'Token present, validity unverified'},
   {'name':'Order protection watcher','status':'UNKNOWN','detail':'Heartbeat and broker protection unverified'}
  ]},
 '/control/amo/orders':{'summary':'Synthetic journal fixture: no AMO orders verified','orders':[],'fno_local_queue':[]}
}


def run():
 OUT.mkdir(parents=True,exist_ok=True)
 results=[];errors=[];mutation_calls=[];setup_names=[]
 with TemporaryDirectory(prefix='v17_preview_') as tmp:
  old=os.getcwd();os.chdir(tmp)
  try:
   Path('data').mkdir()
   Path('.env').write_text('TRADING_MODE=TEST\nOPERATOR_NAME=Udit\nBOT_INSTANCE_NAME=UditBot\n',encoding='utf-8')
   Path('.env').chmod(0o600)
   with patch('src.webui_password.permitted_transport',return_value=True),patch('src.webui_password.password_configured',return_value=True),patch('src.webui_password.valid_session',return_value=True):
    client=TestClient(app)
    html={k:client.get(k).text for k in ['/','/setup','/setup/telegram-designer']}
    cfg=client.get('/setup/telegram-designer/config')
    assert cfg.status_code==200,(cfg.status_code,cfg.text[:400])
    data={**MOCK,'/setup/telegram-designer/config':cfg.json()}
    with sync_playwright() as p:
     browser=p.chromium.launch(headless=True,executable_path=os.getenv('V17_CHROMIUM','/usr/bin/chromium'),args=['--no-sandbox','--disable-dev-shm-usage'])
     try:
      for width,height in [(1440,900),(390,844)]:
       for appearance in ['light','dark']:
        ctx=browser.new_context(viewport={'width':width,'height':height},device_scale_factor=1,is_mobile=(width<600),has_touch=(width<600))
        page=ctx.new_page()
        page.on('pageerror',lambda exc:errors.append(str(exc)))
        page.route('**/*',lambda route:route.abort())
        for view in ['dashboard','control','setup','telegram','amo','health']:
         page.goto('about:blank')
         source=html['/'] if view in ('dashboard','control','amo','health') else html['/setup' if view=='setup' else '/setup/telegram-designer']
         page.evaluate('''data=>{window.__v17pCalls=[];window.fetch=async (url,opts={})=>{const path=String(url);const method=String(opts.method||'GET').toUpperCase();window.__v17pCalls.push({path,method});if(method!=='GET')throw Error('Mutation forbidden in preview audit');if(!(path in data))throw Error('Unexpected URL '+path);return {ok:true,status:200,redirected:false,json:async()=>data[path]};};}''',data)
         page.set_content(source,wait_until='load')
         page.wait_for_timeout(150)
         if appearance=='dark':
          page.locator('#premiumThemeToggle').click()
         if view in ('dashboard','control','amo','health'):
          page.evaluate('(v)=>{location.hash="#"+v}',view)
          page.wait_for_timeout(100)
          assert page.locator('#v17-'+view if view in ('dashboard','control') else '#v17-amo-settings' if view=='amo' else '#system-health').is_visible(),view
          if view=='dashboard':
           page.wait_for_function("document.getElementById('v17-db-service').textContent==='RUNNING'")
           page.wait_for_function("document.getElementById('v17p-owner-warning').hidden===false")
           assert 'Owner service intent' in page.locator('#v17p-owner-warning').inner_text()
           assert page.locator('#v17p-reconciliation').inner_text()=='UNVERIFIED'
           assert page.locator('#v17p-protection').inner_text()=='UNVERIFIED'
           assert page.locator('#v17p-gate').inner_text()=='UNVERIFIED'
           assert 'Not independently verified' in page.locator('#v17p-dhan').inner_text()
          if view=='control':
           page.wait_for_function("document.getElementById('stop').disabled===false")
           assert page.locator('#restart').is_disabled(), 'WARN Owner intent must block restart'
           assert page.locator('#start').is_disabled()
           assert page.locator('#stop').is_enabled()
           assert page.locator('#health').is_enabled()
          if view=='health':
           page.locator('#v17-health-run').click()
           page.wait_for_function("document.getElementById('health-state').textContent.includes('WARN: 1')")
           assert page.locator('.check').count()==len(data['/control/health']['checks'])
         if view=='setup':
          names=page.locator('form[action="/setup"] [name]').evaluate_all('(els)=>els.map(x=>x.name)')
          setup_names=names
          assert len(names)==54 and len(set(names))==54,(len(names),len(set(names)))
          assert page.locator('.v17p-setup-card').count()==4
          assert page.locator('form[action="/setup"]').is_hidden()
          page.locator('#v17pViewAllSettings').click()
          assert page.locator('form[action="/setup"]').is_visible()
          assert page.locator('form[action="/setup"] button[type="submit"]').count()==1
          # Return to preview landing before capture.
          page.evaluate("document.documentElement.classList.add('v17p-setup-collapsed'); document.querySelector('.v17p-setup-landing').hidden=false;window.scrollTo(0,0)")
         if view=='telegram':
          assert page.locator('#event').count()==1
          assert page.locator('#preview').count()==1
         assert page.locator('.v17p-topbar').is_visible()
         assert page.locator('.v17p-bottom-tabs').is_visible()==(width<600)
         assert page.locator('#v17p-runtime-chip').inner_text()=='LIVE'
         assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1'),(view,width,appearance,page.evaluate('document.documentElement.scrollWidth'))
         mutation_calls.extend([c for c in page.evaluate('window.__v17pCalls||[]') if c['method']!='GET'])
         dest=OUT/f'{view}_{width}_{appearance}.png'
         page.screenshot(path=str(dest),full_page=False)
         results.append({'screen':view,'width':width,'theme':appearance,'file':dest.name,'result':'PASS'})
        ctx.close()
     finally:
      browser.close()
  finally:
   os.chdir(old)
 assert not errors,errors
 assert not mutation_calls,mutation_calls
 assert len(results)==24,len(results)
 summary={'result':'PASS','browser_captures':len(results),'setup_unique_named_fields':len(set(setup_names)),'script_errors':errors,'mutation_requests':mutation_calls,'note':'Synthetic offline data. No live Dhan/Telegram or service action performed.','screens':results}
 (OUT/'RESULTS.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
 print('PASS: 24/24 native preview-inspired screens, 54 Setup fields, 0 JS errors, 0 mutation calls, 0 horizontal overflow')
 return summary

if __name__=='__main__':run()
