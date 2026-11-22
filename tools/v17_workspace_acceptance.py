"""Authenticated native-page visual/interaction acceptance, entirely offline.

No trading operations, systemd, broker calls, Telegram delivery or credentials.
The complete UI is rendered from the actual FastAPI GET response; REST status
and designer data are supplied by a deterministic in-browser stub.
"""
from __future__ import annotations
import json, os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from fastapi.testclient import TestClient
from playwright.sync_api import sync_playwright
from src.control_center_entry import app

OUT=Path(os.environ.get('V17_WORKSPACE_EVIDENCE_DIR','v17-workspace-evidence')).resolve()
STATUS={
 'service_state':'STOPPED_BY_OWNER','service_detail':'Test-only owner-stopped service',
 'saved_mode':'TEST','running_mode':'TEST','runtime_verified':False,
 'transition_gate_active':False,'tracker_v2_apply_blocked':False,
 'equity_amo_saved':False,'fno_amo_saved':False,
 'equity_amo_can_edit':False,'fno_amo_can_edit':False,'fno_amo_active':None,
 'config_file_revision':'0'*64
}
HEALTH={'summary':'Read-only health completed (synthetic)',
        'checks':[{'name':'Broker','status':'UNKNOWN','detail':'Never called in tests'}]}
ORDERS={'summary':'Journal synthetic fixture: no orders','orders':[],'fno_local_queue':[]}


def run() -> dict:
    OUT.mkdir(parents=True,exist_ok=True)
    results=[]; exceptions=[]; posts=[]
    with TemporaryDirectory(prefix='v17_workspace_') as tmp:
        oldcwd=os.getcwd();os.chdir(tmp)
        try:
            Path('data').mkdir()
            Path('.env').write_text('TRADING_MODE=TEST\nOPERATOR_NAME=Offline Test Only\n',encoding='utf-8')
            Path('.env').chmod(0o600)
            with patch('src.webui_password.permitted_transport',return_value=True), \
                 patch('src.webui_password.password_configured',return_value=True), \
                 patch('src.webui_password.valid_session',return_value=True):
                client=TestClient(app)
                html={k:client.get(k).text for k in ['/','/setup','/setup/telegram-designer']}
                config=client.get('/setup/telegram-designer/config')
                assert config.status_code==200,config.status_code
                data={'/control/status':STATUS,'/control/health':HEALTH,'/control/amo/orders':ORDERS,
                      '/setup/telegram-designer/config':config.json()}
                with sync_playwright() as play:
                    browser=play.chromium.launch(executable_path=os.getenv('V17_CHROMIUM','/usr/bin/chromium'),headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
                    try:
                        for width,height in [(1440,900),(390,844)]:
                            context=browser.new_context(viewport={'width':width,'height':height},device_scale_factor=1,is_mobile=width<500,has_touch=width<500)
                            page=context.new_page();page.on('pageerror',lambda e:exceptions.append(str(e)))
                            page.route('**/*',lambda route:route.abort())
                            for appearance in ['dark','light']:
                                for view in ['dashboard','control','amo','health','setup','telegram']:
                                    page.goto('about:blank')
                                    page.evaluate('data=>{window.fetch=async(url,init)=>{const path=String(url);window.__v17calls=(window.__v17calls||[]).concat(path);if(!(path in data))throw new Error("Unexpected offline fetch "+path);return {ok:true,status:200,redirected:false,json:async()=>data[path]};}}',data)
                                    source=html['/'] if view in ['dashboard','control','amo','health'] else html['/setup' if view=='setup' else '/setup/telegram-designer']
                                    page.set_content(source,wait_until='load')
                                    if appearance=='light':
                                        page.evaluate("document.getElementById('premiumThemeToggle').click()")
                                        page.wait_for_timeout(250)  # allow intentional 160ms drawer/link transitions to settle
                                    assert page.evaluate('document.documentElement.classList.contains("v17-light")') == (appearance=='light')
                                    if view in ('dashboard','control','amo','health'):
                                        page.evaluate("v=>{window.location.hash='#'+v; v17ShowView()}",view)
                                        expected={'dashboard':['v17-dashboard'], 'control':['v17-control'],'amo':['v17-amo-settings','amo-orders'],'health':['system-health']}[view]
                                        for id in ['v17-dashboard','v17-control','v17-amo-settings','amo-orders','system-health']:
                                            assert page.locator('#'+id).is_visible()==(id in expected),(view,id)
                                        active=page.locator('[data-v17-view][aria-current="page"]')
                                        assert active.count()==1 and active.get_attribute('data-v17-view')==view
                                        if view=='dashboard':
                                            page.wait_for_function("document.getElementById('v17-db-service').textContent==='STOPPED BY OWNER'")
                                            assert page.locator('#v17-db-verified').inner_text()=='UNVERIFIED'
                                            for label in ['Trading Controls','Configuration','System Health','AMO Orders','Telegram Designer']:
                                                assert page.locator('.v17-quick-grid').get_by_text(label).count()>=1,label
                                        if view=='health':
                                            page.locator('#v17-health-run').click()
                                            page.wait_for_function("document.getElementById('health-state').textContent.includes('Read-only health completed')")
                                            assert page.locator('#v17-health-run').is_enabled()
                                    elif view=='setup':
                                        names=page.locator('form[action="/setup"] [name]').evaluate_all('(els)=>els.map(e=>e.name)')
                                        assert len(names)==54 and len(set(names))==54
                                    # Rendering must never issue a mutation POST.
                                    calls=page.evaluate('window.__v17calls || []')
                                    assert all(not x.startswith('/setup/service-control') and not x.startswith('/control/amo/fno/cancel') for x in calls)
                                    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth+1'),(width,appearance,view)
                                    dest=OUT/f'{view}_{width}_{appearance}.png'
                                    page.screenshot(path=str(dest),full_page=False)
                                    results.append({'view':view,'width':width,'appearance':appearance,'status':'PASS','screenshot':dest.name})
                            context.close()
                    finally:browser.close()
        finally:os.chdir(oldcwd)
    assert not exceptions,exceptions
    assert len(results)==24,len(results)
    report={'status':'PASS','screenshots':len(results),'screens':results,'script_errors':exceptions,'broker_calls':0,'systemd_actions':0,'mutations':0,
            'limitations':['Only three actual FastAPI GET templates; views are hash-based presentation states','Read-only endpoints mocked; no live broker, Telegram or VM integration','Approved artwork is a mixed-resolution collage and no pixel-equivalent SSIM is certified']}
    (OUT/'RESULTS.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print('PASS: 24/24 native desktop/iPhone dark/light view captures, 54 Setup names, 0 overflow, 0 script errors, 0 mutation requests')
    return report

if __name__=='__main__':run()
