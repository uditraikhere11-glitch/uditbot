"""V17 isolated browser acceptance against actual authenticated FastAPI GET routes.

Synthetic local config, patched authentication, intercepted transport; **no real
network, broker, Telegram, systemd, token refresh, or trading mutation**.
"""
from __future__ import annotations
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit
from unittest.mock import patch
from fastapi.testclient import TestClient
from playwright.sync_api import sync_playwright

from src.control_center_entry import app

VIEWPORTS = [(1440, 900), (390, 844)]
HOST = 'http://127.0.0.1:8888'

MOCK_STATUS = {
  'service_state':'STOPPED_BY_OWNER','service_detail':'Synthetic stopped-service fixture',
  'saved_mode':'TEST','running_mode':'TEST','runtime_verified':True,
  'transition_gate_active':False,'tracker_v2_apply_blocked':False,
  'equity_amo_saved':False,'fno_amo_saved':False,'equity_amo_can_edit':False,
  'fno_amo_can_edit':False,'fno_amo_active':False,'config_file_revision':'TEST_REV',
}
MOCK_HEALTH = {'summary':'Synthetic read-only checks completed',
               'checks':[{'name':'Broker', 'status':'NOT CALLED','detail':'Offline fixture'},
                         {'name':'Service','status':'STOPPED','detail':'Owner stopped fixture'}]}
MOCK_ORDERS = {'summary':'No synthetic AMO orders','orders':[],'fno_local_queue':[]}


def audit(out: Path) -> dict:
    out = out.resolve()
    out.mkdir(parents=True,exist_ok=True)
    errors = []
    results = []
    with TemporaryDirectory(prefix='v17_native_route_') as tmp:
        previous = os.getcwd()
        os.chdir(tmp)
        try:
            Path('data').mkdir()
            Path('.env').write_text('OPERATOR_NAME=Offline Test Operator\nBOT_INSTANCE_NAME=TestBot\nTRADING_MODE=TEST\n',encoding='utf-8')
            Path('.env').chmod(0o600)
            with (patch('src.webui_password.permitted_transport',return_value=True),
                  patch('src.webui_password.password_configured',return_value=True),
                  patch('src.webui_password.valid_session',return_value=True)):
                client=TestClient(app)
                routes={'Dashboard':'/','Control':'/','Setup':'/setup',
                        'Telegram':'/setup/telegram-designer','AMO':'/','Health':'/'}
                # Assert that the server itself produced successful authenticated
                # native pages; a synthetic HTML fixture is not sufficient.
                actual={path:client.get(path) for path in sorted(set(routes.values()))}
                for path,response in actual.items():
                    assert response.status_code==200,(path,response.status_code)
                    assert '<html' in response.text.lower(),path
                assert len(actual)==3
                with sync_playwright() as playwright:
                    browser=playwright.chromium.launch(executable_path=os.environ.get('V17_CHROMIUM','/usr/bin/chromium'),headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
                    try:
                        for width,height in VIEWPORTS:
                            context=browser.new_context(viewport={'width':width,'height':height},device_scale_factor=1,is_mobile=(width<500),has_touch=(width<500))
                            page=context.new_page()
                            page.on('pageerror',lambda err: errors.append(f'{width}: {err}'))
                            # Chromium in this isolated host disallows HTTP navigation,
                            # even when Playwright intercepts it. Use the bytes returned
                            # by real authenticated FastAPI TestClient GET routes.
                            page.route('**/*',lambda route:route.fulfill(status=200,content_type='text/css',body=''))
                            designer=client.get('/setup/telegram-designer/config')
                            assert designer.status_code==200,designer.status_code
                            rest={'/control/status':MOCK_STATUS,'/control/health':MOCK_HEALTH,
                                  '/control/amo/orders':MOCK_ORDERS,
                                  '/setup/telegram-designer/config':designer.json()}
                            for screen,path in routes.items():
                                page.goto('about:blank')
                                page.evaluate("(data) => { window.fetch = async (url,init) => { const v = data[String(url)]; if (!v) throw new Error('Unexpected offline fetch: '+url); return {ok:true,status:200,json:async()=>v}; }; }",rest)
                                page.set_content(actual[path].text,wait_until='load')
                                if screen in ('Dashboard','Control','AMO','Health'):
                                    page.wait_for_function("document.getElementById('service').textContent !== 'Checking…'")
                                if screen=='Telegram':
                                    # A real GET to the native template configuration;
                                    # the browser callback uses TestClient under isolation.
                                    page.wait_for_timeout(350)
                                if screen=='Dashboard':
                                    page.evaluate("window.location.hash='#dashboard'; v17ShowView()")
                                    page.wait_for_function("!document.getElementById('v17-dashboard').hidden")
                                if screen=='AMO':
                                    page.evaluate("window.location.hash='#amo'; v17ShowView()")
                                    page.locator('#amo-orders').scroll_into_view_if_needed()
                                if screen=='Health':
                                    page.evaluate("window.location.hash='#health'; v17ShowView()")
                                    # The Health button lives in the Control view; invoke its
                                    # same click handler after switching back without a live call.
                                    page.evaluate("document.getElementById('health').click()")
                                    page.wait_for_function("document.getElementById('health-state').textContent.includes('Synthetic read-only')")
                                    page.locator('#system-health').scroll_into_view_if_needed()
                                if screen=='Setup':
                                    names=page.locator('form[action="/setup"] [name]').evaluate_all('(nodes)=>nodes.map(n=>n.name)')
                                    assert len(names)==54 and len(set(names))==54,(width,len(names),names)
                                    visible=page.locator('form[action="/setup"] [name]:not([type="hidden"])').count()
                                    assert visible==49,(width,visible)
                                    for name in names:
                                        if name in ('csrf_token','expected_revision','full_setup_form','sl_pct','target_pct'):
                                            continue
                                        el=page.locator(f'form[action="/setup"] [name="{name}"]')
                                        assert el.count()==1
                                        if not el.is_visible():
                                            # Native switch checkboxes are intentionally CSS-hidden;
                                            # the visible parent label is the interactive surface.
                                            assert (el.get_attribute('type')=='checkbox' and
                                                    el.locator('xpath=parent::label').is_visible()),(width,name,'not reachable')
                                overflow=page.evaluate('({document:document.documentElement.scrollWidth,viewport:innerWidth})')
                                assert overflow['document']<=overflow['viewport']+1,(width,screen,overflow)
                                screenshot=out/f'{screen.lower()}_{width}.png'
                                page.screenshot(path=str(screenshot),full_page=False)
                                results.append({'screen':screen,'viewport':f'{width}x{height}','result':'PASS','screenshot':screenshot.name,'overflow':overflow})
                            context.close()
                    finally:
                        browser.close()
        finally:
            os.chdir(previous)
    report={'method':'Three genuine authenticated FastAPI TestClient GET responses mounted in Chromium document + mocked read-only REST fetches','native_routes':['/','/setup','/setup/telegram-designer'],
            'states':['Dashboard','Control','Setup','Telegram','AMO','Health'],
            'viewports':VIEWPORTS,'snapshots':results,'page_errors':errors,
            'native_setup_named_controls':54,'visible_named_controls':49,'external_network_calls':0,'live_actions':0,
            'limitation':'Dashboard and Control use the same native / route; AMO and Health are anchored states, not standalone URLs. Visual artwork SSIM not established.'}
    (out/'NATIVE_ROUTE_RESULTS.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    assert len(results)==12,(len(results),errors)
    assert not errors,errors
    print('PASS: 12/12 native-renderer viewport captures, 54/54 Setup names, zero horizontal overflow/page errors')
    return report

if __name__=='__main__':
    audit(Path(os.environ.get('V17_EVIDENCE_DIR','v17_native_evidence')))
