"""Offline Chromium verification of Control Center's fail-closed response handling.

Run from repository root: PYTHONPATH=. python tools/v17_control_state_browser_test.py
Uses browser-local mock fetch and no navigation; no HTTP, broker or service calls.
"""
import os
import tempfile
from pathlib import Path
from playwright.sync_api import sync_playwright
from src.control_center_ui import render_control_center

MOCK_JS = r'''
window.__calls = [];
window.__expired = false;
window.__redirect = false;
window.__reject = false;
window.__postOccurred = false;
window.__failAfterPost = false;
window.fetch = async (url, opts={}) => {
  const path = String(url);
  window.__calls.push({path, body: opts.body || ''});
  if(path === '/control/status') {
    if(window.__postOccurred && window.__failAfterPost) return {ok:true,status:200,json:async()=>{throw new SyntaxError('status failure')}};
    if(window.__expired) return {ok:true,status:200,json:async()=>{throw new SyntaxError('HTML login')}};
    if(window.__redirect) return {ok:true,status:200,redirected:true,url:'https://example.test/login',json:async()=>({})};
    return {ok:true,status:200,json:async()=>({
      service_state:'RUNNING',service_detail:'Verified local test service',
      saved_mode:'LIVE',running_mode:'LIVE',runtime_verified:true,
      transition_gate_active:false,tracker_v2_apply_blocked:false,
      equity_amo_saved:false,fno_amo_saved:false,
      equity_amo_can_edit:false,fno_amo_can_edit:false
    })};
  }
  if(path === '/control/amo/orders') return {ok:true,status:200,json:async()=>({orders:[],summary:'None'})};
  if(path === '/setup/service-control') {
    window.__postOccurred = true;
    const body=JSON.parse(opts.body||'{}');
    if(body.action !== 'stop' || body.csrf_token !== 'testing-only-csrf')
      throw new Error('Wrong service request payload');
    if(window.__reject) return {ok:false,status:403,json:async()=>({error:'Invalid CSRF token'})};
    return {ok:true,status:200,json:async()=>({requires_live_confirmation:true,message:'Confirmation required'})};
  }
  throw new Error('Unexpected fetch '+path);
};
'''


def main():
    browser_path = os.environ.get('V17_CHROMIUM','/usr/bin/chromium')
    assert Path(browser_path).is_file(), 'Chromium executable unavailable'
    with tempfile.TemporaryDirectory(prefix='v17_control_') as temp:
        page_file=Path(temp)/'control.html'
        page_file.write_text(render_control_center('testing-only-csrf'),encoding='utf-8')
        with sync_playwright() as p:
            browser=p.chromium.launch(headless=True, executable_path=browser_path,
                                      args=['--no-sandbox','--disable-dev-shm-usage'])
            for viewport in [{'width':1440,'height':900},{'width':390,'height':844}]:
                context=browser.new_context(viewport=viewport, is_mobile=viewport['width'] < 500)
                page=context.new_page()
                page.on('dialog',lambda dialog:dialog.accept())
                page.evaluate("(()=>{" + MOCK_JS + "; return true;})()")
                page.set_content(page_file.read_text(encoding='utf-8'), wait_until='load')
                # V17 now opens on Dashboard; explicitly select native Control view.
                page.evaluate("location.hash='#control'")
                page.wait_for_function("document.getElementById('stop').disabled === false")
                assert page.locator('#service-beacon').get_attribute('class').find('is-active')>=0, 'Verified RUNNING beacon not active'
                assert page.locator('#mode-beacon').get_attribute('class').find('is-active')>=0, 'Verified LIVE beacon not active'
                screenshot_dir = os.environ.get('V17_SCREENSHOT_DIR')
                if screenshot_dir:
                    Path(screenshot_dir).mkdir(parents=True, exist_ok=True)
                    page.screenshot(path=str(Path(screenshot_dir)/f'control_{viewport["width"]}.png'),full_page=True)
                assert page.evaluate('document.documentElement.scrollWidth <= window.innerWidth+1'), 'Horizontal overflow'
                initial_count=page.evaluate("__calls.filter(x=>x.path==='/control/status').length")
                page.locator('#stop').click()
                page.wait_for_function("document.getElementById('info').textContent.includes('LIVE action requires explicit confirmation')")
                counts=page.evaluate("({posts:__calls.filter(x=>x.path==='/setup/service-control').length, statuses:__calls.filter(x=>x.path==='/control/status').length})")
                assert counts['posts']==1, counts
                assert counts['statuses']==initial_count+1, counts
                assert not page.locator('#stop').is_disabled(), 'Stop remained disabled after backend confirmation requirement'
                assert page.locator('#start').is_disabled(), 'Start must stay disabled while RUNNING'
                print(f'{viewport["width"]}x{viewport["height"]} confirmation refresh: PASS')
                page.evaluate('window.__expired = true; refreshStatus()')
                page.wait_for_function("document.getElementById('info').textContent.includes('Control response was not JSON')")
                assert page.locator('#stop').is_disabled()
                assert page.locator('#start').is_disabled()
                assert 'is-active' not in page.locator('#service-beacon').get_attribute('class')
                assert 'is-active' not in page.locator('#mode-beacon').get_attribute('class')
                print(f'{viewport["width"]}x{viewport["height"]} expired-session fail-closed: PASS')
                page.evaluate('window.__expired = false; window.__redirect = true; refreshStatus()')
                page.wait_for_function("document.getElementById('info').textContent.includes('WebUI session expired')")
                assert page.locator('#stop').is_disabled()
                print(f'{viewport["width"]}x{viewport["height"]} login redirect fail-closed: PASS')
                page.evaluate('window.__redirect = false; refreshStatus()')
                page.wait_for_function("document.getElementById('stop').disabled === false")
                page.evaluate('window.__reject = true')
                page.locator('#stop').click()
                page.wait_for_function("document.getElementById('info').textContent.includes('Invalid CSRF token')")
                assert not page.locator('#stop').is_disabled(), 'Stop stayed disabled after rejected request'
                assert page.evaluate("__calls.filter(x=>x.path==='/setup/service-control').length")==2
                print(f'{viewport["width"]}x{viewport["height"]} rejected service request refresh: PASS')
                page.evaluate('window.__reject = false; window.__failAfterPost = true')
                page.locator('#stop').click()
                page.wait_for_function("document.getElementById('info').textContent.includes('Cannot verify bot state')")
                assert page.locator('#stop').is_disabled()
                assert page.locator('#start').is_disabled()
                assert 'requires explicit confirmation' not in page.locator('#info').text_content()
                print(f'{viewport["width"]}x{viewport["height"]} post-request verification failure: PASS')
                context.close()
            browser.close()
    print('PASS: 10/10 browser scenarios; zero actual HTTP or broker calls.')

if __name__=='__main__':main()
