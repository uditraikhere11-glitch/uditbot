"""Offline authenticated Setup render + category navigation regression.

No broker, Telegram, systemd, network or Save & Apply requests are made.
Use: PYTHONPATH=. python tools/v17_setup_category_browser.py
"""
import os
from pathlib import Path
import tempfile
from playwright.sync_api import sync_playwright
from src.setup_app import get_setup_page

ROOT=Path(__file__).resolve().parents[1]
TARGETS=['setup-execution','setup-telegram','setup-broker','setup-sizing','setup-risk','setup-protection']
OUT=Path(os.getenv('V17_SETUP_CAPTURE_DIR','/mnt/data/v17_setup_refactor/evidence'))
OUT.mkdir(parents=True,exist_ok=True)

with tempfile.TemporaryDirectory(prefix='v17_setup_readonly_') as td:
    old=os.getcwd()
    os.chdir(td)
    try:
        Path('.env').write_text('OPERATOR_NAME=Udit\nDHAN_CLIENT_ID=1234567890\n',encoding='utf-8')
        html=get_setup_page().body.decode('utf-8')
    finally:
        os.chdir(old)

with sync_playwright() as p:
    browser=p.chromium.launch(executable_path='/usr/bin/chromium',headless=True,args=['--no-sandbox'])
    for size in [(390,844),(1440,900)]:
        width,height=size
        page=browser.new_page(viewport={'width':width,'height':height},device_scale_factor=1)
        errors=[];requests=[]
        page.on('pageerror',lambda err:errors.append(str(err)))
        page.on('request',lambda req:requests.append((req.method,req.url)))
        # All mocked read-only fetch calls are rejected; rendering uses no network.
        page.route('**/*',lambda route:route.abort())
        page.set_content(html,wait_until='load')
        page.wait_for_timeout(120)
        assert page.locator('.v17p-dhan-badge').count()==1
        assert '***7890' in page.locator('.v17p-dhan-badge').inner_text()
        assert 'Connected' not in page.locator('.v17p-dhan-badge').inner_text()
        assert '1234567890' not in page.locator('.v17p-dhan-badge').inner_text()
        assert page.locator('.system-status-panel').count()==0
        assert page.locator('.v17p-setup-card').count()==6
        assert not any('AMO & Orders' in text for text in page.locator('.v17p-setup-card').all_inner_texts())
        assert page.locator('.v17p-setup-amo-link').get_attribute('href')=='/#amo'
        assert page.locator('form[action="/setup"]').count()==1
        assert len(set(page.locator('form[action="/setup"] [name]').evaluate_all('(els)=>els.map(el=>el.name)')))==54
        assert not page.locator('form[action="/setup"]').is_visible()
        page.screenshot(path=str(OUT/f'setup_{width}_landing.png'),full_page=False)
        expected={
            'setup-execution':'Trading Mode & Segments',
            'setup-telegram':'Signal & Telegram',
            'setup-broker':'Dhan Credentials',
            'setup-sizing':'Sizing & Pricing',
            'setup-risk':'Risk & Limits',
            'setup-protection':'Protection & Timing'}
        for id,label in expected.items():
            if not page.locator('.v17p-setup-landing').is_visible():
                page.locator('.v17p-back-setup').click()
            page.locator(f'.v17p-setup-card[data-target="{id}"]').click()
            assert page.locator('form[action="/setup"]').is_visible(),id
            assert page.locator(f'#{id}').is_visible(),id
            assert all(not page.locator(f'#{other}').is_visible() for other in TARGETS if other!=id)
            assert page.locator('#v17p-setup-current-title').inner_text()==label
            assert page.locator('form[action="/setup"] .btn-submit').is_visible()
            consent=page.locator('input[name="confirm_live"]').locator('xpath=..')
            if id=='setup-execution': assert consent.is_visible()
            else: assert not consent.is_visible()
            # Each category retained exactly the native form (no duplicate fields).
            assert page.locator('form[action="/setup"]').count()==1
            if id in {'setup-broker','setup-sizing','setup-risk'}:
                page.screenshot(path=str(OUT/f'setup_{width}_{id}.png'),full_page=False)
        # Tabs show one native section at a time; values are never cloned/reset.
        page.locator('.setup-section-jumps a[href="#setup-sizing"]').click()
        amount=page.locator('[name="equity_budget_inr"]')
        amount.fill('1777')
        page.locator('.setup-section-jumps a[href="#setup-risk"]').click()
        page.locator('.setup-section-jumps a[href="#setup-sizing"]').click()
        assert amount.input_value()=='1777'
        page.locator('.v17p-back-setup').click()
        page.locator('#v17pViewAllSettings').click()
        assert all(page.locator(f'#{id}').is_visible() for id in TARGETS)
        assert page.locator('form[action="/setup"] .btn-submit').is_visible()
        page.screenshot(path=str(OUT/f'setup_{width}_all.png'),full_page=False)
        page.locator('.setup-section-jumps a[href="#setup-broker"]').click()
        assert page.locator('#setup-broker').is_visible()
        assert not page.locator('#setup-risk').is_visible()
        assert not errors,errors
        assert all(method=='GET' for method,url in requests),requests
        viewport=page.evaluate('({w:document.documentElement.scrollWidth,v:innerWidth})')
        assert viewport['w']<=viewport['v']+2, viewport
        print(f'PASS {width}x{height}: 6 links, 54 unique inputs, all/current/category switching, masked header, no errors')
        page.close()
    browser.close()
