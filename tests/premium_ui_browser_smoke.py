"""Offline Chromium smoke: actual native Setup template and Control Center HTML.

Playwright is installed only by premium-webui-review.yml. This file is not
named test_*.py so other offline unittest workflows do not require a browser.
No requests reach network: all browser URLs are intercepted and synthetic.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import tempfile

from playwright.sync_api import sync_playwright
from src.setup_app import HTML_TEMPLATE
from src.control_center_ui import render_control_center
from src.premium_ui import premium_nav, premium_nav_js, premium_styles

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "review-out" / "visuals"
OUT.mkdir(parents=True, exist_ok=True)
SECTIONS = (
    "setup-execution", "setup-telegram", "setup-broker",
    "setup-sizing", "setup-risk", "setup-protection",
)


def offline_setup() -> str:
    page = (HTML_TEMPLATE
            .replace("<!--PREMIUM_STYLES-->", premium_styles())
            .replace("<!--PREMIUM_NAV-->", premium_nav("setup"))
            .replace("/*PREMIUM_NAV_JS*/", premium_nav_js())
            .replace("<!--SYSTEM_STATUS_BAR-->", "")
            .replace("<!--STATUS_BANNER-->", ""))
    # Synthetic values only: no passwords, tokens, accounts or real forms.
    for key in set(re.findall(r"<!--([A-Z][A-Z0-9_]*)-->", page)):
        page = page.replace("<!--" + key + "-->", "")
    return page


def assert_setup(page, width: int, setup_html: str) -> None:
    page.goto("http://uditbot.test/setup", wait_until="domcontentloaded")
    page.locator(".v17p-setup-card[data-target='setup-execution']").wait_for()
    page.locator(".v17p-setup-card[data-target='setup-execution']").click()
    # Drawer starts closed and must not be a clipped sidebar within Setup.
    menu = page.evaluate("""() => {
      const panel=document.getElementById('premNav');
      const backdrop=document.getElementById('premNavBackdrop');
      return {parent:panel.parentElement.tagName,
              visible:getComputedStyle(panel).visibility,
              right:panel.getBoundingClientRect().right,
              expanded:document.getElementById('v17pAccountOpen').getAttribute('aria-expanded'),
              backdropHidden:backdrop.hidden};
    }""")
    assert menu["parent"] == "BODY", (width, menu)
    assert menu["visible"] == "hidden" and menu["right"] <= 1, (width, menu)
    assert menu["expanded"] == "false" and menu["backdropHidden"], (width, menu)
    page.locator("#v17pAccountOpen").click()
    assert page.locator("#premNav").is_visible(), width
    page.keyboard.press("Escape")
    assert not page.locator("#premNav").is_visible(), width
    assert page.locator("#v17pAccountOpen").get_attribute("aria-expanded") == "false"
    # The same SVG mark is used in the topbar and account drawer.
    assert page.locator(".v17p-brand svg.v17p-ub-mark .v17p-ub-letter").count() == 2
    for section in SECTIONS:
        if section != SECTIONS[0]:
            page.locator('.setup-section-jumps a[href="#' + section + '"]').click()
        assert page.locator("#" + section).is_visible(), (width, section)
        geometry = page.evaluate("""(id) => {
            const form=document.querySelector('form[action="/setup"]');
            const active=document.getElementById(id);
            const nav=form.querySelector('.setup-section-jumps');
            const top=document.querySelector('.v17p-topbar');
            const fr=form.getBoundingClientRect(), ar=active.getBoundingClientRect();
            const nc=getComputedStyle(nav);
            return {
                formWidth:fr.width, cardWidth:ar.width, formLeft:fr.left,
                navPosition:nc.position, navFilter:nc.backdropFilter,
                navBg:nc.backgroundColor, topHeight:top.getBoundingClientRect().height,
                hiddenCards:[...form.querySelectorAll('.section-card')].filter(el=>!el.hidden).length,
                horizontalOverflow:document.documentElement.scrollWidth > innerWidth+1,
            };
        }""", section)
        assert geometry["cardWidth"] >= geometry["formWidth"] - 2, (width, section, geometry)
        assert geometry["formWidth"] <= 1041, (width, section, geometry)
        assert geometry["navPosition"] == "static", (width, section, geometry)
        assert geometry["navFilter"] in ("none", ""), (width, section, geometry)
        assert geometry["hiddenCards"] == 1, (width, section, geometry)
        assert not geometry["horizontalOverflow"], (width, section, geometry)
        if section == SECTIONS[0] and width in (1280, 1440, 1600, 1920):
            page.evaluate("window.scrollTo(0, 0)")
            page.wait_for_function("window.scrollY === 0")
            page.screenshot(path=str(OUT / ("setup-" + str(width) + "-light.png")), full_page=True)
            if width == 1440:
                page.locator(".v17p-brand").screenshot(path=str(OUT / "monogram-1440-light.png"))
        if width == 1920 and section in ("setup-sizing", "setup-risk", "setup-protection"):
            page.screenshot(path=str(OUT / (section + "-1920.png")), full_page=True)
    assert page.locator("#v17pViewAllSettings").count() == 1
    # Native form: no clone, no second POST form, exact toggle payload.
    check = page.evaluate("""() => {
        const form=document.querySelector('form[action="/setup"]');
        const names=[...form.querySelectorAll('[name]')].map(x=>x.name);
        const keys=['is_live_mode','equity_active','fno_active'];
        const initially=Object.fromEntries(keys.map(x=>[x,new FormData(form).has(x)]));
        for(const k of keys)form.querySelector('[name="'+k+'"]').checked=true;
        const checked=Object.fromEntries(keys.map(x=>[x,new FormData(form).get(x)]));
        for(const k of keys)form.querySelector('[name="'+k+'"]').checked=false;
        const unchecked=Object.fromEntries(keys.map(x=>[x,new FormData(form).has(x)]));
        return {forms:document.querySelectorAll('form[action="/setup"]').length,
                count:names.length,unique:new Set(names).size,initially,checked,unchecked,
                csrf:names.includes('csrf_token'),rev:names.includes('expected_revision'),
                confirm:names.includes('confirm_live')};
    }""")
    assert check["forms"] == 1 and check["count"] == check["unique"] == 54, check
    assert check["csrf"] and check["rev"] and check["confirm"], check
    assert all(v == "true" for v in check["checked"].values()), check
    assert not any(check["unchecked"].values()), check


def assert_control(page, runtime_verified: bool, running_mode: str, expected: str) -> None:
    page.goto("http://uditbot.test/#control", wait_until="domcontentloaded")
    page.wait_for_function("() => document.getElementById('mode').textContent !== 'UNKNOWN'")
    page.wait_for_function("(want) => document.getElementById('mode').textContent === want",
                           arg=expected)
    service = page.locator("#service").inner_text()
    assert service == ("RUNNING" if runtime_verified else "UNVERIFIED"), service
    assert page.locator("#mode").inner_text() == expected
    assert page.locator("#mode-beacon").evaluate(
        "el=>el.classList.contains('is-active')"
    ) == (runtime_verified and running_mode == "LIVE")
    assert page.locator("#mode-beacon").evaluate(
        "el=>el.classList.contains('is-test')"
    ) == (runtime_verified and running_mode == "TEST")
    if runtime_verified:
        assert page.locator("#service-beacon").evaluate(
            "el=>el.classList.contains('is-active')"
        )
    else:
        assert not page.locator("#service-beacon").evaluate(
            "el=>el.classList.contains('is-active')"
        )


def main() -> None:
    # Entirely synthetic filesystem context for rendering display labels.
    with tempfile.TemporaryDirectory() as temp:
        os.chdir(temp)
        setup_html = offline_setup()
        control_html = render_control_center("SYNTHETIC_CSRF")
        state = dict(service_state="RUNNING", runtime_verified=False,
                     running_mode="LIVE", saved_mode="LIVE",
                     owner_intent_status="PASS", mode_can_edit=False,
                     protection_watcher="UNVERIFIED", new_entries_gate="BLOCKED")
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                for width in (1280, 1440, 1600, 1920, 390):
                    context = browser.new_context(viewport={"width": width, "height": 900 if width != 390 else 844},
                                                  reduced_motion="no-preference")
                    page = context.new_page()

                    def reply(route):
                        url = route.request.url
                        if url.endswith("/control/status"):
                            return route.fulfill(status=200, content_type="application/json",
                                                 body=json.dumps(state))
                        if url.endswith("/control/health"):
                            return route.fulfill(status=200, content_type="application/json",
                                                 body=json.dumps({"checks":[]}))
                        if url.endswith("/control/broker-status"):
                            return route.fulfill(status=200, content_type="application/json",
                                                 body=json.dumps({"status":"UNVERIFIED"}))
                        if url.endswith("/setup"):
                            return route.fulfill(status=200, content_type="text/html", body=setup_html)
                        if url.endswith("/") or "/#control" in url:
                            return route.fulfill(status=200, content_type="text/html", body=control_html)
                        return route.abort()

                    page.route("**/*", reply)
                    assert_setup(page, width, setup_html)
                    if width == 390:
                        page.locator('.setup-section-jumps a[href="#setup-execution"]').click()
                        page.locator("#premiumThemeToggle").click()
                        page.evaluate("window.scrollTo(0, 0)")
                        page.wait_for_function("window.scrollY === 0")
                        page.screenshot(path=str(OUT / "setup-390-dark.png"), full_page=True)
                        page.locator(".v17p-brand").screenshot(path=str(OUT / "monogram-390-dark.png"))
                    if width in (1280, 390):
                        for verified, mode, expect in ((False,"LIVE","UNVERIFIED"),
                                                       (True,"LIVE","LIVE"),
                                                       (True,"TEST","TEST")):
                            state.update(runtime_verified=verified,running_mode=mode)
                            assert_control(page, verified, mode, expect)
                        page.screenshot(path=str(OUT / ("control-"+str(width)+".png")), full_page=True)
                    context.close()
            finally:
                browser.close()
    print("PASS: 6 Setup views at 1280/1440/1600/1920/390, native form payload,")
    print("      runtime verification states, SVG/theme snapshots; no external requests.")


if __name__ == "__main__":
    main()
