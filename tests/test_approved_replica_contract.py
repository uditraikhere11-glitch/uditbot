"""Visual-only six-section contracts: prove no working input was redesigned.

Protects the exact previously working PR #100 HTML template (commit 7688eec).
No broker calls, network calls, service starts, or configuration writes.
"""
from __future__ import annotations

import ast
import hashlib
from html.parser import HTMLParser
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_CANONICAL_TEMPLATE_SHA256 = "4993337e7676744596d82c85bc057ebad6cfd74096086395349db37d667e6a17"
EXPECTED_SETUP_HANDLERS_SHA256 = "e46d1d36edb2d202db7a67080315ea5c032b6e8eee000edfcbebdb1f3df8a3bc"
EXPECTED_NAV_SCRIPT_SHA256 = "d6efc7afb4e5fb7b02f1b9ca91f80cde91e9a09079c6002eea95bb101f5d21f7"


def source(path: str) -> str:
    content = (ROOT / path).read_text(encoding="utf-8")
    if path.endswith(".py"):
        ast.parse(content)
    return content


def setup_template() -> str:
    tree = ast.parse(source("src/setup_app.py"))
    return next(n.value.value for n in tree.body if isinstance(n, ast.Assign)
                for target in n.targets if isinstance(target, ast.Name)
                and target.id == "HTML_TEMPLATE")


class Inputs(HTMLParser):
    def __init__(self):
        super().__init__()
        self.named = []

    def handle_starttag(self, tag, attrs):
        if tag in ("input", "select", "textarea"):
            attr = dict(attrs)
            if attr.get("name"):
                self.named.append((tag, attr))


class RealSixCardUIContracts(unittest.TestCase):
    def test_every_input_definition_is_identical_to_pre_replica_setup(self):
        # Locks input type, required, step, min, max, checked, select options,
        # form names, and native browser validation in the FULL original template.
        page = setup_template()
        self.assertEqual(hashlib.sha256(page.encode()).hexdigest(),
                         EXPECTED_CANONICAL_TEMPLATE_SHA256)

    def test_original_form_handlers_and_config_methods_are_unchanged(self):
        code = source("src/setup_app.py")
        self.assertEqual(hashlib.sha256(code[code.index("def _extract_val("):].encode()).hexdigest(),
                         EXPECTED_SETUP_HANDLERS_SHA256)
        self.assertIn("fetch('/setup/save-apply'", code)
        self.assertIn("name=\"csrf_token\"", code)
        self.assertIn("name=\"expected_revision\"", code)
        self.assertIn("name=\"confirm_live\"", code)
        self.assertIn("result.status === 'RESTARTED' && result.restart_performed === true", code)

    def test_six_real_section_cards_not_four_new_proxy_cards(self):
        page = setup_template()
        for name in ("execution", "telegram", "broker", "sizing", "risk", "protection"):
            self.assertIn(f'id="setup-{name}" class="section-card"', page)
        self.assertEqual(page.count('class="section-card"'), 6)
        for removed in ("replicaBoard", "replicaAdvanced", "replicaApply", "data-mirror",
                        "APPROVED_REFERENCE_BOARD", "replicaQuoteAge", "replicaSaveOnly"):
            self.assertNotIn(removed, source("src/setup_app.py"))

    def test_all_54_authoritative_fields_exist_once(self):
        parser = Inputs()
        parser.feed(setup_template())
        names = [attrs["name"] for _, attrs in parser.named]
        self.assertEqual(len(names), 54)
        self.assertEqual(len(set(names)), 54)
        for field in ("max_quote_age_sec", "equity_budget_inr", "fno_capital_per_trade",
                      "fno_sl_pct", "fno_target_pct", "tg_api_hash", "dhan_access_token",
                      "equity_cmp_execution_policy", "csrf_token", "full_setup_form"):
            self.assertEqual(names.count(field), 1)

    def test_quote_staleness_native_limits_work_for_two(self):
        page = setup_template()
        self.assertRegex(page, re.compile(
            r'<input type="number" step="0\.5" name="max_quote_age_sec" value="<!--MAX_QUOTE_AGE_SEC-->" required>'))
        self.assertNotIn('id="replicaQuoteAge"', page)
        self.assertNotIn('min="0.1" step="0.5" data-mirror', page)

    def test_navigation_has_no_unconfigured_pages(self):
        from src.premium_ui import premium_nav
        for page in ("control", "setup", "designer"):
            nav = premium_nav(page)
            self.assertEqual(nav.count("<a href="), 3)
            for route in ('href="/"', 'href="/setup"', 'href="/setup/telegram-designer"'):
                self.assertIn(route, nav)
            for unavailable in ('href="/logs"', 'href="/reports"', 'href="/trading-orders"'):
                self.assertNotIn(unavailable, nav)

    def test_no_extra_client_side_submission_or_proxies(self):
        code = source("src/premium_ui.py")
        self.assertNotIn("approved_replica_js", code)
        self.assertNotIn("approved_replica_styles", code)
        nav = source("src/premium_ui.js")
        self.assertEqual(hashlib.sha256(nav.encode()).hexdigest(), EXPECTED_NAV_SCRIPT_SHA256)
        for mutation in ("/setup/save-apply", "/control/amo/fno", "requestSubmit", "data-mirror", "fetch("):
            self.assertNotIn(mutation, nav)

    def test_systemwide_css_not_form_modifications(self):
        style = source("src/premium_ui.css")
        for selector in (".premium-setup", ".premium-control", ".premium-designer",
                         'form[action="/setup"]', 'grid-template-columns:repeat(2,minmax(0,1fr))',
                         '@media(max-width:760px)', '#simpleSaveStatus[hidden]'):
            self.assertIn(selector, style)
        self.assertNotIn('Reconciliation: Healthy', style)
        self.assertNotIn('OCO Guard: Active', style)

    def test_control_and_designer_keep_functioning(self):
        control = source("src/control_center_ui.py")
        designer = source("src/notifier/message_designer_ui.py")
        for attr in ('id="start"', 'id="stop"', 'id="health"', 'id="equity-amo"', 'id="fno-amo"',
                     '/control/status', '/control/amo/fno', '/setup/service-control'):
            self.assertIn(attr, control)
        for attr in ('id="save"', 'id="preview"', '/setup/telegram-designer/save', 'Templates are opt-in'):
            self.assertIn(attr, designer)


if __name__ == "__main__":
    unittest.main()
