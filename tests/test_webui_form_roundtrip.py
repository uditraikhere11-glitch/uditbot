"""Form-level AC-01 isolation regression for whole Setup HTML form.

Uses only fake .env and offline TestClient. A notification-only change must not
truncate fractional numeric settings or rewrite unrelated secrets/defaults.
"""
from contextlib import contextmanager
from html import unescape
import os
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from src.setup_app import app
from src.engine.config_manager import ConfigChangeTracker, ServiceOperatingState
from src.engine.config_patch import revision


ENV = (
    "TRADING_MODE=TEST\n"
    "TG_API_ID=0\n"
    "LOG_GROUP_ID=0\n"
    "TEST_GROUP_ID=0\n"
    "EQUITY_BUDGET_INR=125000.5\n"
    "MIN_TRADE_FUNDS_INR=123456.75\n"
    "FNO_CAPITAL_PER_TRADE=19500.25\n"
    "MAX_ORDER_AMOUNT_INR=50000.75\n"
    "TEST_MODE_PAPER_FUNDS_INR=100000.125\n"
    "MAX_DAILY_LOSS_INR=15000.9\n"
    "MAX_TOTAL_EXPOSURE_INR=150000.025\n"
    "MAX_MESSAGE_AGE_SEC=60.25\n"
    "DEDUP_WINDOW_SEC=20.5\n"
    "ORDER_TIMEOUT_SEC=60.125\n"
    "SCRIP_MASTER_MAX_AGE_SEC=86400.125\n"
    "DHAN_PIN=FAKE_PIN\n"
    "TG_API_HASH=FAKE_HASH\n"
    "DHAN_TOTP_SECRET=FAKE_TOTP\n"
    "CUSTOM_SETTING=exactly-preserved\n"
)


def form_fields(rendered: str) -> dict:
    """Mimic submitting all successful native form controls, including selects."""
    body = rendered.split('<form method="POST" action="/setup">', 1)[1].split('</form>', 1)[0]
    fields = {}
    for tag in re.findall(r'<input\b[^>]*>', body):
        attrs = dict(re.findall(r'([\w-]+)="([^"]*)"', tag))
        name = attrs.get('name')
        if not name or attrs.get('type') in ('button', 'submit', 'reset', 'file'):
            continue
        if attrs.get('type') == 'checkbox' and not re.search(r'\bchecked(?:\s|>)', tag):
            continue
        fields[name] = unescape(attrs.get('value', 'on' if attrs.get('type') == 'checkbox' else ''))
    for m in re.finditer(r'<select\b([^>]*)>(.*?)</select>', body, re.DOTALL):
        attrs = dict(re.findall(r'([\w-]+)="([^"]*)"', m.group(1)))
        if not attrs.get('name'):
            continue
        opts = re.findall(r'<option\b([^>]*)>', m.group(2))
        chosen = next((o for o in opts if re.search(r'\bselected\b', o)), opts[0] if opts else '')
        v = re.search(r'\bvalue="([^"]*)"', chosen)
        if v:
            fields[attrs['name']] = unescape(v.group(1))
    return fields


class SetupFormRoundTripRegression(unittest.TestCase):
    def test_ac01_full_rendered_form_only_changes_response_group(self):
        old_cwd = os.getcwd()
        original_env = dict(os.environ)
        with tempfile.TemporaryDirectory() as root:
            try:
                os.chdir(root)
                p = Path(root) / '.env'
                p.write_text(ENV, encoding='utf-8')
                with patch.object(ConfigChangeTracker, 'resolve_service_operating_state',
                                  return_value=(ServiceOperatingState.STOPPED_BY_OWNER, 'synthetic')), \
                     patch.object(ConfigChangeTracker, 'get_readonly_broker',
                                  side_effect=AssertionError('Notification update cannot consult Dhan')), \
                     patch.object(ConfigChangeTracker, 'start_coordinator_daemon',
                                  side_effect=AssertionError('No restart/coordinator')):
                    client = TestClient(app)
                    view = client.get('/')
                    self.assertEqual(view.status_code, 200)
                    form = form_fields(view.text)
                    self.assertEqual(form.get('expected_revision'), revision(ENV))
                    self.assertEqual(form.get('full_setup_form'), '1')
                    self.assertEqual(form.get('tg_api_id'), '0')
                    self.assertEqual(form.get('log_group_id'), '0')
                    self.assertEqual(form.get('test_group_id'), '0')
                    for field, display in (
                        ('equity_budget_inr', '125000.5'),
                        ('min_trade_funds_inr', '123456.75'),
                        ('fno_capital_per_trade', '19500.25'),
                        ('max_order_amount_inr', '50000.75'),
                        ('test_mode_paper_funds_inr', '100000.125'),
                        ('max_daily_loss_inr', '15000.9'),
                        ('max_total_exposure_inr', '150000.025'),
                        ('max_message_age_sec', '60.25'),
                        ('dedup_window_sec', '20.5'),
                        ('order_timeout_sec', '60.125'),
                        ('scrip_master_max_age_sec', '86400.125'),
                    ):
                        with self.subTest(field=field):
                            self.assertEqual(form.get(field), display)
                            tag = next((t for t in re.findall(r'<input\b[^>]*>', view.text)
                                        if ('name="' + field + '"') in t), '')
                            self.assertIn('step="any"', tag)
                    form['dhan_response_group_name'] = '-5514889030'
                    result = client.post('/setup/submit-async', data=form)
                    self.assertEqual(result.status_code, 200, result.text)
                    obj = result.json()
                    self.assertEqual(obj.get('changed_fields'), ['DHAN_RESPONSE_GROUP_NAME'])
                    self.assertFalse(obj.get('runtime_verified'))
                    self.assertFalse(obj.get('restart_performed'))
                    self.assertEqual(p.read_text(encoding='utf-8'), ENV + 'DHAN_RESPONSE_GROUP_NAME=-5514889030\n')
            finally:
                os.chdir(old_cwd)
                os.environ.clear()
                os.environ.update(original_env)


if __name__ == '__main__':
    unittest.main()
