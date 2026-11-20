"""PR84 remaining acceptance matrix: synthetic, no network/systemd/broker activity.

These tests deliberately assert NEGATIVE effects for a financial trading bot:
no unexpected restart, broker GET, queue mutation or runtime verification.
"""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.engine.config_patch import (
    ConfigurationConflict, ConfigurationRejected, REGISTRY, ENV_REGISTRY,
    parse_env_strict, patch_env, revision, validate_effective_config,
)
from src.engine.config_manager import (
    ConfigApplyState, ConfigChangeRecord, ConfigChangeTracker, OperationState,
    ServiceOperatingState, write_env_atomic, write_json_atomic,
)

SYNTHETIC = (
    '# Synthetic data: no Dhan or Telegram credentials\n'
    'TRADING_MODE=LIVE\n'
    'EQUITY_ACTIVE=true\n'
    'FNO_ACTIVE=true\n'
    'DHAN_RESPONSE_GROUP_NAME=-100551111111\n'
    'DHAN_CLIENT_ID=FAKE_ID\n'
    'DHAN_PIN=FAKE_PIN\n'
    'DHAN_TOTP_SECRET=FAKE_TOTP\n'
    'TG_API_HASH=FAKE_HASH\n'
    'EQUITY_BUDGET_INR=500\n'
    'CUSTOM_PRESERVE=as-is\n'
)



class ControlCenterFnoAmoTransactionAudit(unittest.TestCase):
    def test_fno_disable_failed_or_accepted_enqueues_cannot_mutate_old_local_queue(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from src.control_center_backend import register_control_center_routes
        from src.engine.fno_amo_queue import FnoAmoQueue
        with tempfile.TemporaryDirectory() as root:
            cwd = os.getcwd()
            try:
                os.chdir(root)
                env = os.path.join(root, '.env')
                before = 'TRADING_MODE=TEST\nFNO_AMO_ENABLED=true\nDHAN_PIN=FAKE_PIN\n'
                Path(env).write_text(before)
                app = FastAPI()
                register_control_center_routes(app, 'SYNTHETIC_CSRF')
                client = TestClient(app)
                payload = {
                    'enabled': False, 'csrf_token': 'SYNTHETIC_CSRF',
                    'expected_revision': revision(before),
                    'request_id': 'FNO-CANCEL-ISOLATION-0001',
                }
                with patch.object(FnoAmoQueue, 'cancel_all_queued',
                                  side_effect=AssertionError('Queue cancellation requires deliberate action')) as cancelled, \
                     patch('src.control_center_backend.apply_settings',
                           side_effect=OSError('Injected transaction failure')):
                    failed = client.post('/control/amo/fno', json=payload)
                    self.assertIn(failed.status_code, (409, 503))
                    self.assertEqual(Path(env).read_text(), before)
                    cancelled.assert_not_called()

                with patch.object(ConfigChangeTracker, 'resolve_service_operating_state',
                                  return_value=(ServiceOperatingState.STOPPED_BY_OWNER, 'synthetic stopped')), \
                     patch.object(ConfigChangeTracker, 'get_readonly_broker',
                                  side_effect=AssertionError('No broker for stopped AMO toggle')), \
                     patch.object(FnoAmoQueue, 'cancel_all_queued',
                                  side_effect=AssertionError('Queue must remain intact')) as cancelled:
                    accepted = client.post('/control/amo/fno', json=payload)
                    self.assertEqual(accepted.status_code, 200, accepted.text)
                    self.assertEqual(accepted.json()['state'], 'SAVED_FOR_NEXT_START')
                    cancelled.assert_not_called()
                    self.assertIn('FNO_AMO_ENABLED=false', Path(env).read_text())
                    self.assertIn('DHAN_PIN=FAKE_PIN', Path(env).read_text())
                    for endpoint in ('/control/amo/equity', '/control/amo/fno'):
                        stale = client.post(endpoint, json={
                            'enabled': True, 'csrf_token': 'SYNTHETIC_CSRF',
                            'expected_revision': '0' * 64,
                        })
                        self.assertEqual(stale.status_code, 409)
                    cancelled.assert_not_called()
            finally:
                os.chdir(cwd)


if __name__ == '__main__':
    unittest.main()
