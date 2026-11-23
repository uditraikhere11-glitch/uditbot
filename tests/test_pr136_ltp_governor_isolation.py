"""PR136: offline regression tests for quote policy, cooldown, and explicit isolation."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import os
import sqlite3
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src.broker.dhan_client import DhanClient
from src.broker.read_governor import ReadGovernor
from src.engine.config_manager import is_isolated_test_environment


class LtpExplicitPolicyRetryTests(unittest.TestCase):
    def _exercise(self, status: int, policy: str, default: str) -> None:
        # __new__ avoids initializing any broker sessions, tokens or home files.
        broker = DhanClient.__new__(DhanClient)
        broker.token = "EXPIRED_TEST_TOKEN"
        broker.cfg = SimpleNamespace(
            dhan_pin="TEST_PIN", dhan_totp_secret="TEST_SECRET",
            equity_cmp_execution_policy=default, max_quote_age_sec=10.0,
        )
        broker._headers = lambda: {"access-token": broker.token}
        unauthorized = SimpleNamespace(status_code=status, text="expired test token")
        successful = SimpleNamespace(
            status_code=200, text="",
            json=lambda: {"data": {"NSE_FNO": {"123": {"last_price": 101.25}}}},
        )
        broker._governed_quote_post = Mock(side_effect=[unauthorized, successful])
        broker._requires_token_refresh = Mock(return_value=True)

        def refresh(**kwargs):
            self.assertEqual(kwargs["rejected_token"], "EXPIRED_TEST_TOKEN")
            broker.token = "ROTATED_TEST_TOKEN"
            return True, "ROTATED_TEST_TOKEN", "verified"

        broker.generate_token_with_totp = Mock(side_effect=refresh)
        with patch.object(broker, "get_ltp", wraps=broker.get_ltp) as intercepted:
            result = intercepted(
                "NSE_FNO", "123", retry=True, now_utc=1750000000.0,
                max_quote_age_sec=10.0, cmp_policy=policy,
            )
            self.assertEqual(intercepted.call_count, 2)
            self.assertTrue(result[0], result)
            self.assertEqual(result[1], 101.25)
            retry = intercepted.call_args_list[1]
            self.assertIs(retry.kwargs["retry"], False)
            self.assertEqual(retry.kwargs["cmp_policy"], policy)
            self.assertEqual(retry.kwargs["max_quote_age_sec"], 10.0)
            self.assertEqual(retry.kwargs["now_utc"], 1750000000.0)
            self.assertEqual(retry.args, ("NSE_FNO", "123"))
        broker.generate_token_with_totp.assert_called_once()

    def test_401_darvas_policy_preserved(self):
        self._exercise(401, "DARVAS_HTTP_SUCCESS", "STRICT_TIMESTAMP")

    def test_403_darvas_policy_preserved(self):
        self._exercise(403, "DARVAS_HTTP_SUCCESS", "STRICT_TIMESTAMP")

    def test_401_nondefault_strict_policy_preserved(self):
        self._exercise(401, "STRICT_TIMESTAMP", "DARVAS_HTTP_SUCCESS")

    def test_403_nondefault_strict_policy_preserved(self):
        self._exercise(403, "STRICT_TIMESTAMP", "DARVAS_HTTP_SUCCESS")


class OneStrikePerCooldownTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = [10000.0]
        self.governor = self._new_governor()

    def _new_governor(self):
        return ReadGovernor(
            self.tmp.name, "SYNTHETIC_ACCOUNT",
            clock=lambda: self.now[0],
        )

    def _state(self):
        with sqlite3.connect(self.governor.path) as conn:
            return conn.execute(
                "SELECT until, strikes FROM category_cooldown WHERE category=?",
                ("NON_TRADING",),
            ).fetchone()

    def test_four_concurrent_429s_generate_only_one_strike(self):
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(
                lambda _: self._new_governor().throttled(None),
                range(4),
            ))
        self.assertEqual(len(results), 4)
        until, strikes = self._state()
        self.assertEqual(strikes, 1)
        self.assertAlmostEqual(until, 10060.0)
        self.assertEqual(results, [60.0] * 4)

    def test_long_retry_after_extends_active_window_without_strike(self):
        self.governor.throttled(None)
        self.now[0] += 5.0
        self.governor.throttled("180")
        until, strikes = self._state()
        self.assertEqual(strikes, 1)
        self.assertEqual(until, 10185.0)
        self.governor.throttled("15")
        self.assertEqual(self._state(), (10185.0, 1))

    def test_expired_window_escalates_and_success_resets(self):
        self.governor.throttled(None)
        self.now[0] = 10060.0  # exact expiry must count as a new window
        self.governor.throttled(None)
        self.assertEqual(self._state(), (10180.0, 2))
        self.now[0] = 10180.0
        self.governor.record_success()
        self.assertEqual(self._state(), (10180.0, 0))
        self.governor.throttled(None)
        self.assertEqual(self._state(), (10240.0, 1))

    def test_repeat_429_never_shortens_server_cooldown(self):
        self.governor.throttled("240")
        self.now[0] += 10
        self.governor.throttled(None)
        self.assertEqual(self._state(), (10240.0, 1))


class ExplicitIsolationOnlyTests(unittest.TestCase):
    def test_imported_unittest_does_not_switch_to_isolation(self):
        # This module necessarily imports unittest; only the explicit flag matters.
        instance = DhanClient.__new__(DhanClient)
        home_token = os.path.expanduser("~/.dhan_token")
        with patch.dict(os.environ, {"AUDITBOT_TEST_ISOLATION": "0"}):
            self.assertFalse(is_isolated_test_environment())
            self.assertFalse(instance._is_isolated_test_default_token_path(home_token))
        with patch.dict(os.environ, {"AUDITBOT_TEST_ISOLATION": "1"}):
            self.assertTrue(is_isolated_test_environment())
            self.assertTrue(instance._is_isolated_test_default_token_path(home_token))
            self.assertFalse(instance._is_isolated_test_default_token_path("/tmp/synthetic_token"))

    def test_missing_isolation_flag_is_production_mode(self):
        instance = DhanClient.__new__(DhanClient)
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(is_isolated_test_environment())
            self.assertFalse(instance._is_isolated_test_default_token_path(
                os.path.expanduser("~/.dhan_token")
            ))

    def test_no_implicit_test_modules_checks_in_python_production_code(self):
        import re
        for path in Path("src").rglob("*.py"):
            source = path.read_text(encoding="utf-8")
            self.assertFalse(
                re.search(
                    r"""(?:["'](?:unittest|pytest)["']\s+in\s+(?:os\.)?sys\.modules)""",
                    source,
                ),
                path.as_posix(),
            )
        # The tracked historical .bak was also scrubbed to keep source audits clean.
        archived = Path("src/broker/dhan_client.py.bak")
        self.assertNotIn('"unittest" in sys.modules', archived.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
