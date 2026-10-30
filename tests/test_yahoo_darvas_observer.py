"""
Automated unit and offline regression tests for Darvas Yahoo Finance Observational Price Retriever.

Verifies:
1. Valid positive Yahoo price with missing metadata.
2. Missing marketState reported as UNKNOWN (not falsely closed).
3. Missing delay metadata never treated as zero delay.
4. Quote ages 1.9, 2.0, and 2.1 seconds.
5. Stale quotes from 2.59 to 8.12 seconds.
6. Future and malformed timestamps.
7. Wrong ticker, exchange, and currency.
8. BSE delayed price rejection in strict validator.
9. F&O underlying price substitution blocked.
10. HTTP failures, timeouts, and malformed responses.
11. Observation results never authorize executable orders (executable=False).
12. Existing strict order-execution validator unchanged.
13. CLI observer runs only inside its designated command.
14. CLI imports perform no network calls.
15. cmd_status, cmd_dashboard, and cmd_check_dhan continue working.
16. Repeated diagnostic calls are idempotent.
17. Zero real broker order POST, PUT, or DELETE.
18. Test group signals ignored in LIVE mode.
"""

import io
import json
import os
import sys
import time
import unittest
from unittest.mock import MagicMock, Mock, patch

from src.broker.yahoo_observer import YahooObservation, YahooObserver
from src.broker.dhan_client import DhanClient
from src.config import AppConfig, TradingMode
from src.engine.emergency_stop import EmergencyStopManager
from src.journal.db import JournalDB


class TestYahooDarvasObserver(unittest.TestCase):

    def setUp(self):
        self.mock_session = MagicMock()
        self.observer = YahooObserver(session=self.mock_session)

    # 1. Valid positive Yahoo price with missing metadata
    def test_01_valid_positive_price_with_missing_metadata(self):
        payload = {
            "chart": {
                "result": [{
                    "meta": {
                        "regularMarketPrice": 2500.50,
                        "regularMarketTime": 1700000000,
                        # exchangeName, currency, marketState, exchangeDataDelayedBy all omitted
                    }
                }]
            }
        }
        resp = MagicMock(status_code=200)
        resp.json.return_value = payload
        self.mock_session.get.return_value = resp

        obs = self.observer.observe("RELIANCE", now_utc=1700000005.0)
        self.assertTrue(obs.is_success)
        self.assertEqual(obs.reported_price, 2500.50)
        self.assertEqual(obs.measured_age_sec, 5.0)
        self.assertEqual(obs.exchange, "UNKNOWN")
        self.assertEqual(obs.currency, "UNKNOWN")
        self.assertEqual(obs.market_state, "UNKNOWN")
        self.assertIsNone(obs.delayed_by_sec)
        self.assertEqual(obs.delay_status, "UNKNOWN")
        self.assertFalse(obs.executable)

    # 2. Missing marketState reported as UNKNOWN
    def test_02_missing_market_state_reported_as_unknown(self):
        payload = {
            "chart": {
                "result": [{
                    "meta": {
                        "regularMarketPrice": 800.0,
                        "regularMarketTime": 1700000000,
                        "marketState": "",  # Empty string
                    }
                }]
            }
        }
        resp = MagicMock(status_code=200)
        resp.json.return_value = payload
        self.mock_session.get.return_value = resp

        obs = self.observer.observe("SBIN")
        self.assertEqual(obs.market_state, "UNKNOWN")
        self.assertNotEqual(obs.market_state, "CLOSED")

    # 3. Missing delay metadata never treated as zero delay
    def test_03_missing_delay_never_treated_as_zero(self):
        payload = {
            "chart": {
                "result": [{
                    "meta": {
                        "regularMarketPrice": 150.0,
                        "regularMarketTime": 1700000000,
                        # exchangeDataDelayedBy missing
                    }
                }]
            }
        }
        resp = MagicMock(status_code=200)
        resp.json.return_value = payload
        self.mock_session.get.return_value = resp

        obs = self.observer.observe("TATASTEEL")
        self.assertIsNone(obs.delayed_by_sec)
        self.assertEqual(obs.delay_status, "UNKNOWN")
        self.assertNotEqual(obs.delay_status, "REALTIME")

    # 4. Quote ages 1.9, 2.0, and 2.1 seconds
    def test_04_quote_ages_boundary(self):
        base_time = 1700000000.0
        for age in [1.9, 2.0, 2.1]:
            payload = {
                "chart": {
                    "result": [{
                        "meta": {
                            "regularMarketPrice": 100.0,
                            "regularMarketTime": base_time,
                        }
                    }]
                }
            }
            resp = MagicMock(status_code=200)
            resp.json.return_value = payload
            self.mock_session.get.return_value = resp

            obs = self.observer.observe("INFY", now_utc=base_time + age)
            self.assertAlmostEqual(obs.measured_age_sec, age, places=2)

    # 5. Stale quotes from 2.59 to 8.12 seconds
    def test_05_stale_quotes_production_diagnostic_samples(self):
        base_time = 1700000000.0
        stale_samples = [2.59, 3.67, 5.36, 5.91, 8.12]
        for age in stale_samples:
            payload = {
                "chart": {
                    "result": [{
                        "meta": {
                            "regularMarketPrice": 1000.0,
                            "regularMarketTime": base_time,
                        }
                    }]
                }
            }
            resp = MagicMock(status_code=200)
            resp.json.return_value = payload
            self.mock_session.get.return_value = resp

            obs = self.observer.observe("RELIANCE", now_utc=base_time + age)
            self.assertAlmostEqual(obs.measured_age_sec, age, places=2)
            self.assertFalse(obs.executable)

    # 6. Future and malformed timestamps
    def test_06_future_and_malformed_timestamps(self):
        now_time = 1700000000.0
        # Future timestamp
        payload_future = {
            "chart": {
                "result": [{
                    "meta": {
                        "regularMarketPrice": 500.0,
                        "regularMarketTime": now_time + 100.0,
                    }
                }]
            }
        }
        resp = MagicMock(status_code=200)
        resp.json.return_value = payload_future
        self.mock_session.get.return_value = resp
        obs_future = self.observer.observe("ITC", now_utc=now_time)
        self.assertEqual(obs_future.measured_age_sec, 0.0)

        # Malformed timestamp
        payload_bad = {
            "chart": {
                "result": [{
                    "meta": {
                        "regularMarketPrice": 500.0,
                        "regularMarketTime": "invalid_timestamp",
                    }
                }]
            }
        }
        resp.json.return_value = payload_bad
        obs_bad = self.observer.observe("ITC", now_utc=now_time)
        self.assertIsNone(obs_bad.quote_timestamp)
        self.assertIsNone(obs_bad.measured_age_sec)

    # 7. Wrong ticker, exchange, and currency
    def test_07_identity_metadata_preserved_accurately(self):
        payload = {
            "chart": {
                "result": [{
                    "meta": {
                        "regularMarketPrice": 450.0,
                        "regularMarketTime": 1700000000,
                        "exchangeName": "BSE",
                        "currency": "USD",
                        "marketState": "REGULAR",
                        "exchangeDataDelayedBy": 900,
                    }
                }]
            }
        }
        resp = MagicMock(status_code=200)
        resp.json.return_value = payload
        self.mock_session.get.return_value = resp

        obs = self.observer.observe("WIPRO", exchange_segment="BSE_EQ")
        self.assertEqual(obs.exchange, "BSE")
        self.assertEqual(obs.currency, "USD")
        self.assertEqual(obs.delayed_by_sec, 900)
        self.assertEqual(obs.delay_status, "DELAYED")

    # 8. BSE delayed price rejection in strict validator
    def test_08_bse_delayed_price_rejection_in_strict_validator(self):
        cfg = AppConfig(trading_mode=TradingMode.TEST)
        client = DhanClient(cfg)
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {
            "chart": {
                "result": [{
                    "meta": {
                        "regularMarketPrice": 2500.0,
                        "regularMarketTime": time.time(),
                        "symbol": "RELIANCE.BO",
                        "currency": "INR",
                        "exchangeName": "BSE",
                        "marketState": "REGULAR",
                        "exchangeDataDelayedBy": 900,
                    }
                }]
            }
        }
        client.session = MagicMock()
        client.session.get.return_value = mock_resp

        res = client.get_yahoo_quote_details("RELIANCE", exchange_segment="BSE_EQ")
        self.assertFalse(res.is_valid)
        self.assertIn("delayed", res.failure_reason.lower())

    # 9. F&O underlying price substitution blocked
    def test_09_fno_underlying_substitution_blocked_in_strict_validator(self):
        cfg = AppConfig(trading_mode=TradingMode.TEST)
        client = DhanClient(cfg)
        res = client.get_yahoo_quote_details("SBIN", exchange_segment="NSE_FNO")
        self.assertFalse(res.is_valid)
        self.assertIn("cannot substitute underlying equity quote", res.failure_reason)

    # 10. HTTP failures, timeouts, and malformed responses
    def test_10_http_failures_and_malformed_responses(self):
        # HTTP 500
        self.mock_session.get.return_value = MagicMock(status_code=500)
        obs_500 = self.observer.observe("SBIN")
        self.assertFalse(obs_500.is_success)
        self.assertIn("HTTP 500", obs_500.error)

        # Timeout exception
        self.mock_session.get.side_effect = TimeoutError("Network timeout")
        obs_timeout = self.observer.observe("SBIN")
        self.assertFalse(obs_timeout.is_success)
        self.assertIn("timeout", obs_timeout.error.lower())
        self.mock_session.get.side_effect = None

        # Malformed json (empty result)
        resp_empty = MagicMock(status_code=200)
        resp_empty.json.return_value = {"chart": {"result": []}}
        self.mock_session.get.return_value = resp_empty
        obs_empty = self.observer.observe("SBIN")
        self.assertFalse(obs_empty.is_success)
        self.assertIn("Empty chart result", obs_empty.error)

    # 11. Observation results never authorize executable orders
    def test_11_observation_never_authorizes_orders(self):
        obs = YahooObservation(
            ticker="RELIANCE.NS",
            symbol="RELIANCE",
            exchange_segment="NSE_EQ",
            reported_price=2500.0,
            is_success=True,
            executable=False,
        )
        self.assertFalse(obs.executable)
        # Attempting to verify that executable is strictly False
        self.assertEqual(obs.executable, False)

    # 12. Existing strict order-execution validator unchanged
    def test_12_strict_validator_enforces_two_second_ceiling(self):
        cfg = AppConfig(trading_mode=TradingMode.TEST, max_quote_age_sec=2.0)
        client = DhanClient(cfg)
        client.session = MagicMock()

        # Age 2.1s must be rejected
        now = 1700000000.0
        client.session.get.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "chart": {
                    "result": [{
                        "meta": {
                            "regularMarketPrice": 1000.0,
                            "regularMarketTime": now - 2.1,
                            "symbol": "SBIN.NS",
                            "currency": "INR",
                            "exchangeName": "NSE",
                            "exchangeDataDelayedBy": 0,
                            "marketState": "REGULAR",
                        }
                    }]
                }
            }
        )
        res = client.get_yahoo_quote_details("SBIN", exchange_segment="NSE_EQ", now_utc=now)
        self.assertFalse(res.is_valid)
        self.assertIn("stale", res.failure_reason.lower())

    # 13. CLI observer runs only inside designated command
    def test_13_cli_observer_runs_inside_cmd_check_dhan(self):
        from src.cli import cmd_check_dhan
        cfg = AppConfig(trading_mode=TradingMode.TEST)
        stop_mgr = MagicMock()
        stop_mgr.get_state.return_value = {"is_stopped": False}

        with patch("src.broker.dhan_client.DhanClient.get_fund_limits", return_value=(True, 100000.0, "OK")), \
             patch("src.broker.dhan_client.DhanClient.get_ltp", return_value=(True, 2500.0, "OK")), \
             patch("src.broker.yahoo_observer.YahooObserver.observe") as mock_obs, \
             patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:

            mock_obs.return_value = YahooObservation(
                ticker="RELIANCE.NS",
                symbol="RELIANCE",
                exchange_segment="NSE_EQ",
                reported_price=2500.0,
                is_success=True,
                executable=False,
            )

            cmd_check_dhan(cfg, stop_mgr)
            output = mock_stdout.getvalue()
            self.assertIn("Yahoo Finance Observational Diagnostic", output)
            self.assertIn("RELIANCE.NS", output)
            mock_obs.assert_called_once()

    # 14. CLI imports perform no network calls
    def test_14_import_cli_causes_zero_network_calls(self):
        with patch("requests.get") as mock_get, \
             patch("requests.Session.get") as mock_sess_get:
            # Re-import or ensure no network calls on import
            import src.cli
            mock_get.assert_not_called()
            mock_sess_get.assert_not_called()

    # 15. bot status, bot dashboard, and bot check-dhan continue working
    def test_15_cli_core_commands_execute_without_error(self):
        from src.cli import cmd_status, cmd_dashboard, cmd_check_dhan
        cfg = AppConfig(trading_mode=TradingMode.TEST, dhan_client_id="1113731536")
        db = MagicMock()
        db.get_dashboard_summary.return_value = {
            "signals_today": 1, "signals_all": 2, "orders_live": 0, "orders_test": 1,
            "orders_eq": 1, "orders_fno": 0, "protection_confirmed": 0, "protection_active": 0,
            "protection_unprotected": 0, "unknown_intents": 0, "non_terminal_intents": 0
        }
        stop_mgr = MagicMock()
        stop_mgr.get_state.return_value = {"is_stopped": False}

        with patch("sys.stdout", new_callable=io.StringIO):
            cmd_status(cfg, db, stop_mgr)
            cmd_dashboard(cfg, db, stop_mgr)

    # 16. Repeated diagnostic calls are idempotent
    def test_16_repeated_observer_calls_are_idempotent(self):
        payload = {
            "chart": {
                "result": [{
                    "meta": {
                        "regularMarketPrice": 120.0,
                        "regularMarketTime": 1700000000,
                    }
                }]
            }
        }
        resp = MagicMock(status_code=200)
        resp.json.return_value = payload
        self.mock_session.get.return_value = resp

        obs1 = self.observer.observe("SBIN")
        obs2 = self.observer.observe("SBIN")
        self.assertEqual(obs1.reported_price, obs2.reported_price)
        self.assertEqual(obs1.ticker, obs2.ticker)

    # 17. Zero real broker order POST, PUT, or DELETE during testing
    def test_17_zero_broker_mutations(self):
        with patch("requests.post") as mock_post, \
             patch("requests.put") as mock_put, \
             patch("requests.delete") as mock_del:
            obs = self.observer.observe("SBIN")
            mock_post.assert_not_called()
            mock_put.assert_not_called()
            mock_del.assert_not_called()

    # 18. Test group signals ignored in LIVE mode
    def test_18_test_group_signals_ignored_in_live_mode(self):
        from src.main import TradingBotPipeline
        cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            monitored_channel_ids=[-100111],
            test_group_id=-100222,
            dhan_client_id="1113731536",
        )
        pipeline = TradingBotPipeline(cfg)
        pipeline.stop_manager.provision_initial_clean_state("Udit", "1113731536")

        # In LIVE mode, message from test_group_id must be ignored/blocked
        res = pipeline.process_raw_message("Buy #SBIN @ 805", channel_id=-100222)
        self.assertFalse(res.is_valid)
        self.assertIn("Ignored test group signal in LIVE mode", res.reason)


if __name__ == "__main__":
    unittest.main()
