"""
Unit tests for Darvas Script features merged into AuditBot (Updated for V12 2.0s Quote Freshness Gate):
1. `looks_like_option` gatekeeper & clean separation of Equity vs F&O exit signals.
2. Company-name resolution (`normalize_name`, `build_name_index`, `resolve_company_name`).
3. Yahoo Finance Live LTP (`fetch_yahoo_ltp` / `_fetch_ltp`) with validated source quote timestamp (`<= 2.0s`).
4. Dhan Super Orders (`place_super_order`, `modify_super_order_protection`, `cancel_super_order_leg`).
5. Nearest-upcoming option expiry fallback when `expiry_month` is omitted after monthly expiry.
6. Shared Dhan token file coordinator (`src/broker/dhan_token.py`).
"""

from datetime import date
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from src.config import AppConfig
from src.models import OrderRequest, ProductType, SignalAction, SignalType, TradingMode
from src.instruments import resolver as resolver_mod
from src.instruments.resolver import (
    InstrumentResolver,
    build_name_index,
)
from src.parsers.equity_parser import (
    extract_trade,
    filter_message,
    parse_equity_regex,
)
from src.parsers.fno_parser import (
    looks_like_option,
    parse_fno_regex,
)
from src.broker.dhan_client import DhanClient
from src.broker import dhan_token
from src.engine.emergency_stop import EmergencyStopManager
from src.engine.protection_watcher import ProtectionWatcher
from src.journal.db import JournalDB
from src.main import TradingBotPipeline


DARVAS_SCRIP_ROWS = [
    ("NSE", "MPIMANIPAL", "Manipal Payment and Identity Solutions"),
    ("BSE", "MPIMANIPAL", "Manipal Payment and Identity Solutions"),
    ("NSE", "MANIPALHOS", "Manipal Health Enterprises"),
    ("BSE", "MNPLFIN", "Manipal Finance"),
    ("NSE", "M&M", "Mahindra & Mahindra"),
    ("NSE", "M&MFIN", "M&M Financial Services"),
    ("NSE", "HEGAM", "HEG Advanced Materials"),
    ("BSE", "710HEGI31", "HEGICL-7.1%-09112031-NCD"),
    ("NSE", "SBIN", "State Bank of India"),
    ("NSE", "SBILIFE", "SBI Life Insurance Company"),
    ("NSE", "SBICARD", "SBI Cards and Payment Services"),
    ("NSE", "POLYCAB", "Polycab India"),
    ("NSE", "BAJFINANCE", "Bajaj Finance"),
    ("NSE", "BAJAJFINSV", "Bajaj Finserv"),
]


class TestDarvasMergedFeatures(unittest.TestCase):
    def setUp(self):
        self._orig_valid = set(resolver_mod.VALID_EQUITY_SYMBOLS)
        self._orig_index = dict(resolver_mod._NAME_INDEX)

    def tearDown(self):
        resolver_mod.VALID_EQUITY_SYMBOLS.clear()
        resolver_mod.VALID_EQUITY_SYMBOLS.update(self._orig_valid)
        resolver_mod._NAME_INDEX.clear()
        resolver_mod._NAME_INDEX.update(self._orig_index)

    def test_looks_like_option_gatekeeper(self):
        self.assertTrue(looks_like_option("#POLYCAB 8000 PE OCT @90-105"))
        self.assertTrue(looks_like_option("INFY 1500 CALL OCT @ 32.50"))
        self.assertTrue(looks_like_option("Book profit in POLYCAB 8000 PE"))
        self.assertFalse(looks_like_option("Exit #SBIN at 810"))
        self.assertFalse(looks_like_option("Book profit in TATAMOTORS"))
        self.assertFalse(looks_like_option("Buy #SBIN @ 805"))
        self.assertFalse(looks_like_option("Close trading room for today"))

        sig, _, action = parse_fno_regex("Exit #SBIN at 810")
        self.assertIsNone(sig)
        self.assertEqual(action, SignalAction.IGNORED)

    def test_pipeline_routes_equity_exit_to_equity_not_fno(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = os.path.join(tmpdir, "data")
            os.makedirs(data_dir, exist_ok=True)
            cfg = AppConfig(
                trading_mode=TradingMode.TEST,
                equity_active=True,
                fno_active=True,
                data_dir=data_dir,
                journal_db_path=os.path.join(tmpdir, "journal.db"),
            )
            pipeline = TradingBotPipeline(cfg)
            pipeline.stop_manager.provision_initial_clean_state("TestRunner", "1113731536")

            res_eq = pipeline.process_raw_message(
                text="Exit #SBIN at 810",
                channel_id=-1001,
                message_id=501,
                msg_timestamp=1000.0,
                current_time=1002.0,
            )
            self.assertFalse(res_eq.is_valid)
            self.assertEqual(res_eq.action, SignalAction.EXIT_SIGNAL)
            self.assertEqual(res_eq.signal_type, SignalType.EQUITY)

            res_opt = pipeline.process_raw_message(
                text="Book profit in POLYCAB 8000 PE",
                channel_id=-1001,
                message_id=502,
                msg_timestamp=1000.0,
                current_time=1002.0,
            )
            self.assertFalse(res_opt.is_valid)
            self.assertEqual(res_opt.action, SignalAction.EXIT_SIGNAL)
            self.assertEqual(res_opt.signal_type, SignalType.OPTION)

    def test_company_names_resolve_to_ticker(self):
        resolver = InstrumentResolver()
        resolver.load_company_names(DARVAS_SCRIP_ROWS)

        positive_cases = [
            ("Bought\n\nMANIPAL PAYMENT", ("MPIMANIPAL", 0.0)),
            ("Bought Manipal Payment at 450", ("MPIMANIPAL", 450.0)),
            ("Added more Manipal Payment and Identity", ("MPIMANIPAL", 0.0)),
            ("Buy MANIPAL PAYMENT & IDENTITY SOLUTIONS @ 455", ("MPIMANIPAL", 455.0)),
            ("#MANIPAL PAYMENT buy", ("MPIMANIPAL", 0.0)),
            ("Bought Manipal Health", ("MANIPALHOS", 0.0)),
            ("Buy State Bank of India @ 810", ("SBIN", 810.0)),
            ("Bought Bajaj Finance at 7000", ("BAJFINANCE", 7000.0)),
            ("Bought HEG at 500", ("HEGAM", 500.0)),
            ("Buy #SBIN @ 805-810", ("SBIN", 805.0)),
            ("Bought in POLYCAB @ 7000", ("POLYCAB", 7000.0)),
            ("Added more #SBIN", ("SBIN", 0.0)),
            ("Bought Polycab India", ("POLYCAB", 0.0)),
        ]
        for text, expected in positive_cases:
            self.assertIsNone(filter_message(text), f"Filter blocked valid message: {text}")
            sig, reason = parse_equity_regex(text)
            self.assertIsNotNone(sig, f"Failed to parse company name in: {text} ({reason})")
            self.assertEqual((sig.symbol, sig.stated_price), expected, f"Mismatch on: {text}")
            trade_dict = extract_trade(text)
            self.assertIsNotNone(trade_dict)
            self.assertEqual((trade_dict["stock_symbol"], trade_dict["entry_price"]), expected)

    def test_ambiguous_or_weak_company_names_rejected(self):
        resolver = InstrumentResolver()
        resolver.load_company_names(DARVAS_SCRIP_ROWS)

        ambiguous_cases = [
            "Bought MANIPAL",
            "Bought SBI",
            "Bought Bajaj",
            "#M&M buy at 3000",
            "Bought M and M",
            "Bought something nice today",
            "Bought some at 500",
        ]
        for text in ambiguous_cases:
            sig, _ = parse_equity_regex(text)
            self.assertIsNone(sig, f"Should have rejected ambiguous/weak name: {text}")

    def test_debt_ncd_rows_excluded_from_name_index(self):
        idx = build_name_index(DARVAS_SCRIP_ROWS)
        all_tickers = [t for entries in idx.values() for _, t in entries]
        self.assertNotIn("710HEGI31", all_tickers)

    def test_resolver_resolve_equity_by_company_name(self):
        resolver = InstrumentResolver()
        resolver.load_equity(
            "MPIMANIPAL",
            security_id="99112",
            exchange_segment="NSE_EQ",
            company_name="Manipal Payment and Identity Solutions",
        )
        inst, _ = resolver.resolve_equity("Manipal Payment")
        self.assertIsNotNone(inst)
        self.assertEqual(inst.symbol, "MPIMANIPAL")
        self.assertEqual(inst.security_id, "99112")

    @patch("requests.Session.get")
    def test_yahoo_finance_live_ltp_direct(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "regularMarketPrice": 812.45,
                            "regularMarketTime": 1730200000.0,
                            "symbol": "SBIN.NS",
                            "exchangeName": "NSI",
                            "currency": "INR",
                            "marketState": "REGULAR",
                            "exchangeDataDelayedBy": 0,
                        }
                    }
                ]
            }
        }
        mock_get.return_value = mock_resp

        cfg = AppConfig.from_env()
        client = DhanClient(cfg)

        ok, price, msg = client.fetch_yahoo_ltp("SBIN", "NSE_EQ", now_utc=1730200001.5)
        self.assertTrue(ok)
        self.assertEqual(price, 812.45)
        self.assertIn("SBIN.NS", msg)
        self.assertEqual(client._fetch_ltp("SBIN", now_utc=1730200001.5), 812.45)

    @patch("requests.Session.get")
    def test_get_ltp_uses_yahoo_primary_with_fresh_timestamp(self, mock_get):
        yf_resp = MagicMock()
        yf_resp.status_code = 200
        yf_resp.json.return_value = {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "regularMarketPrice": 2495.50,
                            "regularMarketTime": 1730200000.0,
                            "symbol": "RELIANCE.NS",
                            "exchangeName": "NSI",
                            "currency": "INR",
                            "marketState": "REGULAR",
                            "exchangeDataDelayedBy": 0,
                        }
                    }
                ]
            }
        }
        mock_get.return_value = yf_resp

        cfg = AppConfig.from_env()
        client = DhanClient(cfg)

        ok, ltp, msg = client.get_ltp("NSE_EQ", "2885", symbol="RELIANCE", now_utc=1730200001.0)
        self.assertTrue(ok)
        self.assertEqual(ltp, 2495.50)
        self.assertIn("Yahoo Finance", msg)

    @patch("requests.Session.put")
    @patch("requests.Session.post")
    def test_dhan_super_order_place_and_reanchor_on_fill(self, mock_post, mock_put):
        with tempfile.TemporaryDirectory() as tmpdir:
            stop_file = os.path.join(tmpdir, "stop.json")
            stop_mgr = EmergencyStopManager(stop_file, fail_closed_if_missing=True)
            stop_mgr.provision_initial_clean_state("Udit", "1113731536")

            cfg = AppConfig.from_env()
            cfg.dhan_client_id = "1113731536"
            cfg.dhan_access_token = "mock_token"
            cfg.trading_mode = TradingMode.LIVE

            client = DhanClient(cfg, stop_manager=stop_mgr)

            post_resp = MagicMock()
            post_resp.status_code = 200
            post_resp.text = '{"orderId": "SUPER-9901", "orderStatus": "PENDING"}'
            post_resp.json.return_value = {"orderId": "SUPER-9901", "orderStatus": "PENDING"}
            mock_post.return_value = post_resp

            req = OrderRequest(
                correlation_id="OPT-POLYCAB-1",
                symbol="POLYCAB-29OCT26-8000-PE",
                security_id="54231",
                exchange_segment="NSE_FNO",
                product_type=ProductType.MARGIN,
                price=100.0,
                quantity=100,
                trading_mode=TradingMode.LIVE,
            )
            ok, oid, _ = client.place_super_order(req, stop_loss_price=97.0, target_price=103.0)
            self.assertTrue(ok)
            self.assertEqual(oid, "SUPER-9901")

            put_resp = MagicMock()
            put_resp.status_code = 200
            put_resp.text = '{"orderId": "SUPER-9901", "orderStatus": "TRADED"}'
            put_resp.json.return_value = {"orderId": "SUPER-9901", "orderStatus": "TRADED"}
            mock_put.return_value = put_resp

            db = JournalDB(os.path.join(tmpdir, "journal.db"))
            watcher = ProtectionWatcher(
                broker=client,
                sl_pct=3.0,
                target_pct=3.0,
                path=os.path.join(tmpdir, "pending.json"),
                dry_run=False,
                clock=lambda: 1000.0,
                journal_db=db,
            )
            watcher.track(
                order_id="SUPER-9901",
                symbol="POLYCAB",
                security_id="54231",
                segment="NSE_FNO",
                qty=100,
                product_type="MARGIN",
                is_super_order=True,
            )
            super_rec = {
                "orderId": "SUPER-9901",
                "dhanClientId": "1113731536",
                "securityId": "54231",
                "exchangeSegment": "NSE_FNO",
                "productType": "MARGIN",
                "orderStatus": "TRADED",
                "filledQty": 100,
                "averageTradedPrice": 100.0,
                "legDetails": [
                    {"legName": "STOP_LOSS_LEG", "transactionType": "SELL", "orderStatus": "PENDING", "remainingQuantity": 100, "price": 97.0},
                    {"legName": "TARGET_LEG", "transactionType": "SELL", "orderStatus": "PENDING", "remainingQuantity": 100, "price": 103.0},
                ],
            }
            client.get_super_orders = MagicMock(return_value=[super_rec])
            client.order_status = MagicMock(return_value=super_rec)
            client.trades_avg_price = MagicMock(return_value=100.0)

            watcher.check_once()
            self.assertEqual(mock_put.call_count, 2)
            self.assertNotIn("SUPER-9901", watcher.pending)

    def test_nearest_upcoming_expiry_fallback_when_month_omitted(self):
        resolver = InstrumentResolver()
        resolver.load_option("POLYCAB", "54231", 8000.0, "PE", date(2026, 10, 29), 100)
        resolver.load_option("POLYCAB", "64231", 8000.0, "PE", date(2026, 11, 26), 100)
        contract, _ = resolver.resolve_option(
            symbol="POLYCAB",
            strike=8000.0,
            option_type="PE",
            expiry_month=None,
            reference_date=date(2026, 10, 30),
        )
        self.assertIsNotNone(contract)
        self.assertEqual(contract.security_id, "64231")
        self.assertEqual(contract.expiry, date(2026, 11, 26))

    def test_shared_dhan_token_adopts_existing_before_login(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tok_file = os.path.join(tmpdir, ".dhan_token")
            dhan_token.write_token_atomic("TOKEN_FROM_OTHER_BOT", tok_file)

            login_calls = []
            def fake_login():
                login_calls.append(1)
                return "NEW_LOGIN_TOKEN"

            adopted = dhan_token.fresh_token("OLD_TOKEN", fake_login, path=tok_file)
            self.assertEqual(adopted, "TOKEN_FROM_OTHER_BOT")
            self.assertEqual(len(login_calls), 0)

            refreshed = dhan_token.fresh_token("TOKEN_FROM_OTHER_BOT", fake_login, path=tok_file)
            self.assertEqual(refreshed, "NEW_LOGIN_TOKEN")
            self.assertEqual(len(login_calls), 1)
            self.assertEqual(dhan_token.read_token(tok_file), "NEW_LOGIN_TOKEN")


if __name__ == "__main__":
    unittest.main()
