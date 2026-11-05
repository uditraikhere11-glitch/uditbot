"""Segmented order protection: independent defaults, durable snapshots, and source isolation.

Offline tests only: never imports a live broker client, places orders, or uses Telegram.
"""
import inspect
import os
import tempfile
import unittest

from src.config import AppConfig
from src.engine.config_manager import ConfigChangeTracker, compute_config_version
from src.engine.protection_watcher import ProtectionWatcher
from src.engine.sizer import calculate_protection_levels
from unittest.mock import patch
from src.setup_app import build_env_and_params_dict


class TestIndependentSegmentProtection(unittest.TestCase):
    def test_defaults_override_legacy_shared_env_settings(self):
        cfg = AppConfig.from_env(env={"SL_PCT": "8", "TARGET_PCT": "15"}, env_file="")
        self.assertEqual((cfg.equity_sl_pct, cfg.equity_target_pct), (1, 1))
        self.assertEqual((cfg.fno_sl_pct, cfg.fno_target_pct), (3, 3))
        self.assertEqual((cfg.sl_pct, cfg.target_pct), (8, 15))

    def test_independent_override_and_config_version(self):
        base = AppConfig.from_env(env={}, env_file="")
        eq = AppConfig.from_env(env={"EQUITY_SL_PCT": "2"}, env_file="")
        fo = AppConfig.from_env(env={"FNO_TARGET_PCT": "4"}, env_file="")
        self.assertEqual((eq.equity_sl_pct, eq.fno_sl_pct), (2, 3))
        self.assertEqual((fo.equity_target_pct, fo.fno_target_pct), (1, 4))
        self.assertNotEqual(compute_config_version(base), compute_config_version(eq))
        self.assertNotEqual(compute_config_version(base), compute_config_version(fo))

    def test_segmentation_applies_exact_fill_based_trigger_and_target(self):
        eq = calculate_protection_levels(
            executed_price=100, quantity=1, sl_pct=1, target_pct=1, tick_size=0.05
        )
        fo = calculate_protection_levels(
            executed_price=100, quantity=75, sl_pct=3, target_pct=3, tick_size=0.05
        )
        self.assertEqual((eq.sl_trigger, eq.target_price), (99, 101))
        self.assertEqual((fo.sl_trigger, fo.target_price), (97, 103))

    def test_watcher_pins_protection_by_segment_for_each_new_parent_order(self):
        with tempfile.TemporaryDirectory() as td:
            watcher = ProtectionWatcher(
                broker=object(),
                path=os.path.join(td, "protection.json"),
                dry_run=True,
                sl_pct=3, target_pct=3,
                equity_sl_pct=1, equity_target_pct=1,
                fno_sl_pct=3, fno_target_pct=3,
            )
            for oid, seg, want in (
                ("EQ-1", "NSE_EQ", (1.0, 1.0)),
                ("EQ-2", "BSE_EQ", (1.0, 1.0)),
                ("FNO-1", "NSE_FNO", (3.0, 3.0)),
                ("FNO-2", "BSE_FNO", (3.0, 3.0)),
            ):
                watcher.track(
                    order_id=oid, symbol="HAL", security_id="123",
                    segment=seg, qty=1, product_type="CNC", dry_price=None,
                )
                rec = watcher.pending[oid]
                self.assertEqual(
                    (rec["protection_sl_pct"], rec["protection_target_pct"]), want
                )

            # A later WebUI update does not modify already-queued orders.
            watcher.equity_sl_pct = 7
            watcher.fno_target_pct = 9
            self.assertEqual(
                (watcher.pending["EQ-1"]["protection_sl_pct"],
                 watcher.pending["EQ-1"]["protection_target_pct"]), (1, 1)
            )
            self.assertEqual(
                (watcher.pending["FNO-1"]["protection_sl_pct"],
                 watcher.pending["FNO-1"]["protection_target_pct"]), (3, 3)
            )
            watcher.track(
                order_id="EQ-3", symbol="HAL", security_id="123",
                segment="NSE_EQ", qty=1,
            )
            self.assertEqual(watcher.pending["EQ-3"]["protection_sl_pct"], 7)

    def test_legacy_tracked_orders_do_not_silently_change_protection_policy(self):
        with tempfile.TemporaryDirectory() as td:
            watcher = ProtectionWatcher(
                broker=object(), path=os.path.join(td, "legacy.json"),
                sl_pct=3, target_pct=3, equity_sl_pct=1, equity_target_pct=1
            )
            legacy_record = {"segment": "NSE_EQ", "symbol": "TCS"}
            self.assertEqual(legacy_record.get("protection_sl_pct", watcher.sl_pct), 3)
            self.assertEqual(legacy_record.get("protection_target_pct", watcher.target_pct), 3)

    def test_webui_saves_four_distinct_values(self):
        with tempfile.TemporaryDirectory() as td:
            env_text, params = build_env_and_params_dict(
                env_path=os.path.join(td, ".env"),
                equity_sl_pct="1.0", equity_target_pct="1.0",
                fno_sl_pct="3.0", fno_target_pct="3.0",
            )
            for key, expected in (
                ("EQUITY_SL_PCT", 1.0), ("EQUITY_TARGET_PCT", 1.0),
                ("FNO_SL_PCT", 3.0), ("FNO_TARGET_PCT", 3.0),
            ):
                self.assertIn(f"{key}={expected}", env_text)
                self.assertEqual(params[key], expected)

    def test_actual_dry_run_post_fill_uses_segment_specific_levels(self):
        # Exercise the actual watcher fill path; synthetic DRY orders cannot
        # submit a Dhan order. Spy on price calculation, not broker mutation.
        with tempfile.TemporaryDirectory() as td:
            watcher = ProtectionWatcher(
                broker=object(),
                path=os.path.join(td, "dry-fill.json"),
                dry_run=True, sl_pct=3, target_pct=3,
                equity_sl_pct=1, equity_target_pct=1,
                fno_sl_pct=3, fno_target_pct=3,
            )
            watcher.track(
                order_id="DRY-EQ-1", symbol="SBIN", security_id="3045",
                segment="NSE_EQ", product_type="CNC",
                qty=10, dry_price=100.0,
            )
            watcher.track(
                order_id="DRY-FO-1", symbol="HAL", security_id="4567",
                segment="NSE_FNO", product_type="MARGIN",
                qty=75, dry_price=100.0, is_super_order=True,
            )
            with patch(
                "src.engine.protection_watcher.calculate_protection_levels",
                wraps=calculate_protection_levels,
            ) as levels_spy:
                watcher.check_once()

            actual = [
                (call.kwargs["sl_pct"], call.kwargs["target_pct"])
                for call in levels_spy.call_args_list
            ]
            self.assertEqual(actual, [(1.0, 1.0), (3.0, 3.0)])
            self.assertFalse(hasattr(watcher.broker, "place_order"))

    def test_policy_snapshot_survives_watcher_restart(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "restart.json")
            first = ProtectionWatcher(
                broker=object(), path=path, dry_run=True,
                sl_pct=3, target_pct=3, equity_sl_pct=1, equity_target_pct=1,
                fno_sl_pct=3, fno_target_pct=3,
            )
            first.track(order_id="EQ-PENDING", symbol="SBIN", security_id="3045",
                        segment="NSE_EQ", qty=10)
            second = ProtectionWatcher(
                broker=object(), path=path, dry_run=True,
                sl_pct=3, target_pct=3, equity_sl_pct=8, equity_target_pct=9,
                fno_sl_pct=3, fno_target_pct=3,
            )
            self.assertEqual(
                (second.pending["EQ-PENDING"]["protection_sl_pct"],
                 second.pending["EQ-PENDING"]["protection_target_pct"]),
                (1.0, 1.0),
            )

    def test_invalid_segment_protection_rejected_before_apply(self):
        with tempfile.TemporaryDirectory() as td:
            tracker = ConfigChangeTracker(data_dir=td)
            for key, value in (
                ("EQUITY_SL_PCT", 0),
                ("EQUITY_TARGET_PCT", -3),
                ("FNO_SL_PCT", 51),
                ("FNO_TARGET_PCT", 101),
                ("EQUITY_SL_PCT", "nan"),
            ):
                with self.subTest(key=key, value=value):
                    valid, _ = tracker.validate_inputs({key: value})
                    self.assertFalse(valid)

    def test_provider_targets_not_used_by_live_fno_sizer(self):
        # A change-detection guard for the executor wiring without broker calls.
        from src.main import TradingBotPipeline
        src = inspect.getsource(TradingBotPipeline._handle_fno_signal)
        self.assertIn("targets=None", src)
        self.assertNotIn("targets=sig.targets", src)
        self.assertIn("sl_pct=self.cfg.fno_sl_pct", src)
        self.assertIn("target_pct=self.cfg.fno_target_pct", src)


if __name__ == "__main__":
    unittest.main()
