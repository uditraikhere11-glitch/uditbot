"""Offline regression tests for non-authoritative ingress queue telemetry.

Does not construct a broker or send network requests. Synthetic timing only.
"""
import math
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from src.engine.latency_monitor import LatencyMonitor
from src.models import TradingMode
from src.main import TradingBotPipeline


class QueueMetricsTests(unittest.TestCase):
    def test_empty_and_bounded_samples(self):
        m = LatencyMonitor(max_samples=3)
        self.assertEqual(m.get_summary()["queue_samples"], 0)
        for sample in (10, 20, 30, 40):
            m.record_preprocessing_wait(total_ms=sample, lock_ms=sample / 2)
        d = m.get_summary()
        self.assertEqual(d["queue_samples"], 3)
        self.assertEqual(len(m.ingress_to_processing_ms), 3)
        self.assertGreater(d["p99_ingress_to_processing_ms"], d["p50_ingress_to_processing_ms"])

    def test_invalid_metrics_are_ignored(self):
        m = LatencyMonitor()
        for total, lock in ((-1, 0), (10, -1), (3, 8), (math.nan, 0), (math.inf, 1)):
            m.record_preprocessing_wait(total_ms=total, lock_ms=lock)
        self.assertEqual(m.get_summary()["queue_samples"], 0)

    def test_no_callback_timestamp_preserves_existing_behavior(self):
        m = LatencyMonitor()
        p = object.__new__(TradingBotPipeline)
        p._pipeline_lock = threading.Lock()
        p.cfg = SimpleNamespace(
            monitored_channel_ids=[123],
            trading_mode=TradingMode.TEST,
            test_group_id=None, enforce_source_allowlist=True,
            log_group_id=None,
        )
        with patch("src.main.GLOBAL_LATENCY_MONITOR", m):
            result = p.process_raw_message("Buy ABC", channel_id=456)
        self.assertFalse(result.is_valid)
        self.assertEqual(m.get_summary()["queue_samples"], 0)

    def test_callback_to_lock_measurement_preserves_gate(self):
        m = LatencyMonitor()
        p = object.__new__(TradingBotPipeline)
        p._pipeline_lock = threading.Lock()
        p.cfg = SimpleNamespace(
            monitored_channel_ids=[123], trading_mode=TradingMode.TEST,
            test_group_id=None, enforce_source_allowlist=True, log_group_id=None,
        )
        callback = time.perf_counter_ns() - 5_000_000
        with patch("src.main.GLOBAL_LATENCY_MONITOR", m):
            result = p.process_raw_message("Buy ABC", channel_id=456,
                                           callback_monotonic_ns=callback)
        self.assertFalse(result.is_valid)
        d = m.get_summary()
        self.assertEqual(d["queue_samples"], 1)
        self.assertGreaterEqual(d["p50_ingress_to_processing_ms"], 5.0)

    def test_untrusted_future_timestamp_is_ignored(self):
        m = LatencyMonitor()
        p = object.__new__(TradingBotPipeline)
        p._pipeline_lock = threading.Lock()
        p.cfg = SimpleNamespace(
            monitored_channel_ids=[123], trading_mode=TradingMode.TEST,
            test_group_id=None, enforce_source_allowlist=True, log_group_id=None,
        )
        with patch("src.main.GLOBAL_LATENCY_MONITOR", m):
            p.process_raw_message("Buy ABC", channel_id=456,
                                  callback_monotonic_ns=time.perf_counter_ns() + 10_000_000)
        self.assertEqual(m.get_summary()["queue_samples"], 0)


if __name__ == "__main__":
    unittest.main()
