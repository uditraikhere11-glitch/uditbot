"""Offline callback and lock intervals using the real rejected-signal entry path."""
from concurrent.futures import ThreadPoolExecutor
import json
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

from src.engine.latency_monitor import LatencyMonitor
from src.main import TradingBotPipeline
from src.models import TradingMode
from tools.execution_modernization_metrics import describe


def measure():
    monitor = LatencyMonitor(max_samples=120)
    pipeline = object.__new__(TradingBotPipeline)
    pipeline._pipeline_lock = threading.Lock()
    pipeline.db = SimpleNamespace(record_signal=lambda *args, **kwargs: None)
    pipeline.cfg = SimpleNamespace(monitored_channel_ids=[123], trading_mode=TradingMode.TEST,
                                   test_group_id=None, enforce_source_allowlist=True, log_group_id=None)
    with patch("src.main.GLOBAL_LATENCY_MONITOR", monitor), ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(pipeline.process_raw_message, "fixture", channel_id=456,
                               callback_monotonic_ns=time.perf_counter_ns()) for _ in range(120)]
        assert all(not future.result().is_valid for future in futures)
    assert len(monitor.pipeline_lock_wait_ms) == 120
    return {"fixture": "synthetic 120 disallowed callbacks, four worker threads",
            "callback_to_worker_ms": describe([t - w for t, w in zip(
                monitor.ingress_to_processing_ms, monitor.pipeline_lock_wait_ms)]),
            "pipeline_lock_ms": describe(monitor.pipeline_lock_wait_ms),
            "dhan_request_to_ack_ms": describe([]), "fill_to_verified_protection_ms": describe([]),
            "risk_query_report": "journal_risk_benchmark.json", "orders_submitted": 0}


if __name__ == "__main__":
    print(json.dumps(measure(), indent=2))
