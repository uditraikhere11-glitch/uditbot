"""Source-arrival identity stability across same-millisecond UditBot signals."""
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from src.main import TradingBotPipeline
from src.models import TradingMode


class TestTraceIdentity(unittest.TestCase):
    def test_sequential_signals_do_not_reuse_trace_identifier(self):
        p=object.__new__(TradingBotPipeline)
        p._pipeline_lock=threading.Lock()
        p.cfg=SimpleNamespace(monitored_channel_ids=[123],
            trading_mode=TradingMode.TEST,test_group_id=None,
            enforce_source_allowlist=True,log_group_id=None)
        p.db=SimpleNamespace(record_signal=lambda *a,**k: None)
        with patch("src.main.SignalLatencyTrace") as trace:
            p.process_raw_message("ignored",channel_id=456,current_time=1000)
            p.process_raw_message("ignored",channel_id=456,current_time=1000)
        args=[c.kwargs["trace_id"] for c in trace.call_args_list]
        self.assertEqual(len(args),2)
        self.assertNotEqual(args[0],args[1])
