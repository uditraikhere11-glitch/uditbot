"""Telegram startup card must not be green when watcher is unverified."""
import unittest
from unittest.mock import patch
from types import SimpleNamespace
from src.cli import format_startup_online_card
from src.models import TradingMode

class StartupCardReadiness(unittest.TestCase):
    def setUp(self):
        self.cfg=SimpleNamespace(instance_name="SyntheticBot",trading_mode=TradingMode.LIVE,data_dir="/nonexistent")
    def render(self,**kw):
        with patch("src.notifier.message_designer.render",return_value=None):
            return format_startup_online_card(self.cfg,allowed_chats_count=2,**kw)
    def test_gate_open_but_watcher_unverified_never_green(self):
        s=self.render(reconciliation_ok=True,watcher_verified=False)
        self.assertIn("Open (watcher unverified)",s)
        self.assertIn("Unverified",s)
        self.assertNotIn("🟢",s)
    def test_gate_closed_reports_blocked(self):
        s=self.render(reconciliation_ok=False,watcher_verified=False,
                      reconciliation_msg="Exit evidence unresolved")
        self.assertIn("New Entries Gate: Blocked",s)
        self.assertNotIn("🟢",s)
    def test_both_verified_green(self):
        s=self.render(reconciliation_ok=True,watcher_verified=True)
        self.assertIn("New Entries Gate: Open",s)
        self.assertIn("🟢",s)

if __name__=="__main__":
    unittest.main()
