"""Verify startup supervision handoff does not defer active order supervision."""
import asyncio
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace as NS
from unittest.mock import Mock, patch
from src.engine.protection_watcher import ProtectionWatcher

def record():
    return dict(order_id="BUY-OLD",correlation_id="CORR-OLD",symbol="YESBANK",
                security_id="11915",segment="NSE_EQ",product_type="CNC",
                state="TRADED",qty=1,filled_qty=1,exit_filled_qty=0,
                position_state="UNPROTECTED_MANUAL_RISK",created_at=1.0)

class StartupHandoff(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        tmp=tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.broker=NS(cfg=NS(dhan_client_id="SYNTH-ACCOUNT"),_read_governor=None)
        self.db=NS(is_manual_risk_acknowledged=Mock(return_value=True))
        self.w=ProtectionWatcher(broker=self.broker,journal_db=self.db,
                                 path=str(Path(tmp.name)/"watcher.json"))
        self.w.terminal_orders={"BUY-OLD":record()}
        self.w.should_poll=lambda *_:True
        self.w.check_once=Mock(return_value=False)
        self.w._supervision_read_complete=True
    async def drive(self,ticks=3,callback=None):
        class StopLoop(Exception): pass
        count=0
        async def tick(_):
            nonlocal count
            count+=1
            if callback:callback(count)
            if count>=ticks:raise StopLoop()
        with patch("src.engine.protection_watcher.is_lifecycle_open",return_value=False), \
             patch("src.engine.protection_watcher.asyncio.sleep",new=tick):
            with self.assertRaises(StopLoop):
                await self.w.run()
        self.assertFalse(self.w._is_running)
    async def test_completed_startup_avoids_immediate_duplicate_get(self):
        with patch("src.engine.protection_watcher.is_lifecycle_open",return_value=False):
            self.w.mark_completed_startup_supervision()
        self.assertTrue(self.w._startup_supervision_hint)
        await self.drive()
        self.w.check_once.assert_not_called()
    async def test_new_order_immediately_invalidates_handoff(self):
        with patch("src.engine.protection_watcher.is_lifecycle_open",return_value=False):
            self.w.mark_completed_startup_supervision()
        def add(n):
            if n==1:
                self.w.pending["NEW"]=dict(order_id="NEW",state="PENDING",
                    created_at=1.0,filled_qty=0,position_state="ENTRY")
        await self.drive(callback=add)
        self.assertGreaterEqual(self.w.check_once.call_count,1)
    async def test_live_leg_never_receives_handoff(self):
        self.w.terminal_orders["BUY-OLD"]["sl_order_id"]="EXIT-ACTIVE"
        with patch("src.engine.protection_watcher.is_lifecycle_open",return_value=False):
            self.w.mark_completed_startup_supervision()
        self.assertIsNone(getattr(self.w,"_startup_supervision_hint",None))
        await self.drive()
        self.assertGreaterEqual(self.w.check_once.call_count,1)

if __name__=="__main__":
    unittest.main()
