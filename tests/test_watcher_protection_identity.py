"""Watcher verification must use the full broker instrument identity."""
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from src.broker.dhan_client import DhanClient
from src.engine.protection_watcher import ProtectionWatcher


class WatcherProtectionIdentityTests(unittest.TestCase):
    def test_forever_group_product_and_exchange_match_parent(self):
        broker = object.__new__(DhanClient)
        broker.cfg = SimpleNamespace(dhan_client_id="A")
        watcher = object.__new__(ProtectionWatcher)
        watcher.broker = broker
        watcher.require_broker_leg_confirmation = True
        parent = dict(security_id="3045", segment="NSE_EQ", product_type="CNC")
        levels = SimpleNamespace(sl_trigger=97.0, target_price=103.0)
        for segment, product, expected in [
            ("NSE_EQ", "CNC", True),
            ("NSE_EQ", "MTF", False),
            ("BSE_EQ", "CNC", False),
        ]:
            with self.subTest(segment=segment, product=product):
                broker.get_forever_orders = Mock(return_value=[dict(
                    orderId="O", orderStatus="PENDING", securityId="3045",
                    transactionType="SELL", quantity=10, quantity1=10,
                    triggerPrice=97.0, price1=103.0, exchangeSegment=segment,
                    productType=product, dhanClientId="A",
                )])
                verified, reason, _ = watcher._verify_forever_oco_on_broker(
                    "O", parent, 10, levels, 0.05
                )
                self.assertEqual(verified, expected, reason)

    def test_malformed_structured_read_never_falls_back_to_ack(self):
        watcher = object.__new__(ProtectionWatcher)
        watcher.require_broker_leg_confirmation = False
        levels = SimpleNamespace(sl_trigger=97.0, target_price=103.0)
        for route in ("forever_oco", "super_order"):
            with self.subTest(route=route):
                watcher.broker = SimpleNamespace(
                    **{"evaluate_" + route + "_protection": Mock(return_value=None)},
                    verify_forever_leg_active=Mock(return_value=(True, "ACK")),
                    verify_super_order_legs_active=Mock(return_value=(True, "ACK")),
                )
                verify = getattr(watcher, "_verify_" + route + "_on_broker")
                verified, _, terminal = verify("O", {"security_id": "1"}, 10, levels, 0.05)
                self.assertFalse(verified)
                self.assertFalse(terminal)
                watcher.broker.verify_forever_leg_active.assert_not_called()
                watcher.broker.verify_super_order_legs_active.assert_not_called()

    def test_placement_ack_alone_does_not_verify_resting_protection(self):
        watcher = object.__new__(ProtectionWatcher)
        watcher.require_broker_leg_confirmation = False
        watcher.broker = SimpleNamespace(get_forever_orders=lambda: [])
        verified, _, terminal = watcher._verify_forever_oco_on_broker(
            "O", {"security_id": "1"}, 10,
            SimpleNamespace(sl_trigger=97.0, target_price=103.0), 0.05,
        )
        self.assertFalse(verified)
        self.assertFalse(terminal)

    def test_terminal_rejection_requires_matching_group_identity(self):
        broker = object.__new__(DhanClient)
        broker.cfg = SimpleNamespace(dhan_client_id="A")
        watcher = object.__new__(ProtectionWatcher)
        watcher.broker = broker
        row = dict(orderId="O", securityId="1", exchangeSegment="NSE_EQ",
                   transactionType="SELL", dhanClientId="A", productType="CNC")
        parent = dict(security_id="1", segment="NSE_EQ", product_type="CNC")
        for status, product, terminal in [
            ("REJECTED", "CNC", True), ("CANCELLED", "MTF", False),
            ("TRIGGERED", "CNC", False),
        ]:
            with self.subTest(status=status, product=product):
                broker.get_forever_orders = Mock(return_value=[
                    dict(row, orderStatus=status, productType=product)
                ])
                ok, _, rejected = watcher._verify_forever_oco_on_broker(
                    "O", parent, 10, SimpleNamespace(sl_trigger=97, target_price=103), 0.05
                )
                self.assertFalse(ok)
                self.assertEqual(rejected, terminal)
