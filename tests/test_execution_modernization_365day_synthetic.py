"""365-session synthetic broker-evidence matrix (not historical market data).

For each representative date, assert a valid negative CNC delivery sale is
diagnosed as requiring journal allocation and cannot authorize gate opening,
position closure, or protection cancellation. Then introduce mismatch and
missing holdings failures. No network, broker or journal mutations.
"""
from datetime import date,timedelta
import unittest
from src.engine.delivery_sell_triage import triage_negative_delivery_position

START=date(2025,11,22)
BASE={
    "dhanClientId":"ACC","securityId":"SEC","exchangeSegment":"NSE_EQ",
    "productType":"CNC","netQty":-2,
}
HOLD={
    "dhanClientId":"ACC","securityId":"SEC","totalQty":3,
    "dpQty":3,"t1Qty":0,
}


def classify(day,quantity=2,holdings=None):
    record={
        "dhanClientId":"ACC","securityId":"SEC","exchangeSegment":"NSE_EQ",
        "productType":"CNC","exchangeTradeId":"X-"+day.isoformat(),
        "orderId":"OID-"+day.isoformat(),"transactionType":"SELL",
        "tradedQuantity":quantity,
        "exchangeTime":day.isoformat()+" 09:30:00",
    }
    return triage_negative_delivery_position(
        BASE, account_id="ACC",market_date=day.isoformat(),
        trades=[record],holdings=[HOLD] if holdings is None else holdings,
        books_complete=True,
    )


class Test365DaySyntheticDelivery(unittest.TestCase):
    def test_365_days_of_positive_and_negative_evidence(self):
        n=0
        for step in range(365):
            day=START+timedelta(days=step)
            with self.subTest(day=day.isoformat()):
                good=classify(day)
                self.assertEqual(
                    good.classification,
                    "CORROBORATED_DELIVERY_SELL_REQUIRES_ALLOCATION"
                )
                self.assertFalse(good.allow_entry_gate_open)
                self.assertFalse(good.allow_position_close)
                self.assertFalse(good.allow_protection_cancel)
                mismatch=classify(day,quantity=1)
                self.assertEqual(mismatch.classification,"NET_MISMATCH")
                missing=classify(day,holdings=[])
                self.assertEqual(missing.classification,"UNVERIFIED")
                n+=3
        self.assertEqual(n,1095)
