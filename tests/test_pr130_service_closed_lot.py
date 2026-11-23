"""PR130 offline T+0/T+1 service-control closure evidence regressions."""
from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from unittest.mock import patch

from zoneinfo import ZoneInfo
from src.engine.service_closed_lot import verified_service_closed_delivery_lot

IST = ZoneInfo("Asia/Kolkata")
PARENT = "321261120279408"
CHILD = "32126112357408"
GROUP = "23132611201560"
ACCOUNT = "1113731536"
SEC = "750518"


class FakeBroker:
    def __init__(self, positions, holdings, trades):
        self.positions = positions
        self.holdings = holdings
        self.trades = trades

    def get_positions(self):
        return self.positions

    def get_holdings(self):
        return self.holdings

    def get_trade_book(self):
        return self.trades


def fixture(root):
    path = root / "trade_journal.db"
    con = sqlite3.connect(path)
    con.executescript("""
    CREATE TABLE positions_lifecycle (
        parent_order_id TEXT, account_id TEXT, position_state TEXT, security_id TEXT,
        exchange_segment TEXT, product_type TEXT, buy_filled_qty INT, sell_filled_qty INT,
        confirmed_open_qty INT, manual_exit_detected INT, orphaned_legs_detected INT);
    CREATE TABLE fills (
        broker_order_id TEXT, exchange_trade_id TEXT, transaction_type TEXT,
        parent_order_id TEXT, security_id TEXT, exchange_segment TEXT,
        product_type TEXT, traded_quantity INTEGER, traded_price REAL,
        traded_at REAL, is_verified_trade INT);
    CREATE TABLE exit_allocations (
        parent_order_id TEXT, exchange_trade_id TEXT);
    CREATE TABLE protection_orders (
        parent_order_id TEXT, forever_order_id TEXT, status TEXT);
    CREATE TABLE manual_risk_acknowledgements (
        parent_order_id TEXT, account_id TEXT, quantity INTEGER);
    """)
    con.execute(
        "INSERT INTO positions_lifecycle VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (PARENT, ACCOUNT, "POSITION_CLOSED", SEC, "NSE_EQ", "CNC", 1, 1, 0, 0, 0),
    )
    previous = (datetime.now(IST) - timedelta(days=3)).timestamp()
    today = datetime.now(IST).replace(hour=9, minute=15, second=18, microsecond=0)
    today_ts = today.timestamp()
    con.executemany(
        "INSERT INTO fills VALUES (?,?,?,?,?,?,?,?,?,?,?)", [
            (PARENT, "603051757", "BUY", PARENT, SEC, "NSE_EQ", "CNC", 1, 34.69, previous, 1),
            (CHILD, "600110497", "SELL", PARENT, SEC, "NSE_EQ", "CNC", 1, 35.06, today_ts, 1),
        ],
    )
    con.execute("INSERT INTO protection_orders VALUES (?,?,?)", (PARENT, GROUP, "POSITION_CLOSED_LOCAL"))
    for oid in ("221261102657708", "23126110439808"):
        con.execute(
            "INSERT INTO manual_risk_acknowledgements VALUES (?,?,?)",
            (oid, ACCOUNT, 1),
        )
    con.commit()
    con.close()

    order = {
        "dhanClientId": ACCOUNT, "algoId": GROUP, "orderId": CHILD,
        "transactionType": "SELL", "exchangeSegment": "NSE_EQ",
        "productType": "CNC", "securityId": SEC, "orderStatus": "TRADED",
        "quantity": 1, "filledQty": 1, "remainingQuantity": 0,
    }
    forever = {
        "dhanClientId": ACCOUNT, "orderId": GROUP, "orderStatus": "TRIGGERED",
        "legName": "TARGET_LEG", "exchangeSegment": "NSE_EQ",
        "productType": "CNC", "securityId": SEC,
    }
    trade = {
        "dhanClientId": ACCOUNT, "orderId": CHILD, "exchangeTradeId": "600110497",
        "transactionType": "SELL", "exchangeSegment": "NSE_EQ",
        "productType": "CNC", "securityId": SEC, "tradedQuantity": 1,
        "tradedPrice": 35.06, "exchangeTime": today.strftime("%Y-%m-%d %H:%M:%S"),
    }
    pos = {
        "dhanClientId": ACCOUNT, "securityId": SEC, "exchangeSegment": "NSE_EQ",
        "productType": "CNC", "netQty": -1,
    }
    holdings = {
        "securityId": SEC, "totalQty": 1, "availableQty": 0,
    }
    return path, FakeBroker([pos], [holdings], [trade]), [order], [forever]


def verify(path, broker, orders, forever):
    return verified_service_closed_delivery_lot(
        db_path=path, parent_order_id=PARENT, quantity=1,
        expected_security_id=SEC, account_id=ACCOUNT, broker=broker,
        order_book=orders, forever_book=forever, super_book=[],
    )


def test_exact_vedpower_live_target_t0_and_yesbank_unchanged():
    with TemporaryDirectory() as d:
        path, broker, orders, forever = fixture(Path(d))
        assert verify(path, broker, orders, forever)
        with sqlite3.connect(path) as con:
            assert con.execute(
                "SELECT COUNT(*) FROM manual_risk_acknowledgements"
            ).fetchone()[0] == 2
            assert con.execute(
                "SELECT COUNT(*) FROM fills WHERE parent_order_id=?", (PARENT,),
            ).fetchone()[0] == 2


def test_following_day_flat_with_verified_historical_journal():
    with TemporaryDirectory() as d:
        path, broker, orders, forever = fixture(Path(d))
        broker.positions, broker.holdings, broker.trades = [], [], []
        assert verify(path, broker, [], [])


def test_manual_sell_wrong_algo_id_stays_blocked():
    with TemporaryDirectory() as d:
        path, broker, orders, forever = fixture(Path(d))
        orders[0]["algoId"] = "UNRELATED"
        assert not verify(path, broker, orders, forever)


def test_unfilled_child_remains_blocked():
    with TemporaryDirectory() as d:
        path, broker, orders, forever = fixture(Path(d))
        orders[0]["remainingQuantity"] = 1
        assert not verify(path, broker, orders, forever)


def test_missing_holdings_for_negative_cnc_fails_closed():
    with TemporaryDirectory() as d:
        path, broker, orders, forever = fixture(Path(d))
        broker.holdings = []
        assert not verify(path, broker, orders, forever)


def test_open_lifecycle_cannot_bypass_protection():
    with TemporaryDirectory() as d:
        path, broker, orders, forever = fixture(Path(d))
        with sqlite3.connect(path) as con:
            con.execute(
                "UPDATE positions_lifecycle SET position_state='PROTECTED',"
                "sell_filled_qty=0, confirmed_open_qty=1 WHERE parent_order_id=?",
                (PARENT,),
            )
        assert not verify(path, broker, orders, forever)


def test_unverified_sell_cannot_bypass_protection():
    with TemporaryDirectory() as d:
        path, broker, orders, forever = fixture(Path(d))
        with sqlite3.connect(path) as con:
            con.execute(
                "UPDATE fills SET is_verified_trade=0 WHERE exchange_trade_id='600110497'",
            )
        assert not verify(path, broker, orders, forever)


def test_active_oco_sibling_blocks_closed_claim():
    with TemporaryDirectory() as d:
        path, broker, orders, forever = fixture(Path(d))
        forever.append({
            **forever[0], "orderStatus": "ACTIVE", "legName": "STOP_LOSS_LEG"
        })
        assert not verify(path, broker, orders, forever)


def test_extra_trade_or_different_security_blocks_closed_claim():
    with TemporaryDirectory() as d:
        path, broker, orders, forever = fixture(Path(d))
        broker.trades.append({
            **broker.trades[0], "orderId": "MANUAL",
            "exchangeTradeId": "ANOTHER", "tradedPrice": 35.05
        })
        assert not verify(path, broker, orders, forever)
