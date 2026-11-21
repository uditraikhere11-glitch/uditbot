"""Production-risk journal query equivalence: historical rows stay out of hot path.

Uses the REAL JournalDB schema, migrations, and risk methods. A legacy
full-history SQL oracle is retained in this test to verify no change to daily
order counts, portfolio positions or exposure. All accounts/orders synthetic.
"""
import sqlite3
import tempfile
import unittest
from pathlib import Path

from src.journal.db import JournalDB


START=1800000000.0


def _legacy_risk(conn):
    sums={}
    for r in conn.execute(
        "SELECT broker_order_id,SUM(traded_quantity) AS sq FROM fills "
        "GROUP BY broker_order_id"
    ):
        if r["broker_order_id"]:
            sums[str(r["broker_order_id"])]=int(r["sq"] or 0)
    rows=conn.execute(
        "SELECT broker_order_id,security_id,price,quantity,"
        "COALESCE(filled_quantity,0) AS filled_qty,status "
        "FROM orders WHERE created_at>=? AND trading_mode='LIVE'",
        (START,),
    )
    active=set()
    exposure=0.0
    for r in rows:
        st=str(r["status"] or "").upper()
        boid=str(r["broker_order_id"] or "")
        requested=int(r["quantity"] or 0)
        filled=int(r["filled_qty"] or 0) or sums.get(boid,0)
        if st in ("PENDING","ACKED","TRANSIT","TRIGGERED","PART_FILLED","PART_TRADED"):
            effective=requested
        elif st=="TRADED":
            effective=filled if filled>0 else requested
        elif st in ("CANCELLED","EXPIRED") and filled>0:
            effective=filled
        else:
            effective=0
        if effective>0:
            active.add(str(r["security_id"]))
            exposure+=float(r["price"] or 0.0)*effective
    cnt=conn.execute(
        "SELECT COUNT(*) FROM orders WHERE created_at>=? "
        "AND trading_mode='LIVE' AND ("
        "status NOT IN ('REJECTED','CANCELLED','EXPIRED','NOT_SUBMITTED') "
        "OR COALESCE(filled_quantity,0)>0 "
        "OR (broker_order_id IS NOT NULL AND broker_order_id!='' "
        "AND broker_order_id IN "
        "(SELECT broker_order_id FROM fills WHERE traded_quantity>0)))",
        (START,),
    ).fetchone()[0]
    return dict(daily_trades=cnt,open_positions=len(active),
                current_exposure=exposure)


class TestRiskJournalIndexedEquivalence(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=Path(self.tmp.name)/"real-schema.sqlite3"
        self.journal=JournalDB(str(self.path),account_id="SYNTHETIC")
        with self.journal._get_conn() as c:
            # Every tested order is a synthetic local journal record.
            cases=[
                ("E1","PENDING",10,0,100.0,START+1,"SEC1","LIVE"),
                ("E2","TRADED",10,2,101.0,START+2,"SEC2","LIVE"),
                ("E3","CANCELLED",10,0,102.0,START+3,"SEC3","LIVE"),
                ("E4","REJECTED",10,0,103.0,START+4,"SEC4","LIVE"),
                ("E5","PART_TRADED",4,1,104.0,START+5,"SEC5","LIVE"),
                ("E6","EXPIRED",10,0,105.0,START+6,"SEC6","LIVE"),
                ("E7","TRADED",6,0,106.0,START+7,"SEC7","LIVE"),
                ("TEST1","PENDING",10,0,107.0,START+8,"SEC8","TEST"),
                ("OLD","PENDING",10,0,108.0,START-1,"SEC9","LIVE"),
                ("E8","NOT_SUBMITTED",10,0,109.0,START+9,"SEC10","LIVE"),
            ]
            c.executemany(
                "INSERT INTO orders(correlation_id,broker_order_id,"
                "trading_mode,security_id,price,quantity,filled_quantity,"
                "status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                [(oid,oid,mode,sec,price,qty,filled,status,stamp,stamp)
                 for oid,status,qty,filled,price,stamp,sec,mode in cases],
            )
            fills=[("E2","X-E2-1",3),("E3","X-E3-1",2),("E6","X-E6-1",1)]
            c.executemany(
                "INSERT INTO fills(broker_order_id,exchange_trade_id,traded_quantity)"
                " VALUES(?,?,?)",fills,
            )

    def tearDown(self):
        self.tmp.cleanup()

    def test_exact_equivalence_with_old_sql(self):
        with self.journal._get_conn() as c:
            expected=_legacy_risk(c)
        got=self.journal.get_live_portfolio_risk_state(start_of_day=START)
        self.assertEqual(got,expected)
        self.assertGreater(got["open_positions"],0)

    def test_today_counts_include_cancelled_partial_and_exclude_test(self):
        self.assertEqual(self.journal.get_today_orders_count(START),6)

    def test_historical_rows_do_not_change_today_risk(self):
        before=self.journal.get_live_portfolio_risk_state(START)
        with self.journal._get_conn() as c:
            c.executemany(
                "INSERT INTO fills(broker_order_id,exchange_trade_id,traded_quantity)"
                " VALUES(?,?,?)",
                [(f"HIST-{i}",f"X-HIST-{i}",1) for i in range(14000)],
            )
        after=self.journal.get_live_portfolio_risk_state(START)
        self.assertEqual(before,after)
        with self.journal._get_conn() as c:
            self.assertEqual(after,_legacy_risk(c))

    def test_more_than_one_sqlite_batch_preserves_every_execution(self):
        # 425 currently active broker orders require TWO bounded IN batches.
        with self.journal._get_conn() as c:
            c.executemany(
                "INSERT INTO orders(correlation_id,broker_order_id,trading_mode,"
                "security_id,price,quantity,filled_quantity,status,created_at)"
                " VALUES(?,?,?,?,?,?,?,?,?)",
                [(f"NEW-{i}",f"NEW-{i}","LIVE",f"S-{i}",10.0,3,0,
                  "TRADED",START+100+i) for i in range(425)],
            )
            c.executemany(
                "INSERT INTO fills(broker_order_id,exchange_trade_id,"
                "traded_quantity) VALUES(?,?,?)",
                [(f"NEW-{i}",f"X-NEW-{i}",2) for i in range(425)],
            )
            expected=_legacy_risk(c)
        actual=self.journal.get_live_portfolio_risk_state(START)
        self.assertEqual(actual,expected)
        self.assertEqual(actual["daily_trades"],
                         self.journal.get_today_orders_count(START))

    def test_null_broker_ids_and_duplicate_order_ids_preserve_legacy_semantics(self):
        with self.journal._get_conn() as c:
            c.execute(
                "INSERT INTO orders(correlation_id,broker_order_id,trading_mode,"
                "security_id,price,quantity,filled_quantity,status,created_at) "
                "VALUES('NO-OID',NULL,'LIVE','MISSING-ID',1.0,1,0,'PENDING',?)",
                (START+55,),
            )
            c.execute(
                "INSERT INTO orders(correlation_id,broker_order_id,trading_mode,"
                "security_id,price,quantity,filled_quantity,status,created_at) "
                "VALUES('DUPLICATE-BOID','E3','LIVE','DUP',10.0,2,0,"
                "'CANCELLED',?)",
                (START+56,),
            )
            expected=_legacy_risk(c)
        self.assertEqual(self.journal.get_live_portfolio_risk_state(START),
                         expected)

    def test_new_fill_visible_immediately_no_cache(self):
        before=self.journal.get_live_portfolio_risk_state(START)
        with self.journal._get_conn() as c:
            c.execute("INSERT INTO fills(broker_order_id,exchange_trade_id,"
                      "traded_quantity) VALUES('E3','X-E3-2',2)")
        after=self.journal.get_live_portfolio_risk_state(START)
        self.assertEqual(after["open_positions"],before["open_positions"])
        self.assertEqual(after["current_exposure"],
                         before["current_exposure"]+2*102.0)

    def test_hot_path_uses_actual_broker_order_index(self):
        with self.journal._get_conn() as c:
            indexes={row[1] for row in c.execute("PRAGMA index_list(fills)")}
            self.assertIn("idx_risk_fills_order_id",indexes)
            plan=[row[3] for row in c.execute(
                "EXPLAIN QUERY PLAN SELECT broker_order_id,"
                "SUM(traded_quantity) FROM fills "
                "WHERE broker_order_id IN ('E2','E3') "
                "GROUP BY broker_order_id"
            )]
            self.assertTrue(any("idx_risk_fills_order_id" in p for p in plan),plan)
            order_indexes={row[1] for row in c.execute("PRAGMA index_list(orders)")}
            self.assertIn("idx_risk_orders_mode_day",order_indexes)

    def test_migration_rerun_does_not_rewrite_existing_trade_evidence(self):
        with self.journal._get_conn() as c:
            prior=c.execute("SELECT COUNT(*) FROM fills").fetchone()[0]
        JournalDB(str(self.path),account_id="SYNTHETIC")
        with self.journal._get_conn() as c:
            self.assertEqual(c.execute("SELECT COUNT(*) FROM fills").fetchone()[0],prior)
            self.assertEqual(self.journal.get_live_portfolio_risk_state(START),
                             _legacy_risk(c))

    def test_missing_fills_schema_fails_closed(self):
        with self.journal._get_conn() as c:
            c.execute("DROP TABLE fills")
        with self.assertRaises(sqlite3.OperationalError):
            self.journal.get_live_portfolio_risk_state(START)
