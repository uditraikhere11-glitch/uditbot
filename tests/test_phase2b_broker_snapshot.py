"""Focused contract tests; no broker I/O or journal writes."""
from dataclasses import FrozenInstanceError, replace
from datetime import date
import unittest

from src.broker.broker_snapshot import (
    NS, LIVE_BOOKS, BookEvidence, BookStatus, BrokerSnapshot,
    EvidenceRequest, Generation, HistoryCoverage, Purpose, SessionContext,
    SessionPhase, account_key, payload_digest,
)

ACCOUNT = '9000000001'
START = 100 * NS
UTC = 2_000_000_000 * NS
GEN = Generation(account_key(ACCOUNT), 'test-boot', 1, 2, 3, 4, 5)
SESSION = SessionContext(SessionPhase.SETTLED, date(2026, 11, 26), UTC, 'a'*64, True)


def book(key, payload=None, *, offset=0, start_offset=0, params=None, status=None):
    if payload is None:
        payload = {'dhanClientId': ACCOUNT} if key == 'profile' else []
    endpoint = '/v2/trades/'+key.removeprefix('history:').replace(':','/') if key.startswith('history:') else '/v2/'+key
    return BookEvidence(key, endpoint, params or {},
                        status or (BookStatus.OK_RECORDS if payload else BookStatus.OK_EMPTY),
                        200, None, None, True, UTC+start_offset, UTC+offset,
                        START+start_offset, START+offset, ACCOUNT, GEN.token_epoch,
                        payload, payload_digest(payload), True)


def snap(**updates):
    vals = dict(snapshot_id='snap-1', generation=GEN, account_id=ACCOUNT,
                profile_account_id=ACCOUNT, profile_verified=True, session=SESSION,
                capture_started_utc_ns=UTC, capture_finished_utc_ns=UTC+6*NS,
                capture_started_mono_ns=START, capture_finished_mono_ns=START+6*NS,
                historical_trades=(), order_status={}, trades_for_order={},
                profile=book('profile'), funds=None, edis=None, history_coverage=(),
                complete=True, invalid_reasons=(), producer_pid=123,
                producer_starttime=456, journal_digest='b'*64, profile_valid_until_mono_ns=START+600*NS)
    vals.update({name: book(name) for name in LIVE_BOOKS})
    vals.update(updates)
    return BrokerSnapshot(**vals)


class SnapshotTests(unittest.TestCase):
    def request(self, **updates):
        vals = dict(account_id=ACCOUNT, purpose=Purpose.ENTRY, required_books=LIVE_BOOKS,
                    deadline_mono_ns=START+1000*NS)
        vals.update(updates)
        return EvidenceRequest(**vals)

    def test_raw_values_and_recursive_immutability(self):
        raw = [{'securityId':'11915', 'totalQty':2, 'other':None, 'nested':{'a':[1, True]}}]
        evidence = book('holdings', raw)
        raw[0]['nested']['a'].append(2)
        self.assertEqual(evidence.payload[0]['nested']['a'], (1, True))
        self.assertIsNone(evidence.payload[0]['other'])
        self.assertNotIn('absent', evidence.payload[0])
        with self.assertRaises(TypeError):
            evidence.payload[0]['totalQty'] = 0
        with self.assertRaises(FrozenInstanceError):
            evidence.complete = False

    def test_failed_read_never_has_empty_success_payload(self):
        success = book('holdings')
        with self.assertRaises(ValueError):
            replace(success, status=BookStatus.HTTP_FAILURE, complete=False)
        failed = replace(success, status=BookStatus.HTTP_FAILURE, complete=False,
                         payload=None, payload_sha256=None, http_status=500)
        self.assertIn('BOOK_UNAVAILABLE:holdings', snap(holdings=failed).validation_reasons(
            self.request(), GEN, START+6*NS))

    def test_unsent_200_cannot_count(self):
        with self.assertRaises(ValueError):
            replace(book('positions'), transport_sent=False)

    def test_malformed_payload_and_account_rejected(self):
        for invalid in ([1], [None], {'x':3}):
            with self.subTest(payload=invalid), self.assertRaises(ValueError):
                book('holdings', invalid)
        with self.assertRaises(ValueError):
            book('holdings', [{'dhanClientId':'different'}])
        with self.assertRaises(ValueError):
            book('profile', {'dhanClientId':'different'})
        with self.assertRaises(ValueError):
            book('profile', {'dhanClientId':ACCOUNT, 'access-token':'secret'})

    def test_timestamp_and_generation_fences(self):
        s = snap()
        self.assertEqual(s.validation_reasons(self.request(), GEN, START+10*NS), ())
        self.assertIn('LIVE_BOOK_STALE', s.validation_reasons(self.request(), GEN, START+10*NS+1))
        self.assertIn('GENERATION_MISMATCH', s.validation_reasons(self.request(), replace(GEN, mutation_epoch=3), START+6*NS))
        self.assertIn('CAPTURE_OUTSIDE_FENCE', s.validation_reasons(self.request(min_capture_mono_ns=START+1), GEN, START+6*NS))
        self.assertIn('DISPLAY_ONLY_REQUEST', s.validation_reasons(self.request(purpose=Purpose.UI), GEN, START+6*NS))

    def test_skew_and_independent_confirmations(self):
        s = snap(positions=book('positions', offset=6*NS))
        self.assertIn('LIVE_BOOK_SKEW', s.validation_reasons(self.request(), GEN, START+6*NS))
        s = snap()
        req = self.request(independent_after_snapshot_id='snap-1')
        self.assertIn('INDEPENDENT_CAPTURE_REQUIRED', s.validation_reasons(req, GEN, START+6*NS, independent_after_mono_ns=START-1))
        req = self.request(independent_after_snapshot_id='prior')
        self.assertIn('INDEPENDENT_CAPTURE_REQUIRED', s.validation_reasons(req, GEN, START+6*NS))
        self.assertEqual(s.validation_reasons(req, GEN, START+6*NS, independent_after_mono_ns=START-1), ())

    def test_history_requires_terminal_page(self):
        lo = hi = date(2026, 11, 25)
        req = self.request(history_ranges=((lo, hi),))
        self.assertIn('HISTORY_INCOMPLETE', snap().validation_reasons(req, GEN, START+6*NS))
        page = book(f'history:{lo}:{hi}:0', [{'orderId':'x'}], params={'from_date':str(lo),'to_date':str(hi),'page':0})
        tail = book(f'history:{lo}:{hi}:1', params={'from_date':str(lo),'to_date':str(hi),'page':1})
        coverage = HistoryCoverage(lo,hi,(page.book_key,tail.book_key),True,True,START,True)
        s = snap(historical_trades=(page,tail), history_coverage=(coverage,))
        self.assertEqual(s.validation_reasons(req, GEN, START+6*NS), ())
        broken = replace(s, historical_trades=(page,))
        self.assertIn('HISTORY_INCOMPLETE', broken.validation_reasons(req, GEN, START+6*NS))

    def test_profile_expiry_and_pre_capture_book_block(self):
        self.assertIn('PROFILE_EXPIRY_UNVERIFIED_OR_EXPIRED',
            snap(profile_valid_until_mono_ns=START+6*NS).validation_reasons(self.request(),GEN,START+6*NS))
        with self.assertRaises(ValueError):
            snap(holdings=book('holdings',offset=-1,start_offset=-1))

    def test_overlapping_history_and_wrong_page_identity_block(self):
        lo=hi=date(2026,11,25)
        with self.assertRaises(ValueError):
            self.request(history_ranges=((lo,hi),(lo,date(2026,11,26))))
        page=book(f'history:{lo}:{hi}:0',params={'from_date':str(lo),'to_date':str(hi),'page':1})
        coverage=HistoryCoverage(lo,hi,(page.book_key,),True,True,START,True)
        self.assertIn('HISTORY_INCOMPLETE',snap(historical_trades=(page,),history_coverage=(coverage,)).validation_reasons(
            self.request(history_ranges=((lo,hi),)),GEN,START+6*NS))

    def test_child_evidence_must_bind_exact_order(self):
        with self.assertRaises(ValueError):
            snap(order_status={'abc':book('order:abc', {'orderId':'abc'}, params={'order_id':'wrong'})})
        req = self.request(order_status_ids=('abc',))
        self.assertIn('ORDER_STATUS_UNAVAILABLE:abc', snap().validation_reasons(req, GEN, START+6*NS))


if __name__ == '__main__':
    unittest.main()
