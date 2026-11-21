"""Fail-closed Dhan correlation lookup regressions, no network."""
import unittest
from src.engine.typed_broker_evidence import classify_external_order_lookup as classify

FOUND = {"orderId": "OID", "correlationId": "CID", "dhanClientId": "ACC", "securityId": "SEC"}

class TestExternalOrderEvidence(unittest.TestCase):
    def test_found_is_not_proof_of_fill(self):
        x=classify(correlation_id="CID", http_status=200, payload=FOUND,
                   expected_account_id="ACC", expected_security_id="SEC")
        self.assertEqual(x.kind, "FOUND_UNVERIFIED_STATUS")
        self.assertEqual(x.broker_order_id, "OID")
        self.assertFalse(x.may_resubmit)
        self.assertFalse(x.may_mark_filled)

    def test_failed_and_missing_lookup_never_authorizes_retry(self):
        for code in (401,403,404,408,429,500,502,503,504):
            with self.subTest(code=code):
                x=classify(correlation_id="CID", http_status=code)
                self.assertNotEqual(x.kind, "FOUND_UNVERIFIED_STATUS")
                self.assertFalse(x.may_resubmit)
                self.assertFalse(x.may_mark_filled)

    def test_transport_error_is_unknown(self):
        x=classify(correlation_id="CID", transport_error=TimeoutError())
        self.assertEqual(x.kind, "TRANSPORT_UNKNOWN")

    def test_identity_and_status_not_trusted(self):
        for payload in ({**FOUND, "correlationId": "OTHER"},
                        {**FOUND, "correlationId": None},
                        {**FOUND, "dhanClientId": "OTHER"},
                        {**FOUND, "securityId": "OTHER"}):
            with self.subTest(payload=payload):
                x=classify(correlation_id="CID", http_status=200, payload=payload,
                           expected_account_id="ACC", expected_security_id="SEC")
                self.assertEqual(x.kind, "IDENTITY_CONFLICT")
        x=classify(correlation_id="CID", http_status=200,
                   payload={**FOUND, "orderStatus": "TRADED"})
        self.assertFalse(x.may_mark_filled)

    def test_malformed_and_missing_requests(self):
        for payload in (None,[],[FOUND,FOUND],"ok",{**FOUND,"orderId":None}):
            self.assertEqual(classify(correlation_id="CID",http_status=200,
                                      payload=payload).kind, "MALFORMED")
        self.assertEqual(classify(correlation_id="",http_status=200,payload=FOUND).kind,
                         "INVALID_LOOKUP")
