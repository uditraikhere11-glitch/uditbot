# Broker-native F&O AMO — order-ID-only review patch

Target: `uditraikhere11-glitch/uditbot` PR #56, commit
`2461eab9454ba3ba087225fc776e4bca01528d5d`.

## Requirement

After hours, accept an eligible trusted stock-option BUY and submit it to
Dhan **immediately** through the documented `POST /v2/orders` API, using
`exchangeSegment=NSE_FNO`, `productType=MARGIN`, `orderType=LIMIT`,
`afterMarketOrder=true`, `amoTime=OPEN`. A genuine `orderId` returned by Dhan
confirms Dhan received the submission. It does NOT guarantee execution or a
successful exchange acceptance after market open.

SL/target deliberately NOT attached to these F&O AMOs. **If the order fills,
it creates an unprotected derivatives position and requires manual management.**
The watcher continues to observe broker fills but will not attempt unsupported
MARGIN Forever OCO or resubmit the entry as a retry. Existing market-hours
F&O Super Order SL/TP logic remains in place and is not changed by this patch.

Additional checks: F&O AMO on weekdays is blocked from 09:15 until 15:30 IST (Dhan window); malformed or null order IDs and broker terminal/unknown statuses are never labeled successful. For missing IDs or HTTP/auth uncertainty, no automatic resubmission or optimistic reconciliation is attempted.

All prior safety checks remain: LIVE mode, FNO_ACTIVE, authorized source,
deduplication, contract/expiry/lot validation, configured sizing, broker funds,
portfolio risk, emergency STOP, reconciliation gate, transition gate and age.
FNO_AMO_ENABLED remains OFF by default and independent of EQUITY_AMO_ENABLED.

Existing PR #56 local queued records are **not** replayed after installing
this patch. They remain visible for explicit cancellation/review; deleting
staged records does not cancel an actual broker order.

## Review instructions

Do NOT apply to running LIVE VM directly. Against the exact PR #56 commit above:

```bash
git apply --check FNO_AMO_ORDER_ID_ONLY.patch
git apply FNO_AMO_ORDER_ID_ONLY.patch
python3 -m unittest tests.test_fno_amo_native_order_id -v
python3 -m unittest tests.test_fno_amo_protected_queue -v
python3 -m unittest discover -s tests -v
```

Run the regression suite and independently verify market-open F&O Super orders,
Equity AMO, watcher restart/recovery and emergency STOP behavior before merging.
This is source-only review packaging, not a claim of successful LIVE Dhan
submission. Verify broker account permissions and actual order receipt in Dhan.

API: https://dhanhq.co/docs/v2/orders/

No GitHub branch/PR/merge or VM was modified during preparation.

## Offline reviewer test evidence (2026-11-05)

- 25/25 focused tests passed in an isolated, mock-only Python harness derived from the PR56 `place_order` implementation and AMO watcher guards.
- 4 release blockers were discovered with adversarial tests and fixed in this revision: `null` order ID, CANCELLED/EXPIRED/FAILED success mislabeling, 15:25–15:30 IST F&O AMO mismatch, and ambiguous 5xx optimism (reconciled ID without pending-status evidence).
- Patch checksum and synthetic Git application verified.
- **The full repository offline regression suite could not be run**: this bundle does not contain the full PR56 checkout and the test container cannot reach GitHub directly. The broker was not contacted. This remains a release gate.
- Required before LIVE: exact-base `git apply --check`, full `unittest discover -s tests`, and controlled Dhan broker-order confirmation.
