# PR #87 A01–A11 production acceptance gates

Baseline: bd594af2e80b92f7ff0349971ba2f2f34150ab4d. Review status: draft, not production-authorized.

| Item | Required invariant | Check before approval |
| --- | --- | --- |
| A01 | No-op is never falsely runtime verified | MainPID, starttime, canonical hash, mode, HMAC all checked |
| A02 | Legacy pending operations never silently deleted | Pending/queued/crashed worker and symlinked journal fail closed |
| A03 | Owner STOP and manual-risk state unchanged | Stopped save no start; deferred STOP blocks |
| A04 | Unknown restart does not trigger duplicate or env rollback | Restart intent fsynced, ACK_UNCERTAIN retained |
| A05 | Emergency Stop, active broker orders block unsafe restart | Shared real Dhan mutation fence, broker/journal gate |
| A06 | Exact latest GitHub main baseline | Re-compare before merge |
| A07 | PR85 and PR86 preserved | Source diff and full regression, no auth/read governor changes |
| A08 | No wall-clock expiry of live apply fence | Cross-process PID/starttime and lock validation |
| A09 | No retrospective SL/OCO edits | Unmodified order bodies and risk/strategy logic |
| A10 | Zero real broker/Telegram tests | Full Combined, AMO, focused test CI at HEAD |
| A11 | Existing Setup form, CSRF, LIVE confirmation, AMO safe | Integrated HTTP and TEST-systemd checks still required |

## Mandatory independent staging

1. Use an isolated TEST-mode Debian systemd unit and fake Dhan/Telegram; no LIVE orders.
2. Verify authenticated Setup GET/POST and saved settings, owner STOP, TEST↔LIVE confirmation prevention, Telegram response-group fallback, AMO options, and disabled-default behavior.
3. Force race between an order POST/PUT/DELETE and a WebUI restart. Exactly one should acquire the cross-process entry barrier; no duplicate broker POST.
4. Test HTTP 200 with missing orderId, 408, 429, 5xx, timeout, post-send fsync fault, lost ACK, and crash after restart intent. Never mark an unknown request NOT_SUBMITTED.
5. Verify reboot with prior in-flight mutation, unresolved ledger preservation, no second live producer, continued protective operations, and blocked configuration restart until positively reconciled.
6. Verify service MainPID, OS /proc starttime, canonical config version, HMAC credential identity, STOP intent, journal and broker evidence before releasing apply barrier.
7. Backup VM source, private runtime data, .env, systemd unit and existing working Git branch. Never reset YESBANK orders, manual-risk acknowledgements, Dhan governor, Emergency Stop or trade_journal.db.
8. Document and verify an operator-facing safe ACK_UNCERTAIN recovery workflow. No blind retry and no manual deletion of safety records.
9. Independent ChatGPT audit every final change against latest GitHub. Merge only after explicit owner authorization; deployment is a separate step.

No LIVE VM changes were performed by this PR preparation.
