# Gemini B01–B10 correction: forward-port review against PR #87

**Review date:** 17 November 2026  
**Source:** `UDITBOT_Tracker_Replacement_B01_B10_Corrected.zip` and its byte-matching `ChangedFiles(1).zip`  
**Gemini baseline:** `bd594af2e80b92f7ff0349971ba2f2f34150ab4d` (pre-PR87)  
**GitHub forward-port base:** `fc693f59b1b3b0d6382291022e68664eacd0adf3` (PR87 merged)  
**Activation:** `UDITBOT_TRACKER_V2_ENABLE` remains **off by default**. No VM update or LIVE trades.

Gemini's full seven-file replacement is **not safe to transplant unchanged**: it
replaces current `src/engine/broker_handoff.py` with a different,
incompatible `BrokerMutationHandoff` API and changes `webui_update.py`,
`config_manager.py`, `main.py`, `setup_app.py` and
`control_center_backend.py` from an older baseline. The current
`broker_handoff.py` is already called by the real
`DhanClient._broker_mutating_http()` send boundary, with producer
registration in `src/main.py`. Replacing it would break PR87's
`register_producer`, `guarded_request`, `acquire_apply_barrier` and
`release_apply_barrier` integration.

| Gemini finding | Audit against merged PR87 | Forward-port decision |
|---|---|---|
| B01 broker mutation handoff absent | PR87 has account-scoped, real HTTP transport interception and durable producer registration | Preserve PR87 implementation; do **not** introduce competing handoff class |
| B02 accidental feature activation | PR87 opt-in `UDITBOT_TRACKER_V2_ENABLE=1`, default false | Preserve |
| B03 canonical hash and secret-only fingerprint | PR87 uses canonical config SHA-256 plus independent local HMAC credential fingerprint | **Strengthen**: check exact saved .env byte revision after the new PID's ACK, before releasing broker barrier |
| B04 pre-restart crash window | PR87 journals `RESTART_ISSUED` before systemctl and never auto-retries unknown outcome | Preserve |
| B05 transition-gate ownership | PR87 shared broker apply barrier has exact account/change identity, durable write/readback | Preserve; new post-ACK .env CAS must block release |
| B06 wrong persisted env_path | PR87 journal records file revisions and uses configured `env_path`; recovery checks disk | No replacement of legacy metadata |
| B07 pending Gemini tracker file deleted/overwritten | Existing V2 migration checks legacy `pending_config_operation.json`, but not Gemini's alternate `pending_tracker_apply.json` | **Fix:** reject and preserve any valid or corrupted `pending_tracker_apply.json` |
| B08 inert Setup localStorage draft script | Current `src/setup_app.py` has no `setupForm`/localStorage draft script | No action |
| B09 baseline drift | Gemini's `bd594af2` is stale relative to merged `fc693f59` | Rebase selected invariants, not wholesale replacement |
| B10 production socket monkeypatch | Current `src/engine/config_manager.py` has no `socket.getaddrinfo` override | Preserve |

## New strict gates

1. Before a new V2 apply, an older `pending_tracker_apply.json` file blocks the new request. An unreadable, symlinked or malformed file also blocks; no file deletion, archive manipulation or reset is performed.
2. After real systemd PID/hash/mode/HMAC ACK but **before** releasing the broker fence, the on-disk `.env` must still match the originally persisted candidate SHA-256.
3. Late ACK recovery independently repeats the same on-disk revision validation immediately before releasing the fence.
4. Failure is `ACTION_REQUIRED` with barrier retained. No automatic restart, LIVE entry, order mutation, rollback or replacement of historical trade records.

## Test interpretation

Gemini's attached `FULL_TEST_LOG.txt` reports 872 passing tests on its
older reconstructed baseline. That is **not** release evidence for current
GitHub. Only CI runs on this PR's current head are release evidence.

This PR intentionally makes a **scoped forward-port**, not a claim that
Gemini's whole replacement implementation was merged. Review and staging are
required before enabling the optional tracker on the LIVE VM.
