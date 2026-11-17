# UditBot Configuration Tracker V2 — PR #87 candidate

**Baseline GitHub main:** bd594af2e80b92f7ff0349971ba2f2f34150ab4d (PR #86).

**Status:** DRAFT, DISABLED BY DEFAULT. Do not merge or activate on the LIVE VM before independent review, staging and owner approval.

## What this PR implements

- Existing authenticated Setup/Control Center entrypoint and CSRF remain intact.
- The current save handler delegates to the V2 coordinator **only** when environment variable UDITBOT_TRACKER_V2_ENABLE=1 is deliberately set. Otherwise the PR84/PR85 coordinator continues unchanged.
- V2 performs field-scoped compare-and-swap, single durable request, private atomic backup, lock-serialized config save, at-most-once restart intent, and strict running-process verification (PID, starttime, canonical config hash, mode, HMAC credential fingerprint).
- The actual trading pipeline registers its PID with a durable broker handoff. Seven real Dhan order POST/PUT/DELETE send sites share a cross-process flock with the restart apply barrier.
- A request denied before HTTP is classified NOT_SUBMITTED. A request sent before an error, unknown response, or local ledger failure remains uncertain. A historic unresolved mutation blocks configuration restart without preventing independently valid protective broker calls.
- A new process may adopt a dead previous producer identity, preserving any interrupted operations as unresolved; a second simultaneously living trader is blocked.
- Owner STOP is never cleared. A verified stopped bot can save next-start values without launching the service; a pending STOP blocks.
- Existing Dhan response-group fallback staging remains before any new configuration is saved. Existing parser, trading BRAIN, risk calculations, order payloads, SL/target policies, PR86 Dhan token fixes, and WebUI password/CSRF code are unchanged.

## Changed modules

Source: src/engine/tracker_v2_candidate.py, tracker_v2_integration.py, tracker_v2_safety.py, tracker_safety_preflight.py, tracker_credentials.py, broker_handoff.py; narrowly scoped hooks in src/engine/webui_update.py, config_manager.py, src/main.py, src/broker/dhan_client.py.

Regression tests: tests/test_tracker_v2_review.py, test_tracker_v2_integration.py, test_tracker_safety_preflight.py, test_tracker_credentials.py, test_tracker_broker_handoff.py. Dedicated .github/workflows/tracker-a01-a11-review.yml.

## Important release limitations

The flag is NOT turned on by this PR. No service override, automatic activation, VM modification, or broker order is included.

Production activation must be simultaneous in the WebUI and trading processes with the same private DATA_DIR and persistent HMAC key. Unknown or incompatible states deliberately fail closed.

All four workflows (Combined Release, AMO Offline, Telegram Designer, tracker-specific) must pass at the FINAL head; earlier passing runs do not certify later commits.

Remaining: independent source audit, authenticated WebUI save/refresh smoke in TEST, a disposable systemd service with real process restart/kill (fake Dhan/Telegram), full fail-injection and rollback testing, confirmation of the YESBANK/manual-risk protection evidence, and an owner-reviewed path to recover an ACK_UNCERTAIN journal safely without another restart.

**Never merge, deploy, or set UDITBOT_TRACKER_V2_ENABLE=1 on the LIVE VM without a separate owner approval.**
