# Independent audit — V2 activation (11d2c4c baseline)

## Main-line source audit

- GitHub main was `11d2c4c3c48264a03e498761b1e5158c8eb6706a` during preparation. PR91 remains open and changes Dhan GET/reconciliation, **none** of the V2 integration files in this candidate.
- Existing `src/engine/webui_update.py` routing was opt-in via `os.environ.get('UDITBOT_TRACKER_V2_ENABLE') == '1'`. When off it enters the legacy coordinator. New guard runs first to prohibit accidental legacy fallback when V2 ledger/journal/barrier exists.
- Existing `src/engine/tracker_v2_integration.py` already performs CAS save, broker readiness, the shared external apply barrier, a single restart, PID/starttime/hash/mode/HMAC ACK, late-ACK recovery and Telegram dispatch. No changes to those algorithms are made here.
- Existing `src/main.py` registers the broker mutation producer and records the HMAC credential fingerprint **only when the flag is on**. Production activation must set the flag in the trading service and WebUI before boot.
- Existing `src/control_center_backend.py` reports Dhan profile/auth/Telegram/quotes as UNKNOWN where not independently checked. This package only adds accurate Owner intent WARN.
- Existing `src/engine/tracker_v2_safety.py` preserves pending operations, broker handoffs and owner STOP; these gates are not modified.
- Existing `deploy/install_vm_services.sh` creates new services with trading `EnvironmentFile=~/uditbot/.env`, but WebUI separately runs without it. Activation installs a **later systemd EnvironmentFile** for each unit without changing `.env`.

## Threat matrix

| Case | Expected |
|---|---|
| LIVE bot active at activation | BLOCK, no restart or stop |
| Stale Owner STOP while bot running | BLOCK, show health WARN |
| Dhan profile UNKNOWN / 429 local read gate | Do not infer broker-ready or clear governor; actual V2 save remains broker-gated |
| Invalid backup, broad permissions, stale `.env`, inconsistent/corrupt SQLite, older than journal, duplicate paths | BLOCK |
| Insecure symlinks / unclean Git / unexpected head | BLOCK |
| Previously created V2 journal, barrier or ledger | BLOCK fresh staging; preserve evidence |
| Service drop-in installed for only one unit | Check both effective configs, fail cleanup safely; V2 save routing guard blocks split brain |
| New V2 WebUI, old trading process | BLOCK configuration save before `.env` change |
| Old WebUI, existing V2 broker ledger | BLOCK legacy configuration save before `.env` change |
| V2 broker mutating HTTP in flight | Existing external barrier + ledger guards block restart |
| Missing PID/hash/starttime/HMAC after start | `--verify-running` BLOCK; no inferred success |
| Malformed Owner STOP / transition gate | BLOCK activation |
| Successful stopped staging | Only WebUI restarted; bot stays stopped |
| Failure after staged files | Revert own flags only if bot stopped and no V2 evidence; else preserve for recovery |
| Automatic downgrade after a V2 producer ran | BLOCK, preserve ledger |
| Duplicate V2 request / late ACK | Existing coordinator must never run a second restart |

## Validation evidence (local, isolated)

Run with `python3 -m unittest -v tests.test_tracker_v2_health tests.test_tracker_v2_dual_service tests.test_tracker_v2_activation_preflight tests.test_tracker_v2_activation_script tests.test_tracker_v2_source_replacements`. Syntax check `bash -n deploy/tracker_v2_activation.sh`; Python compileall of new files. Full repository regression, actual systemd verification, TestWebUI login/CSRF and real broker verification are **not claimed** by this ZIP.

## Source integration points

`patches/build_source_replacements.py` validates exact blob identities `webui_update.py=7009ebf3e9e880f034434e7fe6f6c95f6d1b12ad`, `control_center_backend.py=75b120046ac00ab91bb6aa94873d992f3b661a50` before generating complete replacement files. Any drift => hard error; no silent forward-port. This prevents PR91 or future main changes being overwritten inadvertently.

## Release recommendation

Candidate may enter independent review/TEST staging, **not LIVE activation or merge approval** until repository full CI, actual two-service synthetic systemd test, WebUI auth/CSRF, broker unavailable behavior and owner STOP staging proof pass. PR91 GET-governor/reconciliation must be resolved separately before LIVE save testing.

## Production verification hardening identified during independent recheck (18 Nov 2026)

- **H-01, resolved in candidate:** original backup preflight accepted a tar member named `trade_journal.db` containing arbitrary bytes as structurally verified. Now requires actual SQLite `quick_check`, required order/trade tables, exact archived `.env` parity and row-count parity with stopped local journal, using a private temporary copy; tests cover deliberately corrupt archive and stale/mismatched content.
- **H-02, resolved in candidate:** preflight called after installing systemd V2 flag drop-ins previously used the `stopped` stage, which rejected its own newly installed flags. A separate `staged` phase now requires the exact managed flag references without authorizing a LIVE start.
- **Remaining real-world release gates:** full repository CI on integrated commit, true two-service systemd staging and authenticated WebUI save→restart→ACK, Dhan account/positions/protection verification while Owner STOP was reconciled after the original local audit; all gates require fresh rechecks. None can be represented as passing based on these isolated tests.
