# Step 2 development notes, safety decisions and acceptance gates

## Routes

| Verb | Path | Scope |
|---|---|---|
| GET | `/` | New Control Center landing page |
| GET | `/setup` | Existing complete setup form (same Python renderer) |
| POST | `/setup` | Existing form submission, untouched |
| POST | `/setup/submit-async` | Existing durability coordinator, untouched |
| POST | `/setup/service-control` | Existing live-order-aware service start/stop; reused |
| GET | `/control/status` | Verified state, saved/active Equity AMO flag and edit gate |
| GET | `/control/health` | Read-only local health checks with UNKNOWN where verification unavailable |
| GET | `/control/amo/orders` | Read-only SQLite journal view, no broker-auth claims |
| POST | `/control/amo/equity` | CSRF and strict boolean, serialised queue through coordinator |
| POST | `/control/amo/fno` | **Not registered. Must return 404.** |

## Boundaries

No modifications to `src/main.py`, `src/broker/dhan_client.py`, `src/engine/protection_watcher.py`, parser, Yahoo, risk manager, notifications, Emergency STOP or token refresh. UI payload includes no Order IDs and uses DOM `textContent` for dynamic values. All new JSON responses include `Cache-Control: no-store`.

## Explicit limitations

- Health checks do not make a read-only Dhan authenticated profile call, so broker authentication is marked UNKNOWN rather than PASS.
- A journal-reported fill is not a broker-verified fill, and the UI labels it accordingly.
- No strong server-side operator auth is introduced by this patch. The existing WebUI authentication boundary must be assessed before exposing additional mutation endpoints to public traffic.
- Full repo integration, regression tests and concurrent-config race tests remain required for Step 3.

## Handoff to audit

Copy changed new modules into an isolated baseline checkout, run the guarded wiring script, generate `git diff -- src/setup_app.py tests/test_setup.py`, then independently inspect all new and modified files against the current production baseline. Do not merge or restart the running bot during review.