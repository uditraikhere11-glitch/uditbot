# UDITBOT Control Center + protected stock F&O AMO

Base: verified production integration 59da9d5 (PR #54) and the opt-in Control Center facade already on GitHub main 3d41ab2.

## Equity AMO versus F&O AMO

- Equity AMO: original NSE cash CNC LIMIT order with Dhan regular /orders afterMarketOrder=true; broker queues to OPEN.
- F&O AMO: local staging ONLY after hours, then the existing MARGIN protected Super Order route at verified market OPEN. No overnight F&O broker order is sent.
- Reason: Dhan documents afterMarketOrder for /orders but does not document the AMO request fields for /super/orders. Never bypass mandatory F&O protection.
- Two independent switches in the Control Center. F&O is OFF by default and changes go through the durable WebUI configuration coordinator.
- Equity SL/target 1%/1%; F&O SL/target 3%/3%; Telegram provider targets/SL are never authoritative for execution.

## Staged F&O queue and safety

- Default FNO_AMO_ENABLED=false, FNO_AMO_MAX_AGE_SEC=64800 (18 hours, maximum). Weekend/holiday entries may expire without dispatch.
- Only NSE stock options; Telegram source ID, message ID, Dhan account, actual security ID and contract expiry are recorded.
- Durable private SQLite data/fno_amo_queue.sqlite3 with file permission 0600, 20 queued item cap, duplicate message and contract checks.
- QUEUED is not a broker submission; DISPATCHING is one reserved attempt; SUBMITTED is an acknowledgement, not a confirmed fill or verified protective legs.
- At verified market OPEN the existing F&O pipeline rechecks contract/expiry, source allowlist, risk, funds, broker safety, emergency stop and reconciliation gate.
- One staged order per polling cycle; no automatic repeat of an ambiguous broker result. DISPATCHING/UNCERTAIN blocks all other queued dispatch until manual reconciliation.
- Operator may cancel only locally QUEUED items. Edited messages and switch OFF cancel local queued items. Cancelling does NOT cancel an already submitted broker order.
- Full read-only queue status in Control Center. Never delete a queue DB during a rollback without checking for unresolved Dhan orders.

## Activation is opt-in, not automatic

- Installing these files does NOT change systemd services or cause overnight F&O orders. Enable F&O AMO only after staged acceptance.
- Control Center is an isolated src.control_center_entry ASGI application. The current Setup app stays available; parent facade wraps its GET /setup and mounts existing routes.
- This facade REQUIRES a strong operator password SHA-256 hash in private configuration (UDITBOT_WEBUI_PASSWORD_SHA256); HTTP Basic username is uditbot.
- No public HTTP. HTTPS reverse proxy or loopback SSH/IAP tunnel is required; the facade denies remote plain HTTP (426), and missing password hash (503).
- Stage on localhost before switching the WebUI unit. Never restart LIVE trading during unresolved orders/positions. Back up code and data first.
- Offline tests, complete GitHub CI, real-mode reconciliation and broker-side protective leg verification must pass before enabling any live F&O AMO strategy.

Official API docs: https://dhanhq.co/docs/v2/orders/ and https://dhanhq.co/docs/v2/super-order/

Deployment is NOT broker-certified from offline CI alone.
