# Isolated Forever OCO orphan cleanup

**Default: OFF and PREVIEW ONLY.** This module is not imported by the trading listener,
protection watcher, or WebUI. Removing the module and the opt-in timer leaves the core
bot unchanged.

## Why the one-liner is dangerous

Dhan's `DELETE /v2/orders/{orderId}` is only for regular orders. Forever
OCOs require `DELETE /v2/forever/orders/{orderId}`. An empty positions
response is not proof that a CNC position is closed: settled/T1 shares can
live in Holdings. An unrelated user-created Forever OCO must never be
cancelled merely because the positions net quantity is zero.

## Safety predicates before any DELETE

The worker must verify all of these:
- LIVE account, exact account-bound journal ownership, specific `FOREVER_OCO` attempt
  with `CONFIRMED` state and corresponding active `protection_orders` record
- Existing parent entry order tracked in the journal, no unresolved local
  entry intent for that exact security/segment/product
- A fully verified active SELL OCO pair at Dhan (same security ID, product,
  segment, account, quantity)
- Successful read of all four sources: positions, holdings (including T1,
  settled and collateral), order book and Forever order book
- No corresponding open position, holding, or active broker order
- Second independent read after at least 1 second confirms that nothing
  changed before cancellation is requested

Non-CNC or ambiguous entries fail closed. Broker failures and malformed reads
are errors, never interpreted as empty lists. A submitted DELETE is logged
as a **request**, not as verified final `CANCELLED` status.

## Install files on VM and run read-only preview first

From the usual VM browser SSH terminal:

```bash
cd ~/uditbot
venv/bin/python3 -m src.engine.orphan_cleanup
```

Preview only reads Dhan and local journal. It never calls DELETE.
Review all listed `eligible` IDs against Dhan's Forever book and exposure.

## Optional automatic worker — only after preview review

An independent systemd **user** timer is provided, disabled by default.
The operator must deliberately install and enable it:

```bash
mkdir -p ~/.config/systemd/user
cp deploy/user-services/uditbot-orphan-oco-cleanup.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now uditbot-orphan-oco-cleanup.timer
systemctl --user status uditbot-orphan-oco-cleanup.timer
```

Runs as the VM user in `%h/uditbot`, using `venv/bin/python3` without
restarting or attaching to `trading_bot.service`. For 24/7 execution
while logged out, a working systemd user manager with lingering enabled is
required, which must be configured separately by the VM operator.

## Disable/remove without touching the trading bot

```bash
systemctl --user disable --now uditbot-orphan-oco-cleanup.timer
systemctl --user stop uditbot-orphan-oco-cleanup.service
rm -f ~/.config/systemd/user/uditbot-orphan-oco-cleanup.{service,timer}
systemctl --user daemon-reload
```

Do not remove the shared journal or any original bot source files.
This is a best-effort bounded safety worker, not proof of broker-side automatic
cancellation. Check the Forever book after any requested delete.
