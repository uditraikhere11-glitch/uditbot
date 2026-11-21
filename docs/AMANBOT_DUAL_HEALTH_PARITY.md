# AmanBot daily-health parity (opt-in, separate from UditBot)

## Goal
Preserve AmanBot's existing **08:00 IST Monday-Friday** scheduled Dhan health message
and add UditBot's **17:00 IST every calendar day** scheduled Dhan TOTP renewal and
Telegram confirmation. Never copy UditBot's .env, credentials, Dhan client ID,
token file, Telegram group ID, database, STOP, or trading settings.

This is an AmanBot-only **VM timer choice**. UditBot keeps its existing single
17:00 timer.

## How this works
- AmanBot morning health uses `auditbot-morning-health.service` and
  `auditbot-morning-health.timer`, with environment variable
  `AUDITBOT_HEALTH_SLOT=AM_0800`.
- The original `dhan-token-refresh.timer` is upgraded with the already-reviewed
  `deploy/install_vm_services.sh --upgrade-timer` to 17:00 daily. The existing
  `dhan-token-refresh.service` is preserved and continues invoking
  `./bot daily_health_check`.
- The new slot marker changes **only** the morning Telegram heading, notification
  deduplication identity and local journal event type. The regular 17:00
  identity remains byte-for-byte compatible with UditBot and the existing
  PR #116 recovery alert.
- Both scheduled runs execute the existing health-check command, which includes a
  broker TOTP renewal attempt. The 08:00 renewal was already the existing AmanBot
  behavior. Both the morning and evening runs use the same shared token coordinator
  and broker identity verification.
- Both reports go to *AmanBot's* configured `LOG_GROUP_ID` via its
  `TG_BOT_TOKEN`. The installer requires these values to be configured
  but never prints them.
- 08:00 failures are recorded as `MORNING_HEALTH_CHECK`, so the 5 PM recovery
  notifier cannot incorrectly claim an earlier 5 PM incident from an 08:00 failure.
- No separate Market Calendar section is added. Existing holiday-aware
  readiness behavior remains in the same compact health card.

## Before deployment
This branch is a candidate. Review and run offline tests, perform independent
GitHub audit and merge only with owner approval.

1. Use the **AmanBot VM only**, as the non-root AmanBot Linux owner.
2. Confirm `./bot dashboard` shows the expected masked Aman Dhan account and
   that `LOG_GROUP_ID`, `TG_BOT_TOKEN` are configured locally.
3. Check the effective 08:00 existing timer and the refresh service point to
   the active checkout. Do not overwrite a customized service.
4. Verify zero in-flight/unknown orders and pass the controlled restart safety
   procedure separately before restarting the long-running trading service
   to load the new recovery notification code.

## Preview only
```bash
cd ~/uditbot
bash deploy/amanbot_dual_health_schedule.sh --plan
```

## Apply after PR merge and VM update (changes timers only)
```bash
cd ~/uditbot
bash deploy/amanbot_dual_health_schedule.sh --apply
systemctl list-timers --all dhan-token-refresh.timer auditbot-morning-health.timer --no-pager
```

This invokes systemd to reload unit files and restarts **only the original
timer**, not `trading_bot.service`, `auditbot-web.service` or the broker
health-check service. `Persistent=false` prevents automatically replaying a
missed morning/evening token renewal.

## Rollback
```bash
cd ~/uditbot
bash deploy/amanbot_dual_health_schedule.sh --rollback
```

Rollback restores the original backed-up 08:00 timer and removes only morning
units with the expected managed marker. It does not change any bot settings
or restart trading.

## Verification
Run the offline tests in a temporary/isolation environment:
```bash
AUDITBOT_TEST_ISOLATION=1 python3 -m unittest -v \
  tests.test_amanbot_dual_health_parity \
  tests.test_dhan_health_auto_recovery \
  tests.test_dhan_1700_ist_final \
  tests.test_health_and_system_message_designer
bash -n deploy/amanbot_dual_health_schedule.sh
```

**Do not manually start `dhan-token-refresh.service` merely to test the
message:** doing so makes a real TOTP authentication request.

A successful scheduled health task can enqueue a Telegram alert; this does not
prove delivery until the configured sender/worker confirms the Telegram API.
Check the scheduled-service journal and the log group after the first run.

**No scheduled task or notification can be activated by `git pull` alone.**
The systemd installer must be applied and any long-running Python process must
load the changed code by a separately approved safe restart.
