"""Durable notification after an independently verified reconciliation recovery.

Notification-only: no broker calls, trading mutations, token refresh, restart,
STOP changes, or modifications to order/protection state. The reconciliation
pass has already verified broker books and mandatory safety invariants.
"""

from datetime import datetime, timedelta
from html import escape
import time
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")


def queue_verified_dhan_recovery(gate, *, now_ts=None) -> bool:
    """Queue one recovery notice after today's recorded 5 PM auth failure.

    Emits only after a previously blocked reconciliation gate transitions to
    verified healthy and the durable daily-health journal confirms AUTH_CHECK
    failed earlier that same IST day. Account identity and tomorrow's session
    readiness are deliberately not inferred from the gate being open.
    """
    db = getattr(gate, "db", None)
    broker = getattr(gate, "broker", None)
    cfg = getattr(broker, "cfg", None)
    stop = getattr(gate, "stop_manager", None)
    if not db or not cfg or not getattr(cfg, "log_group_id", None):
        return False
    if not getattr(gate, "is_accepting_entries", False):
        return False
    if stop is None or stop.is_stopped():
        return False
    if str(getattr(getattr(gate, "state", None), "value", "")).upper() != "HEALTHY":
        return False

    now = float(now_ts if now_ts is not None else time.time())
    today = datetime.fromtimestamp(now, IST)
    midnight = today.replace(hour=0, minute=0, second=0, microsecond=0)
    day_start = midnight.timestamp()
    day_end = (midnight + timedelta(days=1)).timestamp()

    # Auth failure must come from the scheduled Dhan health check, rather than
    # an unrelated reconciliation halt or an inferred current token failure.
    with db._get_conn() as conn:
        failed = conn.execute(
            "SELECT 1 FROM system_events WHERE event_type='DAILY_HEALTH_CHECK' "
            "AND created_at>=? AND created_at<? AND description LIKE '%AUTH_CHECK=FAIL%' LIMIT 1",
            (day_start, day_end),
        ).fetchone()
    if not failed:
        return False

    account = str(getattr(cfg, "dhan_client_id", "") or "")
    if not account:
        return False
    identity = (
        f"DAILY_HEALTH:{today:%Y-%m-%d}:{account}:"
        f"{getattr(cfg, 'instance_name', 'AuditBot')}"
    )
    # Report the verified market calendar without authorizing a future trade.
    from src.notifier.market_calendar_status import verified_market_calendar_lines
    tomorrow_line, next_session_line = verified_market_calendar_lines(now)
    card = (
        "🟢 <b>Auditbot — Automatic Recovery Confirmed</b>\n"
        f"🕒 {today:%d %b %Y, %H:%M:%S} IST\n"
        f"💳 Dhan account: ***{escape(account[-4:])}\n"
        "🔑 Dhan connectivity: ✅ RESTORED (authenticated broker reconciliation)\n"
        "🛡️ Reconciliation: ✅ HEALTHY\n"
        "🚦 New Entries Gate: ✅ OPEN\n"
        "📋 Earlier 5 PM authentication failure: RECOVERED\n"
        f"{tomorrow_line}\n"
        f"{next_session_line}\n"
        "⏳ Next-session readiness: Not independently verified by this notice"
    )
    # Atomic durable claim + enqueue prevents duplicate recovery cards across
    # processes and survives Telegram API failure/restart. Never claim delivery.
    return bool(db.enqueue_order_notification_once(
        identity, "DAILY_HEALTH_1700_RECOVERY", card
    ))
