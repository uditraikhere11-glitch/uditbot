"""Operator-configurable Telegram notification presentation.

Strictly presentation-only: reads a versioned JSON settings file. No broker,
risk, watcher, or trading configuration changes. Default is disabled to preserve
existing Telegram messages byte-for-byte until the operator opts in.
"""
from __future__ import annotations

from html import escape
import json
import logging
import os
import tempfile
from typing import Any, Mapping

LOGGER = logging.getLogger(__name__)
FILENAME = "telegram_message_designer.json"

FIELDS = {
    "symbol": "Symbol",
    "contract": "Option Contract",
    "expiry": "Expiry",
    "quantity": "Quantity",
    "lots": "Lots",
    "limit_price": "Limit Price",
    "order_id": "Dhan Order ID",
    "broker_status": "Dhan Status",
    "source": "Signal Source",
    "segment": "Segment",
    "action": "Action",
    "mode": "Mode",
    "signal_time": "Signal Time",
    "response_ms": "Dhan Response Time",
    "filled_qty": "Filled Quantity",
    "fill_price": "Execution Price",
    "trade_value": "Trade Value",
    "protection": "Protection",
    "instance": "Bot Instance",
    "event_time": "Report Time",
    "changed_keys": "Updated Settings",
    "channels": "Monitored Channels",
    "yahoo_cmp": "Yahoo CMP",
    "entry_price": "Entry Price",
    "exit_price": "Exit Price",
    "exit_trade_value": "Exit Trade Value",
    "realized_pnl": "Gross Realized P&L",
    "protection_route": "Protection Route",
    "sl_target": "SL / Target",
    "protected_qty": "Protected Quantity",
    "exit_filled": "Exit Filled",
    "quote_age": "Yahoo Quote Age",
    "exit_time": "Exit Observed",
}

EVENTS = {
    "equity_amo": {
        "title": "Dhan AMO Acknowledgement",
        "fields": ["symbol", "quantity", "limit_price", "broker_status"],
        "allowed": ["symbol", "quantity", "limit_price", "order_id", "broker_status",
                    "source", "mode", "signal_time", "response_ms", "segment"],
    },
    "fno_amo": {
        "title": "Dhan AMO Acknowledgement",
        "fields": ["contract", "quantity", "limit_price", "order_id", "broker_status"],
        "allowed": ["symbol", "contract", "expiry", "quantity", "lots", "limit_price",
                    "order_id", "broker_status", "source", "mode", "signal_time",
                    "response_ms", "segment"],
    },
    "submitted": {
        "title": "Trade Signal Processed",
        "fields": ["symbol", "action", "quantity", "limit_price"],
        "allowed": ["symbol", "contract", "expiry", "action", "segment", "quantity",
                    "lots", "limit_price", "source", "mode", "signal_time"],
    },
    "rejected": {
        "title": "Order Submission Result",
        "fields": ["symbol", "quantity", "limit_price", "source"],
        "allowed": ["symbol", "contract", "action", "segment", "quantity", "lots",
                    "limit_price", "source", "mode", "signal_time"],
    },
    "executed": {
        "title": "Order Execution Confirmed",
        "fields": ["symbol", "quantity", "filled_qty", "fill_price", "trade_value"],
        "allowed": ["symbol", "contract", "action", "segment", "quantity", "filled_qty",
                    "fill_price", "trade_value", "source", "mode", "signal_time",
                    "protection"],
    },
    "partial": {
        "title": "Partial Fill Update",
        "fields": ["symbol", "quantity", "filled_qty", "fill_price"],
        "allowed": ["symbol", "contract", "segment", "quantity", "filled_qty",
                    "fill_price", "trade_value", "source", "protection"],
    },
    "final": {
        "title": "Final Broker Order Result",
        "fields": ["symbol", "quantity", "filled_qty", "broker_status"],
        "allowed": ["symbol", "contract", "segment", "quantity", "filled_qty",
                    "broker_status", "fill_price", "trade_value", "source"],
    },
    "unverified": {
        "title": "Broker Verification Pending",
        "fields": ["symbol", "quantity", "broker_status"],
        "allowed": ["symbol", "contract", "segment", "quantity", "filled_qty",
                    "broker_status", "source"],
    },
    "settings_update": {
        "title": "Settings Update Confirmation",
        "fields": ["changed_keys"],
        "allowed": ["instance", "changed_keys", "mode"],
    },
    "protection_pending": {
        "title": "Protection Pending",
        "fields": ["symbol", "protection_route", "protected_qty", "sl_target"],
        "allowed": ["symbol", "protection_route", "protected_qty", "sl_target", "mode", "event_time"],
    },
    "protection_verified": {
        "title": "Protection Verified",
        "fields": ["symbol", "protection_route", "protected_qty", "sl_target"],
        "allowed": ["symbol", "protection_route", "protected_qty", "sl_target", "mode", "event_time"],
    },
    "protection_unverified": {
        "title": "Protection Unverified",
        "fields": ["symbol", "protection_route", "protected_qty", "sl_target"],
        "allowed": ["symbol", "protection_route", "protected_qty", "sl_target", "mode", "event_time"],
    },
    "protection_rejected": {
        "title": "Protection Rejected / Halted",
        "fields": ["symbol", "protection_route", "protected_qty", "sl_target"],
        "allowed": ["symbol", "protection_route", "protected_qty", "sl_target", "mode", "event_time"],
    },
    "target_triggered": {
        "title": "Target Leg Triggered",
        "fields": ["symbol", "action", "quantity", "entry_price", "protection_route", "sl_target", "exit_filled"],
        "allowed": ["symbol", "contract", "action", "segment", "quantity", "yahoo_cmp", "limit_price",
                    "entry_price", "protection_route", "sl_target", "exit_filled", "quote_age",
                    "source", "mode", "exit_time"],
    },
    "sl_triggered": {
        "title": "Stop-Loss Leg Triggered",
        "fields": ["symbol", "action", "quantity", "entry_price", "protection_route", "sl_target", "exit_filled"],
        "allowed": ["symbol", "contract", "action", "segment", "quantity", "yahoo_cmp", "limit_price",
                    "entry_price", "protection_route", "sl_target", "exit_filled", "quote_age",
                    "source", "mode", "exit_time"],
    },
    "exit_partial_filled": {
        "title": "Exit Partially Executed",
        "fields": ["symbol", "quantity", "entry_price", "exit_price", "exit_filled", "realized_pnl"],
        "allowed": ["symbol", "contract", "action", "segment", "quantity", "yahoo_cmp", "limit_price",
                    "entry_price", "exit_price", "exit_filled", "realized_pnl", "protection_route",
                    "sl_target", "quote_age", "source", "mode", "exit_time"],
    },
    "position_closed": {
        "title": "Position Closed",
        "fields": ["symbol", "quantity", "entry_price", "exit_price", "exit_trade_value", "realized_pnl"],
        "allowed": ["symbol", "contract", "action", "segment", "quantity", "yahoo_cmp", "limit_price",
                    "entry_price", "exit_price", "exit_trade_value", "realized_pnl", "protection_route",
                    "sl_target", "exit_filled", "quote_age", "source", "mode", "exit_time"],
    },
    "reconciliation_required": {
        "title": "Reconciliation Required",
        "fields": ["symbol", "quantity", "entry_price", "exit_filled"],
        "allowed": ["symbol", "contract", "action", "segment", "quantity", "entry_price",
                    "protection_route", "sl_target", "exit_filled", "source", "mode", "exit_time"],
    },
    "dhan_health": {
        "title": "AUDITBOT — 05:00 PM DHAN HEALTH CHECK",
        "fields": [],
        "allowed": [],
    },
    "startup": {
        "title": "AuditBot Startup",
        "fields": ["mode", "channels"],
        "allowed": ["instance", "mode", "channels"],
    },
}

LEGACY_TRADE_EVENTS = frozenset((
    "equity_amo", "fno_amo", "submitted", "rejected",
    "executed", "partial", "final", "unverified",
))
PROTECTION_EVENTS = frozenset((
    "protection_pending", "protection_verified", "protection_unverified", "protection_rejected",
))
EXIT_EVENTS = frozenset((
    "target_triggered", "sl_triggered", "exit_partial_filled", "position_closed", "reconciliation_required",
))
SYSTEM_EVENTS = frozenset(("settings_update", "dhan_health", "startup"))

EVENT_GROUPS = [
    {"label": "Entry & AMO", "events": ["equity_amo", "fno_amo", "submitted", "rejected", "executed", "partial", "final", "unverified"]},
    {"label": "Protection Lifecycle", "events": ["protection_pending", "protection_verified", "protection_unverified", "protection_rejected"]},
    {"label": "Exit & Closure", "events": ["target_triggered", "sl_triggered", "exit_partial_filled", "position_closed", "reconciliation_required"]},
    {"label": "System", "events": ["settings_update", "dhan_health", "startup"]},
]

EMOJIS = ["•", "🌙", "✅", "⏳", "⚠️", "📈", "📉", "📊", "📌", "🔔",
          "💰", "💵", "🎯", "🛡️", "🕒", "📅", "⚡", "📩", "🚀", "🔢", "🏷️", ""]

SAMPLE_VALUES = {
    "symbol": "HAL", "contract": "HAL 4750 CE", "expiry": "26 Nov 2026",
    "quantity": "75", "lots": "1", "limit_price": "₹120.45",
    "order_id": "123456789", "broker_status": "TRANSIT",
    "source": "Premium Signals", "segment": "F&O", "action": "BUY",
    "mode": "LIVE", "signal_time": "18:42 IST", "response_ms": "286 ms",
    "filled_qty": "75", "fill_price": "₹120.60", "trade_value": "₹9,045.00",
    "protection": "Pending verification",
    "instance": "AuditBot", "event_time": "22 Nov 2026, 17:00:14 IST",
    "changed_keys": "EQUITY_SL_PCT, EQUITY_TARGET_PCT", "channels": "2",
    "yahoo_cmp": "₹34.68", "entry_price": "₹34.69", "exit_price": "₹35.06",
    "exit_trade_value": "₹35.06", "realized_pnl": "+₹0.37",
    "protection_route": "FOREVER_OCO", "sl_target": "₹34.34 / ₹35.04",
    "protected_qty": "1/1", "exit_filled": "1/1",
    "quote_age": "4.17s (Accepted under configured policy)",
    "exit_time": "09:15:22 IST",
}


def _default_template(event: str) -> dict:
    definition = EVENTS[event]
    title_emoji = (
        "🌙" if event.endswith("amo")
        else "🕔" if event == "dhan_health"
        else "🛡️" if event in PROTECTION_EVENTS
        else "🎯" if event == "target_triggered"
        else "🛑" if event == "sl_triggered"
        else "🏁" if event == "position_closed"
        else "⚠️" if event == "reconciliation_required"
        else "📨"
    )
    status_emoji = (
        "✅" if event in ("equity_amo", "fno_amo", "executed", "protection_verified", "position_closed")
        else "🎯" if event == "target_triggered"
        else "🛑" if event == "sl_triggered"
        else "🚨" if event == "protection_rejected"
        else "⚠️" if event in ("unverified", "protection_unverified", "reconciliation_required")
        else "❌" if event in ("rejected", "final")
        else "⏳"
    )
    return {
        "enabled": False, "style": "standard", "title": definition["title"],
        "title_emoji": title_emoji,
        "status_emoji": status_emoji,
        "separator": "none", "footer": "",
        "fields": [{"key": key, "label": FIELDS[key], "emoji": "•"}
                   for key in definition["fields"]],
    }


def defaults() -> dict:
    return {"version": 1, "templates": {event: _default_template(event) for event in EVENTS}}


def _text(value: Any, *, max_len: int, name: str) -> str:
    if not isinstance(value, str) or len(value) > max_len:
        raise ValueError(name + " must be a short text value")
    if any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise ValueError(name + " contains control characters")
    return value.strip()


def validate_single_template(event: str, raw: Any) -> dict:
    """Validate and normalize one event template dict."""
    if event not in EVENTS:
        raise ValueError("Unsupported template event: " + str(event))
    definition = EVENTS[event]
    if not isinstance(raw, dict):
        raise ValueError("Invalid template: " + event)
    if type(raw.get("enabled")) is not bool:
        raise ValueError("enabled must be true or false")
    style = raw.get("style")
    separator = raw.get("separator")
    if style not in ("compact", "standard", "detailed") or separator not in ("none", "line", "dots"):
        raise ValueError("Unsupported layout style or separator")
    field_defs = raw.get("fields")
    if not isinstance(field_defs, list) or len(field_defs) > len(definition["allowed"]):
        raise ValueError("Invalid field selection")
    seen = set()
    fields = []
    for field in field_defs:
        if not isinstance(field, dict) or field.get("key") not in definition["allowed"]:
            raise ValueError("Unsupported field for " + event)
        key = field["key"]
        if key in seen:
            raise ValueError("Duplicate field: " + key)
        seen.add(key)
        fields.append({
            "key": key,
            "label": _text(field.get("label"), max_len=40, name="Field label"),
            "emoji": _text(field.get("emoji"), max_len=16, name="Field emoji"),
        })
    return {
        "enabled": raw["enabled"],
        "style": style, "separator": separator,
        "title": _text(raw.get("title"), max_len=85, name="Title"),
        "title_emoji": _text(raw.get("title_emoji"), max_len=16, name="Title emoji"),
        "status_emoji": _text(raw.get("status_emoji"), max_len=16, name="Status emoji"),
        "footer": _text(raw.get("footer"), max_len=240, name="Footer"),
        "fields": fields,
    }


def validate(data: Any) -> dict:
    """Validate entire payload and normalize to a bounded, known-fields schema."""
    if not isinstance(data, dict) or data.get("version") != 1:
        raise ValueError("Unsupported Telegram template schema version")
    source = data.get("templates")
    if (not isinstance(source, dict) or set(source) - set(EVENTS)
            or not LEGACY_TRADE_EVENTS.issubset(source)):
        raise ValueError("Template categories missing or unsupported")
    output = defaults()
    for event in EVENTS:
        if event not in source:
            # Older saved designer files contain only the 8 trade or 11 trade+system templates.
            # Preserve their customization and fill newly added categories as disabled.
            continue
        raw = source[event]
        # Backward compatibility: strip legacy dead fields from dhan_health if present in old saved files
        if event == "dhan_health" and isinstance(raw, dict) and isinstance(raw.get("fields"), list):
            raw = dict(raw, fields=[f for f in raw["fields"] if not (isinstance(f, dict) and f.get("key") in ("instance", "event_time", "mode"))])
        output["templates"][event] = validate_single_template(event, raw)
    return output


# No credential, account, Telegram destination, or free-form operator text
# is ever written to the public Telegram change history as a value.
_PRIVATE_SETTINGS = frozenset((
    "TG_API_ID", "LOG_GROUP_ID", "TEST_GROUP_ID", "DHAN_CLIENT_ID",
))
_CHANGE_LABELS = {
    "TRADING_MODE": "Trading Mode",
    "EQUITY_SL_PCT": "Equity SL", "EQUITY_TARGET_PCT": "Equity Target",
    "FNO_SL_PCT": "F&O SL", "FNO_TARGET_PCT": "F&O Target",
    "SL_PCT": "SL", "TARGET_PCT": "Target",
    "FNO_CAPITAL_PER_TRADE": "F&O Capital Per Trade",
    "EQUITY_BUDGET_INR": "Equity Budget",
    "EQUITY_AMO_ENABLED": "Equity AMO",
    "FNO_AMO_ENABLED": "F&O AMO",
    "DHAN_TOTP_SECRET": "Dhan TOTP Secret",
    "DHAN_PIN": "Dhan PIN",
    "TG_BOT_TOKEN": "Telegram Bot Token",
    "TG_API_HASH": "Telegram API Hash",
    "BOT_INSTANCE_NAME": "Bot Instance Name",
}


def format_changed_settings(changes: Mapping[str, tuple[str, str]]) -> list[str]:
    """Summarize the *effective, verified* before/after config diff safely.

    Names of every changed setting are retained; only schema-validated,
    non-sensitive scalar values may appear in Telegram. Never log either
    raw values or a redacted shadow of an access token.
    """
    from src.engine.config_patch import ENV_REGISTRY, ConfigurationRejected, normalize

    entries = []
    for key in sorted(changes):
        spec = ENV_REGISTRY.get(key)
        label = _CHANGE_LABELS.get(
            key,
            key.replace("_", " ").title().replace("Fno", "F&O").replace("Pct", "%"),
        )
        label = escape(label, quote=False)
        old_value, new_value = changes[key]
        allowed = bool(
            spec is not None and key not in _PRIVATE_SETTINGS
            and not spec.secret and spec.value_type in ("number", "integer", "bool", "mode", "enum")
        )
        if not allowed:
            entries.append("• " + label + ": Updated (values hidden)")
            continue
        try:
            before = normalize(spec, old_value)
            after = normalize(spec, new_value)
        except (ConfigurationRejected, ValueError, TypeError):
            entries.append("• " + label + ": Updated (values hidden)")
            continue
        if spec.value_type == "bool":
            before = "Enabled" if before == "true" else "Disabled"
            after = "Enabled" if after == "true" else "Disabled"
        elif key.endswith("_PCT"):
            before += "%"
            after += "%"
        entries.append(
            "• " + label + ": " + escape(before, quote=False)
            + " → " + escape(after, quote=False)
        )
    return [f"Changed Settings ({len(entries)}):", *entries]


def file_path(data_dir: str | None = None) -> str:
    base = data_dir or os.environ.get("DATA_DIR") or os.path.join(os.getcwd(), "data")
    return os.path.join(os.path.abspath(base), FILENAME)


def load(data_dir: str | None = None) -> dict:
    filename = file_path(data_dir)
    try:
        with open(filename, "r", encoding="utf-8") as handle:
            return validate(json.load(handle))
    except FileNotFoundError:
        return defaults()
    except (OSError, ValueError, TypeError) as exc:
        LOGGER.warning("Invalid notification designer settings; using defaults: %s", type(exc).__name__)
        return defaults()


def save(data: Any, data_dir: str | None = None) -> dict:
    cfg = validate(data)
    filename = file_path(data_dir)
    folder = os.path.dirname(filename)
    os.makedirs(folder, mode=0o700, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix=".msg-designer-", suffix=".tmp", dir=folder)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            os.fchmod(handle.fileno(), 0o600)
            json.dump(cfg, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, filename)
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
    return cfg


def render_with_template(
    event: str,
    values: Mapping[str, Any],
    required_lines: list[str],
    cfg: Mapping[str, Any],
) -> str | None:
    """Render using an already-validated template dict (shared by live render & server preview)."""
    if event not in EVENTS or not cfg or not cfg.get("enabled"):
        return None
    title = escape(cfg["title"], quote=False) or escape(EVENTS[event]["title"])
    icon = escape(cfg["title_emoji"], quote=False)
    heading = (icon + " " if icon else "") + "<b>" + title + "</b>"
    rendered = [heading]
    entries = []
    locked_lines = list(required_lines)
    for field in cfg["fields"]:
        key = field["key"]
        raw_value = values.get(key)
        if raw_value is None or raw_value == "":
            continue
        icon_text = escape(field["emoji"], quote=False)
        label = escape(field["label"], quote=False)
        value = escape(str(raw_value), quote=False)
        if event == "settings_update" and key == "changed_keys":
            for row_index, line in enumerate(locked_lines):
                if line.startswith("Changed Settings ("):
                    locked_lines[row_index] = (
                        ((icon_text + " ") if icon_text and icon_text != "•" else "")
                        + label + " " + line[len("Changed Settings "):]
                    )
                    break
            continue  # Always show the exact verified diff once, not a second list.
        if event == "startup" and key in ("mode", "channels"):
            # These are real rows in the immutable startup card, not extra
            # metadata rows. Change their presentation in place, preserving
            # the original, code-generated mode and listener evidence.
            prefix = "Mode: " if key == "mode" else "Listener: "
            for row_index, line in enumerate(locked_lines):
                if line.startswith(prefix):
                    locked_lines[row_index] = (
                        ((icon_text + " ") if icon_text and icon_text != "•" else "")
                        + label + ": " + line[len(prefix):]
                    )
                    break
            continue
        if event == "startup" and key == "instance" and value.casefold() in title.casefold():
            continue  # The bot name is already in the operator's heading.
        if event == "dhan_health" and key in ("instance", "event_time", "mode"):
            continue  # Report already carries these authoritative facts.
        entries.append(((icon_text + " ") if icon_text else "") + label + ": " + value)
    if cfg["style"] == "compact":
        rendered += [" | ".join(entries[i:i + 2]) for i in range(0, len(entries), 2)]
    else:
        rendered.extend(entries)
    if cfg["separator"] == "line":
        rendered.append("─────────────")
    elif cfg["separator"] == "dots":
        rendered.append("••••••••••••")
    if cfg["style"] == "detailed":
        rendered.append("")
    if cfg["footer"]:
        rendered.append(escape(cfg["footer"], quote=False))
    # Locked status always last. Replace only the code-owned leading status emoji.
    icon_status = escape(cfg["status_emoji"], quote=False)
    for i, line in enumerate(locked_lines):
        if event not in SYSTEM_EVENTS and i == 0 and any(
            line.startswith(icon + " ") for icon in ("✅", "❌", "⏳", "⚠️", "🎯", "🛑", "🏁", "🛡️", "🚨")
        ):
            _, rest = line.split(" ", 1)
            rendered.append((icon_status + " " if icon_status else "") + rest)
        else:
            rendered.append(line)
    return "\n".join(rendered)


def render(
    event: str,
    values: Mapping[str, Any],
    required_lines: list[str],
    data_dir: str | None = None,
    *,
    template_override: Mapping[str, Any] | None = None,
) -> str | None:
    """Format optional fields around caller-supplied *mandatory* status/safety lines.

    Returns None if the operator has not enabled customization for this type.
    Field values and operator-authored text are always HTML-escaped. Required
    strings are code-owned and cannot be changed or suppressed by the operator.
    """
    if event not in EVENTS:
        return None
    cfg = (
        validate_single_template(event, template_override)
        if template_override is not None
        else load(data_dir)["templates"][event]
    )
    return render_with_template(event, values, required_lines, cfg)


def render_event_preview(event: str, template_override: Mapping[str, Any] | None = None) -> str:
    """Single source of truth for WebUI Live Preview.

    Invokes the exact production formatter for `event` using canonical sample data
    (zero broker/Telegram I/O). When `template_override["enabled"]` is False,
    returns the exact default production message Telegram actually sends.
    """
    if event not in EVENTS:
        raise ValueError("Unsupported preview event: " + str(event))
    tpl = (
        validate_single_template(event, template_override)
        if template_override is not None
        else _default_template(event)
    )
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from src.notifier import execution_updates as eu

    ist = ZoneInfo("Asia/Kolkata")
    sig_ts = datetime(2026, 11, 23, 9, 15, 0, tzinfo=ist).timestamp()
    exec_ts = datetime(2026, 11, 23, 9, 15, 22, tzinfo=ist).timestamp()
    health_ts = datetime(2026, 11, 22, 17, 0, 14, tzinfo=ist)

    eq_meta = {
        "symbol": "VEDPOWER", "action": "BUY", "segment": "EQUITY", "mode": "LIVE",
        "source": "Received RDHS", "yahoo_cmp": 34.68, "limit_price": 34.85,
        "quote_age_sec": 4.17, "signal_received_at": sig_ts,
    }
    fno_meta = {
        "symbol": "HAL", "contract": "HAL 4750 CE", "expiry": "26 Nov 2026",
        "action": "BUY", "segment": "F&O", "mode": "LIVE", "lots": 1,
        "source": "Premium Signals", "limit_price": 120.45,
        "signal_received_at": sig_ts, "entry_only_amo": True,
    }

    if event == "equity_amo":
        return eu.format_amo_acknowledged(
            dict(eq_meta, order_id="123456789", response_ms=286), 1, "TRANSIT", template_override=tpl,
        )
    if event == "fno_amo":
        return eu.format_amo_acknowledged(
            dict(fno_meta, order_id="123456789", response_ms=286), 75, "TRANSIT", template_override=tpl,
        )
    if event == "submitted":
        return eu.format_order_submitted(eq_meta, 1, template_override=tpl)
    if event == "rejected":
        return eu.format_order_failed(eq_meta, 1, uncertain=False, template_override=tpl)
    if event == "unverified":
        return eu.format_order_failed(eq_meta, 1, uncertain=True, template_override=tpl)
    if event == "executed":
        return eu.format_order_outcome(
            eq_meta, 1, 1, "TRADED", exec_ts, 34.69, False,
            sl_price=34.34, target_price=35.04, protection_route="FOREVER_OCO",
            template_override=tpl,
        )
    if event == "partial":
        return eu.format_order_outcome(
            eq_meta, 10, 4, "PART_TRADED", exec_ts, 34.69, False,
            sl_price=34.34, target_price=35.04, protection_route="FOREVER_OCO",
            template_override=tpl,
        )
    if event == "final":
        return eu.format_order_outcome(
            eq_meta, 1, 0, "CANCELLED", exec_ts, None, False,
            template_override=tpl,
        )
    if event in PROTECTION_EVENTS:
        state_map = {
            "protection_pending": "PROTECTION_PENDING",
            "protection_verified": "PROTECTION_VERIFIED_ACTIVE",
            "protection_unverified": "PROTECTION_UNVERIFIED",
            "protection_rejected": "PROTECTION_REJECTED",
        }
        return eu.format_protection_lifecycle_card(
            eq_meta, state_map[event],
            protected_qty=1 if event == "protection_verified" else 0,
            total_qty=1, sl_price=34.34, target_price=35.04,
            protection_route="FOREVER_OCO", observed_at=exec_ts,
            reason="awaiting readback" if event == "protection_unverified" else "rejected by broker",
            template_override=tpl,
        )
    if event in EXIT_EVENTS:
        exit_map = {
            "target_triggered": ("TARGET_TRIGGERED", "TARGET_LEG", 0, None, None),
            "sl_triggered": ("SL_TRIGGERED", "STOP_LOSS_LEG", 0, None, None),
            "exit_partial_filled": ("EXIT_PARTIAL_FILLED", "TARGET_LEG", 1, 35.06, 0.37),
            "position_closed": ("POSITION_CLOSED", "TARGET_LEG", 1, 35.06, 0.37),
            "reconciliation_required": ("RECONCILIATION_REQUIRED", "TARGET_LEG", 0, None, None),
        }
        st_k, leg, s_qty, s_prc, pnl = exit_map[event]
        t_qty = 2 if event == "exit_partial_filled" else 1
        return eu.format_exit_lifecycle_card(
            eq_meta, st_k, triggered_leg=leg, sell_qty=s_qty, total_qty=t_qty,
            buy_vwap=34.69, sell_vwap=s_prc, realized_pnl=pnl,
            sl_price=34.34, target_price=35.04, protection_route="FOREVER_OCO",
            observed_at=exec_ts, template_override=tpl,
        )
    if event == "settings_update":
        lines = [
            "✅ <b>Settings Updated</b>",
            "Trading bot: Restarted",
            "Configuration: Verified",
            "Changed Settings (2):",
            "• Equity SL: 3% → 4%",
            "• F&amp;O Target: 3% → 5%",
        ]
        custom = render(
            "settings_update",
            {"instance": "AuditBot", "changed_keys": "Equity SL, F&O Target", "mode": "LIVE"},
            lines[1:], template_override=tpl,
        )
        return custom or "\n".join(lines)
    if event == "dhan_health":
        lines = [
            "🕔 <b>AUDITBOT — 05:00 PM DHAN HEALTH CHECK</b>",
            f"🤖 Instance: <b>Auditbot</b> | 🕒 {health_ts:%d %b %Y, %H:%M:%S} IST",
            "🔑 Dhan authentication: ✅ VERIFIED",
            "🔄 Token renewal: ✅ SUCCESS",
            "💳 Dhan account identity: ✅ VERIFIED (***1536)",
            "⏳ Token expiration: ✅ 2026-11-23 17:00:14 IST (Covers next session through Mon 23 Nov 15:35 IST)",
            "💰 Broker funds: ✅ VERIFIED — ₹949.21",
            "📚 Scrip master: ✅ OK",
            "⚙️ Trading mode: <b>LIVE</b>",
            "🛡️ Emergency STOP: ✅ INACTIVE",
            "🚦 Next trading session readiness: ✅ VERIFIED CHECKS",
            "📊 <b>Overall result: ✅ SUCCESS</b>",
        ]
        custom = render(
            "dhan_health",
            {"instance": "Auditbot", "event_time": health_ts.strftime("%d %b %Y, %H:%M IST"), "mode": "LIVE"},
            lines[1:], template_override=tpl,
        )
        return custom or "\n".join(lines)
    # startup
    lines = [
        "🟢 <b>Auditbot Online</b>",
        "Mode: LIVE",
        "Listener: Active 2 channels",
        "Reconciliation: Healthy",
        "Protection Watcher: Active and Verified",
        "New Entries Gate: Open",
    ]
    custom = render(
        "startup",
        {"instance": "Auditbot", "mode": "LIVE", "channels": "2"},
        lines[1:], template_override=tpl,
    )
    return custom or "\n".join(lines)
