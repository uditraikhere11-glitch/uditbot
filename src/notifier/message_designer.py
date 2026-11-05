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
}

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
}


def _default_template(event: str) -> dict:
    definition = EVENTS[event]
    return {
        "enabled": False, "style": "standard", "title": definition["title"],
        "title_emoji": "🌙" if event.endswith("amo") else "📨",
        "status_emoji": "✅" if event in ("equity_amo", "fno_amo", "executed") else "⏳",
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


def validate(data: Any) -> dict:
    """Validate entire payload and normalize to a bounded, known-fields schema."""
    if not isinstance(data, dict) or data.get("version") != 1:
        raise ValueError("Unsupported Telegram template schema version")
    source = data.get("templates")
    if not isinstance(source, dict) or set(source) != set(EVENTS):
        raise ValueError("Template categories missing or unsupported")
    output = defaults()
    for event, definition in EVENTS.items():
        raw = source[event]
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
        output["templates"][event] = {
            "enabled": raw["enabled"],
            "style": style, "separator": separator,
            "title": _text(raw.get("title"), max_len=85, name="Title"),
            "title_emoji": _text(raw.get("title_emoji"), max_len=16, name="Title emoji"),
            "status_emoji": _text(raw.get("status_emoji"), max_len=16, name="Status emoji"),
            "footer": _text(raw.get("footer"), max_len=240, name="Footer"),
            "fields": fields,
        }
    return output


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


def render(event: str, values: Mapping[str, Any], required_lines: list[str],
           data_dir: str | None = None) -> str | None:
    """Format optional fields around caller-supplied *mandatory* status/safety lines.

    Returns None if the operator has not enabled customization for this type.
    Field values and operator-authored text are always HTML-escaped. Required
    strings are code-owned and cannot be changed or suppressed by the operator.
    """
    if event not in EVENTS:
        return None
    cfg = load(data_dir)["templates"][event]
    if not cfg["enabled"]:
        return None
    title = escape(cfg["title"], quote=False) or escape(EVENTS[event]["title"])
    icon = escape(cfg["title_emoji"], quote=False)
    heading = (icon + " " if icon else "") + "<b>" + title + "</b>"
    rendered = [heading]
    entries = []
    for field in cfg["fields"]:
        raw_value = values.get(field["key"])
        if raw_value is None or raw_value == "":
            continue
        icon_text = escape(field["emoji"], quote=False)
        label = escape(field["label"], quote=False)
        value = escape(str(raw_value), quote=False)
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
    for i, line in enumerate(required_lines):
        if i == 0 and line[:2] in ("✅ ", "❌ ", "⏳ ", "⚠️ "):
            prefix, rest = line.split(" ", 1)
            rendered.append((icon_status + " " if icon_status else "") + rest)
        else:
            rendered.append(line)
    return "\n".join(rendered)
