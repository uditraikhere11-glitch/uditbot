"""Field-scoped, fail-closed WebUI configuration proposals.

This module does not execute broker actions, restart services, repair Telegram
sessions, or load credentials into a log. It also does not apply a proposal:
callers must use the durable ConfigChangeTracker coordinator.
"""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import math
import re
from typing import Any, Dict, Mapping, Optional, Tuple


class ConfigurationConflict(ValueError):
    """A form revision or existing file cannot be changed safely."""


class ConfigurationRejected(ValueError):
    """A requested field/value is not part of the supported WebUI schema."""


@dataclass(frozen=True)
class FieldSpec:
    key: str
    form_name: str
    value_type: str  # text, secret, integer, number, bool, enum, groups
    policy: str  # HOT_NOTIFICATION, FUTURE_ENTRY, SAFETY_CONTROLLED, NEXT_START
    consumer: str
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    choices: Tuple[str, ...] = ()
    secret: bool = False

    @property
    def broker_gate(self) -> bool:
        return self.policy == "SAFETY_CONTROLLED"

    @property
    def verifier(self) -> str:
        if self.policy == "HOT_NOTIFICATION":
            return "TELEGRAM_LISTENER_ACK"
        if self.policy == "SAFETY_CONTROLLED":
            return "SYSTEMD_PID_CONFIG_VERSION_AND_BROKER_SAFETY"
        return "NEXT_CONTROLLED_PROCESS_START"


# Each editable WebUI field is listed *once*. Existing order snapshots are
# immutable: FUTURE_ENTRY never rewrites a pending order or its protection legs.
_LAYOUT = (
    # form name, environment name, type, policy, consumer, min, max, choices
    ("is_live_mode", "TRADING_MODE", "mode", "SAFETY_CONTROLLED", "trading_mode", None, None, ("TEST", "LIVE")),
    ("equity_active", "EQUITY_ACTIVE", "bool", "SAFETY_CONTROLLED", "entry_gate", None, None, ()),
    ("fno_active", "FNO_ACTIVE", "bool", "SAFETY_CONTROLLED", "entry_gate", None, None, ()),
    ("operator_name", "OPERATOR_NAME", "text", "NEXT_START", "operator_identity", None, None, ()),
    ("instance_name", "BOT_INSTANCE_NAME", "text", "NEXT_START", "operator_identity", None, None, ()),
    ("tg_api_id", "TG_API_ID", "integer", "SAFETY_CONTROLLED", "telegram_auth", 0, None, ()),
    ("tg_api_hash", "TG_API_HASH", "secret", "SAFETY_CONTROLLED", "telegram_auth", None, None, ()),
    ("monitored_channel_ids", "MONITORED_CHANNEL_IDS", "groups", "SAFETY_CONTROLLED", "telegram_listener", None, None, ()),
    ("log_group_id", "LOG_GROUP_ID", "integer", "NEXT_START", "telegram_notifier", None, None, ()),
    ("dhan_response_group_name", "DHAN_RESPONSE_GROUP_NAME", "response", "NEXT_START", "telegram_listener", None, None, ()),
    ("test_group_id", "TEST_GROUP_ID", "integer", "SAFETY_CONTROLLED", "telegram_listener", None, None, ()),
    ("ignore_backlog", "IGNORE_TELEGRAM_BACKLOG_ON_RESTART", "bool", "NEXT_START", "telegram_listener", None, None, ()),
    ("tg_bot_token", "TG_BOT_TOKEN", "secret", "NEXT_START", "telegram_notifier", None, None, ()),
    ("dhan_client_id", "DHAN_CLIENT_ID", "text", "SAFETY_CONTROLLED", "dhan_account", None, None, ()),
    ("dhan_pin", "DHAN_PIN", "secret", "SAFETY_CONTROLLED", "dhan_auth", None, None, ()),
    ("dhan_totp_secret", "DHAN_TOTP_SECRET", "secret", "SAFETY_CONTROLLED", "dhan_auth", None, None, ()),
    ("equity_budget_inr", "EQUITY_BUDGET_INR", "number", "FUTURE_ENTRY", "sizer", 0.01, None, ()),
    ("equity_product_type", "EQUITY_PRODUCT_TYPE", "enum", "FUTURE_ENTRY", "equity_order", None, None, ("CNC", "MTF")),
    ("equity_price_markup_pct", "EQUITY_PRICE_MARKUP_PCT", "number", "FUTURE_ENTRY", "equity_order", 0, None, ()),
    ("equity_cmp_execution_policy", "EQUITY_CMP_EXECUTION_POLICY", "enum", "FUTURE_ENTRY", "equity_order", None, None, ("STRICT_TIMESTAMP", "DARVAS_HTTP_SUCCESS")),
    ("equity_sizing_mode", "EQUITY_SIZING_MODE", "enum", "FUTURE_ENTRY", "sizer", None, None, ("BUDGET", "FIXED_QTY")),
    ("equity_fixed_qty", "EQUITY_FIXED_QTY", "integer", "FUTURE_ENTRY", "sizer", 1, None, ()),
    ("fno_capital_per_trade", "FNO_CAPITAL_PER_TRADE", "number", "FUTURE_ENTRY", "sizer", 0.01, None, ()),
    ("fno_max_lots", "FNO_MAX_LOTS", "integer", "FUTURE_ENTRY", "sizer", 1, None, ()),
    ("fno_sizing_mode", "FNO_SIZING_MODE", "enum", "FUTURE_ENTRY", "sizer", None, None, ("BUDGET", "FIXED_LOTS")),
    ("fno_fixed_lots", "FNO_FIXED_LOTS", "integer", "FUTURE_ENTRY", "sizer", 1, None, ()),
    ("allow_next_month_expiry_fallback", "ALLOW_NEXT_MONTH_EXPIRY_FALLBACK", "bool", "FUTURE_ENTRY", "resolver", None, None, ()),
    ("max_order_amount_inr", "MAX_ORDER_AMOUNT_INR", "number", "FUTURE_ENTRY", "risk", 0, None, ()),
    ("min_trade_funds_inr", "MIN_TRADE_FUNDS_INR", "number", "FUTURE_ENTRY", "risk", 0, None, ()),
    ("test_mode_paper_funds_inr", "TEST_MODE_PAPER_FUNDS_INR", "number", "FUTURE_ENTRY", "test_ledger", 0, None, ()),
    ("max_daily_loss_inr", "MAX_DAILY_LOSS_INR", "number", "FUTURE_ENTRY", "risk", 0, None, ()),
    ("max_daily_trades", "MAX_DAILY_TRADES", "integer", "FUTURE_ENTRY", "risk", 0, None, ()),
    ("max_total_exposure_inr", "MAX_TOTAL_EXPOSURE_INR", "number", "FUTURE_ENTRY", "risk", 0, None, ()),
    ("max_open_positions", "MAX_OPEN_POSITIONS", "integer", "FUTURE_ENTRY", "risk", 0, None, ()),
    ("max_drawdown_pct", "MAX_DRAWDOWN_PCT", "number", "FUTURE_ENTRY", "risk", 0, 100, ()),
    ("insufficient_funds_policy", "INSUFFICIENT_FUNDS_POLICY", "enum", "FUTURE_ENTRY", "risk", None, None, ("HARD_REJECT", "SCALE_DOWN")),
    ("equity_sl_pct", "EQUITY_SL_PCT", "number", "FUTURE_ENTRY", "new_order_protection", 0.000001, 50, ()),
    ("equity_target_pct", "EQUITY_TARGET_PCT", "number", "FUTURE_ENTRY", "new_order_protection", 0.000001, 100, ()),
    ("fno_sl_pct", "FNO_SL_PCT", "number", "FUTURE_ENTRY", "new_order_protection", 0.000001, 50, ()),
    ("sl_limit_buffer_pct", "SL_LIMIT_BUFFER_PCT", "number", "FUTURE_ENTRY", "new_order_protection", 0, None, ()),
    ("fno_target_pct", "FNO_TARGET_PCT", "number", "FUTURE_ENTRY", "new_order_protection", 0.000001, 100, ()),
    ("sl_pct", "SL_PCT", "number", "FUTURE_ENTRY", "legacy_protection", 0.000001, 50, ()),
    ("target_pct", "TARGET_PCT", "number", "FUTURE_ENTRY", "legacy_protection", 0.000001, 100, ()),
    ("max_message_age_sec", "MAX_MESSAGE_AGE_SEC", "number", "FUTURE_ENTRY", "telegram_parser", 0, None, ()),
    ("dedup_window_sec", "DEDUP_WINDOW_SEC", "number", "FUTURE_ENTRY", "duplicate_filter", 0, None, ()),
    ("max_quote_age_sec", "MAX_QUOTE_AGE_SEC", "number", "FUTURE_ENTRY", "quote_filter", 0, None, ()),
    ("order_timeout_sec", "ORDER_TIMEOUT_SEC", "number", "FUTURE_ENTRY", "watcher", 0, None, ()),
    ("partial_fill_retry_timeout_sec", "PARTIAL_FILL_RETRY_TIMEOUT_SEC", "number", "FUTURE_ENTRY", "watcher", 0, None, ()),
    ("scrip_master_max_age_sec", "SCRIP_MASTER_MAX_AGE_SEC", "number", "NEXT_START", "resolver", 0, None, ()),
    ("equity_amo_enabled", "EQUITY_AMO_ENABLED", "bool", "SAFETY_CONTROLLED", "equity_amo", None, None, ()),
    ("fno_amo_enabled", "FNO_AMO_ENABLED", "bool", "SAFETY_CONTROLLED", "fno_amo", None, None, ()),
)

REGISTRY = {name: FieldSpec(key, name, kind, policy, consumer, minimum, maximum, tuple(choices), kind == "secret")
            for name, key, kind, policy, consumer, minimum, maximum, choices in _LAYOUT}
ENV_REGISTRY = {spec.key: spec for spec in REGISTRY.values()}
assert len(REGISTRY) == len(ENV_REGISTRY) == len(_LAYOUT) == 51, "Duplicate or missing WebUI setting definition"

# Protocol/control fields are never configuration settings. A submitted unknown
# field is an error, rather than an excuse to guess a default.
REQUEST_METADATA = frozenset({
    "csrf_token", "sync_mode", "spawn_worker", "expected_revision", "request_id", "confirm_live", "full_setup_form",
})
KEEP_BLANK_SECRETS = frozenset({"TG_API_HASH", "TG_BOT_TOKEN", "DHAN_PIN", "DHAN_TOTP_SECRET"})
# Supported pre-registry Setup API alias, not a second independent setting.
LEGACY_FORM_ALIASES = {"daily_trade_limit": "max_daily_trades"}
_LINE = re.compile(r"^(?P<lead>[ \t]*)(?P<key>[A-Za-z_][A-Za-z0-9_]*)(?P<space>[ \t]*)=(?P<rhs>.*)$")
_ID = re.compile(r"^[A-Za-z0-9_-]{8,100}$")


def revision(content: str) -> str:
    return sha256(content.encode("utf-8")).hexdigest()


def parse_env_strict(content: str) -> Tuple[Dict[str, str], Dict[str, int]]:
    """Preserve unrecognized lines; reject *all* ambiguous assignments."""
    if not isinstance(content, str) or "\x00" in content:
        raise ConfigurationConflict("Configuration contains invalid bytes")
    if "\r" in content.replace("\r\n", ""):
        raise ConfigurationConflict("Unsupported configuration line endings")
    values, indexes = {}, {}
    for i, raw in enumerate(content.splitlines(keepends=True)):
        line = raw.rstrip("\r\n")
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        m = _LINE.match(line)
        if not m:
            # Preserve extensions, but reject suspicious key assignments.
            if "=" in line:
                raise ConfigurationConflict(f"Invalid assignment syntax at line {i + 1}")
            continue
        key = m.group("key").upper()
        # Existing runtime parsers are case-sensitive. An editable lowercase
        # .env assignment would be silently ignored by the running process.
        if key in ENV_REGISTRY and m.group("key") != key:
            raise ConfigurationConflict(f"Non-canonical configuration key {key} at line {i + 1}; correct casing before updating")
        if key in indexes:
            raise ConfigurationConflict(f"Duplicate configuration key {key} at line {i + 1}")
        rhs = m.group("rhs").strip()
        if len(rhs) >= 2 and rhs[0] == rhs[-1] and rhs[0] in ("'", '"'):
            val = rhs[1:-1]
        else:
            val = rhs.split("#", 1)[0].strip()
        indexes[key] = i
        values[key] = val
    return values, indexes


def normalize(spec: FieldSpec, input_value: Any) -> str:
    value = str(input_value if input_value is not None else "").strip()
    if len(value) > 4096 or any(ch in value for ch in "\x00\r\n"):
        raise ConfigurationRejected(f"Invalid characters or excessive length in {spec.key}")
    if spec.value_type == "mode":
        if isinstance(input_value, bool):
            return "LIVE" if input_value else "TEST"
        if value.lower() in ("true", "false", "1", "0", "yes", "no", "on", "off"):
            return "LIVE" if value.lower() in ("true", "1", "yes", "on") else "TEST"
        if value.upper() not in ("LIVE", "TEST"):
            raise ConfigurationRejected("Invalid LIVE/TEST selection")
        return value.upper()
    if spec.value_type == "bool":
        if isinstance(input_value, bool):
            return "true" if input_value else "false"
        if value.lower() not in ("true", "false", "1", "0", "yes", "no", "on", "off"):
            raise ConfigurationRejected(f"{spec.key} requires true or false")
        return "true" if value.lower() in ("true", "1", "yes", "on") else "false"
    if spec.value_type == "enum":
        if value.upper() not in spec.choices:
            raise ConfigurationRejected(f"Unsupported {spec.key} value")
        return value.upper()
    if spec.value_type == "integer":
        if not re.fullmatch(r"-?[0-9]+", value):
            raise ConfigurationRejected(f"{spec.key} requires a whole number")
        n = int(value)
        if spec.min_value is not None and n < spec.min_value or spec.max_value is not None and n > spec.max_value:
            raise ConfigurationRejected(f"{spec.key} is outside the permitted range")
        return str(n)
    if spec.value_type == "number":
        try:
            if isinstance(input_value, bool):
                raise ValueError("Boolean not permitted")
            n = float(value)
        except (ValueError, TypeError):
            raise ConfigurationRejected(f"{spec.key} requires a finite number") from None
        if not math.isfinite(n) or (spec.min_value is not None and n < spec.min_value) or (spec.max_value is not None and n > spec.max_value):
            raise ConfigurationRejected(f"{spec.key} is outside the permitted range")
        return format(n, ".15g")
    if spec.value_type == "groups":
        if not value:
            return ""
        chunks = [v.strip() for v in value.split(",")]
        if not all(re.fullmatch(r"-?[0-9]+", v) for v in chunks):
            raise ConfigurationRejected("Monitored channel IDs must be comma-separated integers")
        return ",".join(str(int(v)) for v in chunks)
    if spec.value_type == "response":
        from src.notifier.dhan_response import normalize_group_name
        return normalize_group_name(value)
    if "#" in value or "=" in value and spec.value_type != "secret":
        raise ConfigurationRejected(f"Unsafe characters in {spec.key}")
    if spec.value_type == "text" and len(value) > 200:
        raise ConfigurationRejected(f"{spec.key} must not exceed 200 characters")
    return value


def _default_value(spec: FieldSpec) -> str:
    from src.config import AppConfig
    from enum import Enum
    cfg = AppConfig()
    names = {
        "BOT_INSTANCE_NAME": "instance_name", "IGNORE_TELEGRAM_BACKLOG_ON_RESTART": "ignore_backlog_on_restart",
        "TRADING_MODE": "trading_mode",
    }
    attr = names.get(spec.key, spec.key.lower())
    value = getattr(cfg, attr)
    if isinstance(value, Enum):
        value = value.value
    if isinstance(value, list):
        value = ",".join(str(x) for x in value)
    return normalize(spec, value)


def _render_value(spec: FieldSpec, value: str) -> str:
    # Normalization rejects multiline syntax. Quoted secret values may contain
    # equals signs; a literal quote requires a quoted .env parser not available
    # in the existing app, so disallow it rather than changing semantics.
    if spec.secret and any(x in value for x in ("#", "'", '"')):
        raise ConfigurationRejected(f"{spec.key} contains unsupported quoting characters")
    return value


def patch_env(content: str, requested: Mapping[str, Any], *, expected_revision: Optional[str] = None) -> Tuple[str, Dict[str, Tuple[str, str]]]:
    """Return exact field-scoped content and effective change summary.

    No disk writes, no side effects. All unchanged/unknown lines remain byte-for-byte
    untouched. Call under the coordinator's cross-process operation lock.
    """
    if expected_revision is not None and expected_revision != revision(content):
        raise ConfigurationConflict("Configuration changed since this page loaded; refresh and review before saving")
    old, positions = parse_env_strict(content)
    proposed: Dict[str, str] = {}
    for form_key, raw in requested.items():
        if form_key in REQUEST_METADATA:
            continue
        if form_key == "dhan_access_token":
            if str(raw or "").strip():
                raise ConfigurationRejected("Dhan access token is managed exclusively by the token coordinator")
            continue
        spec = REGISTRY.get(LEGACY_FORM_ALIASES.get(form_key, form_key))
        if spec is None:
            raise ConfigurationRejected(f"Unsupported WebUI field: {form_key}")
        if spec.key in proposed:
            raise ConfigurationRejected(f"Conflicting submissions for {spec.key}")
        if spec.key in KEEP_BLANK_SECRETS and not str(raw or "").strip():
            continue
        proposed[spec.key] = normalize(spec, raw)

    changed = {}
    for key, new_val in proposed.items():
        spec = ENV_REGISTRY[key]
        old_val = old.get(key)
        if old_val is None:
            old_val = _default_value(spec)
        else:
            # Do not silently overwrite old invalid values; operator can fix
            # them explicitly only by supplying a valid replacement.
            try:
                old_val = normalize(spec, old_val)
            except ConfigurationRejected:
                old_val = old[key]
        if old_val != new_val:
            changed[key] = (old_val, new_val)

    if not changed:
        return content, {}

    lines = content.splitlines(keepends=True)
    nl = "\r\n" if "\r\n" in content else "\n"
    for key, (_, new_val) in changed.items():
        rendered = _render_value(ENV_REGISTRY[key], new_val)
        if key in positions:
            i = positions[key]
            old_line = lines[i]
            ends = "\r\n" if old_line.endswith("\r\n") else ("\n" if old_line.endswith("\n") else "")
            m = _LINE.match(old_line.rstrip("\r\n"))
            if not m:
                raise ConfigurationConflict("Configuration changed during patch planning")
            # Preserve left-hand spacing; intentionally discard inline comment
            # only for the changed field to prevent a comment becoming a value.
            lines[i] = f'{m.group("lead")}{m.group("key")}{m.group("space")}={rendered}{ends}'
        else:
            if lines and not lines[-1].endswith(("\n", "\r")):
                lines[-1] += nl
            lines.append(f"{key}={rendered}{nl}")
    new_content = "".join(lines)
    actual, _ = parse_env_strict(new_content)
    for key in old:
        if key not in changed and old.get(key) != actual.get(key):
            raise ConfigurationConflict("Unrelated configuration value would change")
    return new_content, changed


def validate_effective_config(content: str) -> None:
    """Use the actual AppConfig schema, not fallback parser guesses."""
    from src.config import AppConfig
    parsed, _ = parse_env_strict(content)
    # AppConfig validates cross-field allowlist overlap, percentages and enums.
    for key, raw in parsed.items():
        if key in ENV_REGISTRY:
            normalize(ENV_REGISTRY[key], raw)
    AppConfig.from_env(env=parsed, env_file="")


def classify(changed: Mapping[str, Any]) -> str:
    """Most restrictive category dominates a multi-field request."""
    policies = {ENV_REGISTRY[key].policy for key in changed}
    for priority in ("SAFETY_CONTROLLED", "NEXT_START", "FUTURE_ENTRY", "HOT_NOTIFICATION"):
        if priority in policies:
            return priority
    return "UNCHANGED"


def redact(key: str, value: str) -> str:
    spec = ENV_REGISTRY.get(key)
    if spec is None or spec.secret or any(s in key.lower() for s in ("password", "token", "secret", "pin", "hash", "session")):
        return "[REDACTED]"
    return value


def request_id_valid(request_id: str) -> bool:
    return bool(_ID.fullmatch(str(request_id)))
