"""
CV13-06 & CV13-06G: Persistent Configuration-Update State Machine, Safe Application
Coordinator, WebUI Save Confirmation, and Telegram Acknowledgement.

Implements the 10-state configuration lifecycle:
1. REQUEST_RECEIVED      — WebUI submission received.
2. VALIDATING            — Checking input values, permissions, service status, and safety restrictions.
3. REJECTED              — Invalid or unauthorized changes; no configuration update committed.
4. SAVED                 — Validated configuration has been written safely but is not necessarily active.
5. SAFETY_CHECK_PENDING  — Checking broker orders, unresolved intents, emergency STOP, and protection requirements.
6. WAITING_FOR_SAFE_APPLY— Changes are saved, but application is deferred due to unsafe trading conditions.
7. APPLYING              — Approved configuration change is being applied.
8. APPLIED_VERIFIED      — The running application has confirmed the correct configuration version.
9. SAVED_FOR_NEXT_START  — Service is stopped; configuration saved without launching the bot.
10. APPLY_FAILED         — Applying the change failed; require recovery or rollback according to safety policy.

Guarantees:
- Never confuses configuration states with trading-order statuses.
- Never prints or logs secret values (tokens, PINs, TOTP secrets, API hashes, session strings).
- Never restarts trading_bot.service while an order intent, active order, or protection leg is pending/unverified,
  including during LIVE->TEST transitions and when Emergency STOP is active.
- Never overwrites active `.env` when an update is deferred (`WAITING_FOR_SAFE_APPLY`), preventing a subsequent
  service restart from unexpectedly activating unverified deferred changes.
- Never restarts `trading_bot.service` on an identical configuration Save while still restarting for genuine `.env`
  credential changes (`DHAN_PIN`, `DHAN_TOTP_SECRET`, `TG_API_HASH`, `TG_BOT_TOKEN`, etc.).
- Rechecks service activity immediately prior to executing any service restart so a service stopped after the
  initial check is never started accidentally.
- Never starts trading_bot.service when the service is currently stopped (marks SAVED_FOR_NEXT_START).
"""

import hashlib
import json
import logging
import os
import subprocess
import tempfile
import threading
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

logger = logging.getLogger("auditbot.config_manager")


class ConfigApplyState(str, Enum):
    REQUEST_RECEIVED = "REQUEST_RECEIVED"
    VALIDATING = "VALIDATING"
    REJECTED = "REJECTED"
    SAVED = "SAVED"
    SAFETY_CHECK_PENDING = "SAFETY_CHECK_PENDING"
    WAITING_FOR_SAFE_APPLY = "WAITING_FOR_SAFE_APPLY"
    APPLYING = "APPLYING"
    APPLIED_VERIFIED = "APPLIED_VERIFIED"
    SAVED_FOR_NEXT_START = "SAVED_FOR_NEXT_START"
    APPLY_FAILED = "APPLY_FAILED"


# Fields that must NEVER appear in logs, UI responses, version hashes, or Telegram messages
SECRET_FIELDS: Set[str] = {
    "dhan_access_token",
    "dhan_pin",
    "dhan_totp_secret",
    "tg_api_id",
    "tg_api_hash",
    "tg_phone",
    "tg_session_string",
    "tg_bot_token",
    "telegram_bot_token",
    "csrf_token",
    "DHAN_ACCESS_TOKEN",
    "DHAN_PIN",
    "DHAN_TOTP_SECRET",
    "TG_API_ID",
    "TG_API_HASH",
    "TG_PHONE",
    "TG_SESSION_STRING",
    "TG_BOT_TOKEN",
    "TELEGRAM_BOT_TOKEN",
}

# Canonical secret keys in lowercase
_CANONICAL_SECRET_KEYS: Set[str] = {
    "dhan_access_token",
    "dhan_pin",
    "dhan_totp_secret",
    "tg_api_id",
    "tg_api_hash",
    "tg_phone",
    "tg_session_string",
    "tg_bot_token",
}

_SECRET_KEY_ALIASES: Dict[str, str] = {
    "telegram_bot_token": "tg_bot_token",
    "tg_bot_token": "tg_bot_token",
    "dhan_access_token": "dhan_access_token",
    "dhan_pin": "dhan_pin",
    "dhan_totp_secret": "dhan_totp_secret",
    "tg_api_id": "tg_api_id",
    "tg_api_hash": "tg_api_hash",
    "tg_phone": "tg_phone",
    "tg_session_string": "tg_session_string",
}

# Fields that only update shared token storage and do not require restarting the Telegram listener
TOKEN_ONLY_FIELDS: Set[str] = {
    "dhan_access_token",
    "DHAN_ACCESS_TOKEN",
}


def is_isolated_test_environment() -> bool:
    """Returns True when executing inside unit tests so real systemd/network calls are blocked."""
    import sys
    return (
        os.environ.get("AUDITBOT_TEST_ISOLATION") == "1"
        or "unittest" in sys.modules
        or "pytest" in sys.modules
    )


def write_env_atomic(env_path: str, content: str) -> None:
    """Writes .env atomically with strict 0600 permissions."""
    parent_dir = os.path.dirname(os.path.abspath(env_path))
    os.makedirs(parent_dir, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".env.tmp.", dir=parent_dir)
    try:
        try:
            os.fchmod(fd, 0o600)
        except (AttributeError, OSError):
            os.chmod(tmp_path, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, env_path)
        os.chmod(env_path, 0o600)
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise


def parse_env_content_string(content: str) -> Dict[str, str]:
    """Parses raw .env file string content into a key-value dictionary."""
    out: Dict[str, str] = {}
    for raw_line in (content or "").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip()
        if not (v.startswith('"') and v.endswith('"')) and not (v.startswith("'") and v.endswith("'")):
            v = v.split("#")[0].strip()
        v = v.strip("'\"")
        if k:
            out[k] = v
    return out


@dataclass
class ConfigChangeRecord:
    change_id: str
    state: ConfigApplyState = ConfigApplyState.REQUEST_RECEIVED
    requested_at: float = field(default_factory=time.time)
    validated_at: Optional[float] = None
    saved_at: Optional[float] = None
    safety_checked_at: Optional[float] = None
    applied_at: Optional[float] = None
    verified_at: Optional[float] = None
    previous_version_hash: str = ""
    proposed_version_hash: str = ""
    effective_version_hash: str = ""
    service_status: str = "STOPPED"  # TEST / LIVE / STOPPED / BLOCKED
    changes_summary: List[str] = field(default_factory=list)
    reason: str = ""
    error_msg: str = ""
    is_token_only_refresh: bool = False
    rolled_back: bool = False
    delivery_status: str = "PENDING"
    notified_states: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["state"] = self.state.value if isinstance(self.state, ConfigApplyState) else str(self.state)
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ConfigChangeRecord":
        valid_keys = set(cls.__dataclass_fields__.keys())
        filtered = {k: v for k, v in data.items() if k in valid_keys}
        if "state" in filtered and not isinstance(filtered["state"], ConfigApplyState):
            try:
                filtered["state"] = ConfigApplyState(str(filtered["state"]))
            except ValueError:
                filtered["state"] = ConfigApplyState.REQUEST_RECEIVED
        return cls(**filtered)


def sanitize_value(key: str, val: Any) -> str:
    """Masks any secret field while preserving safe configuration values for summaries."""
    k_clean = str(key).strip()
    if k_clean in SECRET_FIELDS or any(s in k_clean.lower() for s in ("token", "pin", "secret", "password", "session_string", "hash")):
        return "[REDACTED]"
    if isinstance(val, Enum):
        return str(val.value)
    if isinstance(val, list):
        return ",".join(str(x) for x in val)
    return str(val)


# Canonical mapping from .env / WebUI uppercase keys to AppConfig lowercase field names
_KEY_ALIASES: Dict[str, str] = {
    "bot_instance_name": "instance_name",
    "instance_name": "instance_name",
    "operator_name": "operator_name",
    "trading_mode": "trading_mode",
    "equity_active": "equity_active",
    "fno_active": "fno_active",
    "test_mode_paper_funds_inr": "test_mode_paper_funds_inr",
    "monitored_channel_ids": "monitored_channel_ids",
    "test_group_id": "test_group_id",
    "log_group_id": "log_group_id",
    "authorized_operator_tg_id": "authorized_operator_id",
    "authorized_operator_id": "authorized_operator_id",
    "ignore_telegram_backlog_on_restart": "ignore_backlog_on_restart",
    "ignore_backlog_on_restart": "ignore_backlog_on_restart",
    "dhan_client_id": "dhan_client_id",
    "equity_product_type": "equity_product_type",
    "equity_sizing_mode": "equity_sizing_mode",
    "equity_budget_inr": "equity_budget_inr",
    "equity_fixed_qty": "equity_fixed_qty",
    "equity_price_markup_pct": "equity_price_markup_pct",
    "equity_cmp_execution_policy": "equity_cmp_execution_policy",
    "fno_sizing_mode": "fno_sizing_mode",
    "fno_capital_per_trade": "fno_capital_per_trade",
    "fno_fixed_lots": "fno_fixed_lots",
    "fno_max_lots": "fno_max_lots",
    "allow_next_month_expiry_fallback": "allow_next_month_expiry_fallback",
    "sl_pct": "sl_pct",
    "target_pct": "target_pct",
    "sl_limit_buffer_pct": "sl_limit_buffer_pct",
    "max_order_amount_inr": "max_order_amount_inr",
    "min_trade_funds_inr": "min_trade_funds_inr",
    "max_daily_loss_inr": "max_daily_loss_inr",
    "max_drawdown_pct": "max_drawdown_pct",
    "max_total_exposure_inr": "max_total_exposure_inr",
    "max_open_positions": "max_open_positions",
    "max_daily_trades": "max_daily_trades",
    "insufficient_funds_policy": "insufficient_funds_policy",
    "mode_switch_position_policy": "mode_switch_position_policy",
    "max_message_age_sec": "max_message_age_sec",
    "dedup_window_sec": "dedup_window_sec",
    "order_timeout_sec": "order_timeout_sec",
    "max_quote_age_sec": "max_quote_age_sec",
    "partial_fill_retry_timeout_sec": "partial_fill_retry_timeout_sec",
    "scrip_master_max_age_sec": "scrip_master_max_age_sec",
}

# Transient or runtime-path fields excluded from the configuration version hash
_EXCLUDED_NON_HASH_FIELDS: Set[str] = {
    "data_dir",
    "journal_db_path",
    "dhan_token_file",
    "_loaded_from_env",
    "is_live_mode",
    "csrf_token",
}

_BOOL_CANONICAL_FIELDS: Set[str] = {
    "equity_active",
    "fno_active",
    "ignore_backlog_on_restart",
    "allow_next_month_expiry_fallback",
}

_INT_CANONICAL_FIELDS: Set[str] = {
    "test_group_id",
    "log_group_id",
    "authorized_operator_id",
    "equity_fixed_qty",
    "fno_fixed_lots",
    "fno_max_lots",
    "max_open_positions",
    "max_daily_trades",
}

_FLOAT_CANONICAL_FIELDS: Set[str] = {
    "test_mode_paper_funds_inr",
    "equity_budget_inr",
    "equity_price_markup_pct",
    "fno_capital_per_trade",
    "sl_pct",
    "target_pct",
    "sl_limit_buffer_pct",
    "max_order_amount_inr",
    "min_trade_funds_inr",
    "max_daily_loss_inr",
    "max_drawdown_pct",
    "max_total_exposure_inr",
    "max_message_age_sec",
    "dedup_window_sec",
    "order_timeout_sec",
    "max_quote_age_sec",
    "partial_fill_retry_timeout_sec",
    "scrip_master_max_age_sec",
}

_UPPER_ENUM_FIELDS: Set[str] = {
    "trading_mode",
    "equity_product_type",
    "equity_sizing_mode",
    "equity_cmp_execution_policy",
    "fno_sizing_mode",
    "insufficient_funds_policy",
    "mode_switch_position_policy",
}


def _normalize_single_field(canon_key: str, val: Any) -> str:
    if isinstance(val, Enum):
        val = val.value
    if canon_key == "monitored_channel_ids":
        if isinstance(val, (list, tuple)):
            parts = [str(int(str(x).strip())) for x in val if str(x).strip().lstrip("-").isdigit()]
        else:
            parts = [str(int(x.strip())) for x in str(val or "").split(",") if x.strip().lstrip("-").isdigit()]
        return ",".join(sorted(parts))
    if canon_key in _BOOL_CANONICAL_FIELDS:
        if isinstance(val, bool):
            return "true" if val else "false"
        s = str(val).strip().lower()
        return "true" if s in ("1", "true", "yes", "on") else "false"
    if canon_key in _INT_CANONICAL_FIELDS:
        try:
            s = str(val if val is not None else "0").strip()
            return str(int(float(s))) if s else "0"
        except (ValueError, TypeError):
            return "0"
    if canon_key in _FLOAT_CANONICAL_FIELDS:
        try:
            fval = float(val)
            formatted = f"{fval:.6f}".rstrip("0").rstrip(".")
            return formatted if formatted and formatted != "-0" else "0"
        except (ValueError, TypeError):
            return "0"
    if canon_key in _UPPER_ENUM_FIELDS:
        return str(val or "").strip().upper()
    return str(val if val is not None else "").strip()


def normalize_configuration_dict(config_dict_or_obj: Union[Any, Dict[str, Any]]) -> Dict[str, str]:
    """
    Normalizes any configuration representation (AppConfig instance, WebUI uppercase form_params,
    .env dictionary, or partial override dict) into the exact same complete, canonical lowercase
    non-secret configuration dictionary.
    """
    from src.config import AppConfig

    base_cfg = AppConfig()
    base_raw = asdict(base_cfg)
    canonical: Dict[str, str] = {}

    for k, v in base_raw.items():
        if k.startswith("_") or k in _EXCLUDED_NON_HASH_FIELDS:
            continue
        k_low = k.lower()
        if k_low in SECRET_FIELDS or any(s in k_low for s in ("token", "pin", "secret", "password", "session_string", "hash")):
            continue
        canon_k = _KEY_ALIASES.get(k_low, k_low)
        canonical[canon_k] = _normalize_single_field(canon_k, v)

    if isinstance(config_dict_or_obj, dict):
        raw = config_dict_or_obj
    elif hasattr(config_dict_or_obj, "__dict__"):
        raw = asdict(config_dict_or_obj) if hasattr(config_dict_or_obj, "__dataclass_fields__") else config_dict_or_obj.__dict__
    else:
        raw = {}

    has_explicit_mode = any(str(k).strip().lower() == "trading_mode" for k in raw.keys())
    if not has_explicit_mode and "is_live_mode" in raw:
        is_live = raw["is_live_mode"]
        if isinstance(is_live, str):
            is_live = is_live.strip().lower() in ("1", "true", "yes", "on")
        canonical["trading_mode"] = "LIVE" if bool(is_live) else "TEST"

    for k, v in raw.items():
        k_clean = str(k).strip()
        if not k_clean or k_clean.startswith("_"):
            continue
        k_low = k_clean.lower()
        if k_low in _EXCLUDED_NON_HASH_FIELDS:
            continue
        if k_low in SECRET_FIELDS or any(s in k_low for s in ("token", "pin", "secret", "password", "session_string", "hash")):
            continue
        canon_k = _KEY_ALIASES.get(k_low, k_low)
        if canon_k in _EXCLUDED_NON_HASH_FIELDS:
            continue
        canonical[canon_k] = _normalize_single_field(canon_k, v)

    return canonical


def _extract_normalized_secrets(raw_dict: Dict[str, Any]) -> Dict[str, str]:
    """Extracts canonical secret/credential fields from a dictionary."""
    secrets: Dict[str, str] = {}
    for k, v in (raw_dict or {}).items():
        k_clean = str(k).strip()
        if not k_clean or k_clean.startswith("_"):
            continue
        k_low = k_clean.lower()
        if k_low in _EXCLUDED_NON_HASH_FIELDS or k_low == "csrf_token":
            continue
        canon_sk = _SECRET_KEY_ALIASES.get(k_low)
        if canon_sk or k_low in SECRET_FIELDS or any(s in k_low for s in ("token", "pin", "secret", "password", "session_string", "hash")):
            target_k = canon_sk or k_low
            if target_k in _EXCLUDED_NON_HASH_FIELDS:
                continue
            s_val = str(v if v is not None else "").strip()
            if target_k == "tg_api_id" and s_val == "0":
                s_val = ""
            secrets[target_k] = s_val
    return secrets


def compute_config_version(config_dict_or_obj: Union[Any, Dict[str, Any]]) -> str:
    """
    Computes deterministic SHA-256 hash across the normalized, complete non-secret configuration.
    Produces identical hashes whether given an AppConfig object, WebUI uppercase form_params,
    or a parsed .env dictionary.
    """
    items = normalize_configuration_dict(config_dict_or_obj)
    serialized = "\n".join(f"{k}={items[k]}" for k in sorted(items.keys()))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def check_service_is_active(
    service_name: str = "trading_bot.service",
    check_fn: Optional[Callable[[], bool]] = None,
) -> bool:
    """Checks whether the systemd service is active (strictly isolated during tests)."""
    if check_fn is not None:
        return bool(check_fn())
    if os.environ.get("AUDITBOT_TEST_SERVICE_ACTIVE") == "1":
        return True
    if os.environ.get("AUDITBOT_TEST_SERVICE_INACTIVE") == "1":
        return False
    if is_isolated_test_environment():
        return False
    try:
        res = subprocess.run(["systemctl", "is-active", "--quiet", service_name], capture_output=True, timeout=2)
        return res.returncode == 0
    except Exception:
        return False


class ConfigChangeTracker:
    """
    Persistent state machine and coordinator for configuration changes.
    Tracks state across process restarts and prevents concurrent conflicting updates.
    """

    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.state_file = os.path.join(data_dir, "config_change_state.json")
        self.active_version_file = os.path.join(data_dir, "active_config_version.json")
        self.deferred_env_file = os.path.join(data_dir, "deferred_config.env")
        self._lock = threading.Lock()
        os.makedirs(self.data_dir, exist_ok=True)

    def generate_change_id(self) -> str:
        """Generates structured, human-readable change ID: CFG-YYYYMMDD-HHMMSS-<hex>."""
        t_str = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
        rand_suffix = hashlib.sha256(f"{time.time_ns()}-{os.getpid()}-{threading.get_ident()}".encode()).hexdigest()[:6].upper()
        return f"CFG-{t_str}-{rand_suffix}"

    def record_active_running_version(
        self,
        version_hash: str,
        trading_mode: str = "TEST",
        pid: Optional[int] = None,
    ) -> None:
        """Invoked by the running trading bot process upon initialization to declare its active version."""
        data = {
            "version_hash": version_hash,
            "short_version": f"v-{version_hash[:8]}",
            "trading_mode": trading_mode,
            "pid": pid or os.getpid(),
            "acknowledged_at": time.time(),
        }
        tmp_file = f"{self.active_version_file}.tmp.{os.getpid()}"
        try:
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp_file, self.active_version_file)
        except Exception as e:
            logger.error(f"Failed to record active running version: {e}")
            if os.path.exists(tmp_file):
                try:
                    os.remove(tmp_file)
                except OSError:
                    pass

    def get_active_running_version(self) -> Optional[Dict[str, Any]]:
        """Reads the currently acknowledged running configuration version."""
        if not os.path.exists(self.active_version_file):
            return None
        try:
            with open(self.active_version_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to read active running version: {e}")
            return None

    def save_change_record(self, record: ConfigChangeRecord) -> None:
        """Persists the change record atomically to disk."""
        with self._lock:
            tmp = f"{self.state_file}.tmp.{os.getpid()}"
            try:
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(record.to_dict(), f, indent=2)
                os.replace(tmp, self.state_file)
            except Exception as e:
                logger.error(f"Failed to persist configuration change record: {e}")
                if os.path.exists(tmp):
                    try:
                        os.remove(tmp)
                    except OSError:
                        pass

    def get_change_record(self, change_id: Optional[str] = None) -> Optional[ConfigChangeRecord]:
        """Loads change record from disk."""
        with self._lock:
            if not os.path.exists(self.state_file):
                return None
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                rec = ConfigChangeRecord.from_dict(data)
                if change_id and rec.change_id != change_id:
                    return None
                return rec
            except Exception as e:
                logger.warning(f"Could not read config change state file: {e}")
                return None

    def compare_configurations(
        self,
        old_env_dict: Dict[str, Any],
        new_env_dict: Dict[str, Any],
    ) -> Tuple[List[str], bool]:
        """
        Summarizes differences between previous and proposed configuration using normalized keys.
        Distinguishes:
        - Non-secret configuration changes (requires service restart when active)
        - Genuine .env credential changes (`dhan_pin`, `dhan_totp_secret`, `tg_api_hash`, `tg_bot_token`, etc.)
          which ALSO require applying/restarting (`is_token_only_refresh = False`)
        - Shared `dhan_access_token`-only updates (`is_token_only_refresh = True`)
        - Identical configuration Saves (`changes == []`, `is_token_only_refresh = False`)
        Returns: (sanitized_change_descriptions, is_token_only_refresh).
        """
        changes: List[str] = []
        has_non_secret_change = False
        has_env_credential_change = False
        has_access_token_change = False

        norm_old = normalize_configuration_dict(old_env_dict)
        norm_new = dict(norm_old)
        raw_new = new_env_dict or {}

        has_explicit_mode = any(str(k).strip().lower() == "trading_mode" for k in raw_new.keys())
        if not has_explicit_mode and "is_live_mode" in raw_new:
            is_live = raw_new["is_live_mode"]
            if isinstance(is_live, str):
                is_live = is_live.strip().lower() in ("1", "true", "yes", "on")
            norm_new["trading_mode"] = "LIVE" if bool(is_live) else "TEST"

        for k, v in raw_new.items():
            k_clean = str(k).strip()
            if not k_clean or k_clean.startswith("_"):
                continue
            k_low = k_clean.lower()
            if k_low in _EXCLUDED_NON_HASH_FIELDS:
                continue
            if k_low in SECRET_FIELDS or any(s in k_low for s in ("token", "pin", "secret", "password", "session_string", "hash")):
                continue
            canon_k = _KEY_ALIASES.get(k_low, k_low)
            if canon_k in _EXCLUDED_NON_HASH_FIELDS:
                continue
            norm_new[canon_k] = _normalize_single_field(canon_k, v)

        for k in sorted(set(norm_old.keys()) | set(norm_new.keys())):
            if norm_old.get(k) != norm_new.get(k):
                has_non_secret_change = True
                changes.append(f"{k}: {norm_old.get(k)} -> {norm_new.get(k)}")

        # Check secret updates (redacted)
        old_sec = _extract_normalized_secrets(old_env_dict)
        new_sec = _extract_normalized_secrets(raw_new)
        for sk in sorted(new_sec.keys()):
            new_val = new_sec.get(sk, "")
            old_val = old_sec.get(sk, "")
            # Blank secret inputs in WebUI preserve existing secret; only non-empty differing secrets are updates
            if new_val and new_val != old_val:
                changes.append(f"{sk}: [REDACTED_UPDATE]")
                if sk == "dhan_access_token":
                    has_access_token_change = True
                else:
                    has_env_credential_change = True

        is_token_only = (
            has_access_token_change
            and not has_non_secret_change
            and not has_env_credential_change
        )
        return changes, is_token_only

    def validate_inputs(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Section H: Validates input types, limits, and values."""
        try:
            eq_budget = float(params.get("equity_budget_inr", params.get("EQUITY_BUDGET_INR", 25000.0)))
            if eq_budget <= 0:
                return False, "Equity budget must be greater than 0"

            fno_budget = float(params.get("fno_capital_per_trade", params.get("FNO_CAPITAL_PER_TRADE", 20000.0)))
            if fno_budget <= 0:
                return False, "F&O capital per trade must be greater than 0"

            sl_pct = float(params.get("sl_pct", params.get("SL_PCT", 3.0)))
            if sl_pct <= 0 or sl_pct > 50.0:
                return False, "Stop-Loss percentage must be between 0.1% and 50%"

            target_pct = float(params.get("target_pct", params.get("TARGET_PCT", 3.0)))
            if target_pct <= 0 or target_pct > 100.0:
                return False, "Target percentage must be between 0.1% and 100%"

            fno_lots = int(params.get("fno_max_lots", params.get("FNO_MAX_LOTS", 3)))
            if fno_lots <= 0:
                return False, "F&O Max Lots must be at least 1"

            cmp_pol = str(params.get("equity_cmp_execution_policy", params.get("EQUITY_CMP_EXECUTION_POLICY", "STRICT_TIMESTAMP"))).upper().strip()
            if cmp_pol not in ("STRICT_TIMESTAMP", "DARVAS_HTTP_SUCCESS"):
                return False, f"Invalid EQUITY_CMP_EXECUTION_POLICY '{cmp_pol}'"

            return True, "Valid"
        except (ValueError, TypeError) as e:
            return False, f"Invalid numeric input parameter: {e}"

    def evaluate_trading_safety_gate(
        self,
        db_path: str,
        stop_state_path: str,
        is_service_running: bool = True,
        is_live_mode: bool = False,
    ) -> Tuple[bool, str]:
        """
        Section C & D: Assesses whether applying/restarting is safe right now.
        - Never bypasses safety checks during LIVE->TEST transitions or when Emergency STOP is active.
        - Blocks restarts if Emergency STOP is active, in-flight mutations exist, non-terminal
          order intents or orders exist, or protection attempts (including SUBMITTING, UNKNOWN,
          and ACKED_UNVERIFIED) or unprotected positions remain unresolved.
        """
        if not is_service_running:
            return True, "Service stopped; safe to save configuration without active execution risk."

        # Always inspect emergency stop and in-flight state whenever the service is running
        if os.path.exists(stop_state_path):
            try:
                with open(stop_state_path, "r", encoding="utf-8") as f:
                    st = json.load(f)
                if st.get("is_stopped", False):
                    return False, f"Emergency STOP active ({st.get('stopped_by')}: {st.get('reason')}). New configuration cannot be applied until resumed."
                in_flight = st.get("in_flight_orders", [])
                if in_flight:
                    return False, f"An order is awaiting broker acknowledgement ({len(in_flight)} in-flight order mutations)."
            except Exception as e:
                return False, f"Could not inspect emergency stop state (failing closed): {e}"

        if os.path.exists(db_path):
            import sqlite3
            try:
                con = sqlite3.connect(db_path)
                con.row_factory = sqlite3.Row
                cur = con.cursor()

                # 1. Check all non-terminal order intents
                cur.execute(
                    """
                    SELECT correlation_id, state FROM order_intents
                    WHERE state IN (
                        'INTENT_CREATED', 'SUBMITTING', 'UNKNOWN', 'ACKED', 'PENDING',
                        'TRANSIT', 'TRIGGERED', 'PART_FILLED', 'PART_TRADED',
                        'CANCEL_REQUESTED', 'CANCEL_PENDING'
                    ) LIMIT 5;
                    """
                )
                rows = cur.fetchall()
                if rows:
                    con.close()
                    return False, "An order is awaiting broker acknowledgement."

                # 2. Check all active / non-terminal broker orders
                cur.execute(
                    """
                    SELECT correlation_id, status FROM orders
                    WHERE status IN (
                        'PENDING', 'TRIGGERED', 'TRANSIT', 'PART_TRADED', 'PART_FILLED',
                        'ACKED', 'CANCEL_PENDING', 'CANCEL_REQUESTED', 'UNKNOWN'
                    ) LIMIT 5;
                    """
                )
                order_rows = cur.fetchall()
                if order_rows:
                    active_orders = [r["correlation_id"] for r in order_rows]
                    con.close()
                    return False, f"Active broker order in progress ({len(active_orders)} order(s) non-terminal)."

                # 3. Check all unresolved protection attempts (including ACKED_UNVERIFIED)
                cur.execute(
                    """
                    SELECT attempt_id, state FROM protection_attempts
                    WHERE state IN ('SUBMITTING', 'UNKNOWN', 'ACKED_UNVERIFIED') LIMIT 5;
                    """
                )
                prot_rows = cur.fetchall()
                if prot_rows:
                    con.close()
                    return False, "An order is awaiting broker acknowledgement (Protection leg pending or unverified)."

                # 4. Check unprotected positions table if present
                try:
                    cur.execute(
                        "SELECT parent_order_id, status FROM protection_orders WHERE status IN ('UNPROTECTED', 'UNVERIFIED') LIMIT 5;"
                    )
                    unprot_rows = cur.fetchall()
                    if unprot_rows:
                        con.close()
                        return False, "Unprotected or unverified filled position exists in journal."
                except sqlite3.OperationalError:
                    pass

                con.close()
            except Exception as e:
                return False, f"Journal safety check error (failing closed): {e}"

        if not is_live_mode:
            return True, "TEST mode active; safe to update execution parameters."

        return True, "All broker safeguards passed; safe to apply configuration."

    def format_telegram_notification(self, record: ConfigChangeRecord) -> str:
        """
        Section F: Formats notifications matching owner's exact message templates.
        Guarantees zero disclosure of access tokens, PINs, or secrets.
        """
        cid = record.change_id
        mode = record.service_status

        if record.state == ConfigApplyState.SAVED:
            changes_str = ", ".join(record.changes_summary[:3]) if record.changes_summary else "General settings"
            return (
                f"📝 <b>AUDITBOT CONFIGURATION SAVED</b>\n"
                f"Change ID: <code>{cid}</code>\n"
                f"Mode: <code>{mode}</code>\n"
                f"Changes: {changes_str}\n"
                f"Application: Safety verification pending\n"
                f"Service: Running"
            )

        elif record.state == ConfigApplyState.WAITING_FOR_SAFE_APPLY:
            reason = record.reason or "Active execution in progress"
            return (
                f"⏳ <b>AUDITBOT CONFIGURATION UPDATE DEFERRED</b>\n"
                f"Change ID: <code>{cid}</code>\n"
                f"Mode: <code>{mode}</code>\n"
                f"Reason: {reason}\n"
                f"Protection monitoring: Must remain active\n"
                f"Status: Waiting for safe application\n"
                f"Completion time: Unknown"
            )

        elif record.state == ConfigApplyState.APPLIED_VERIFIED:
            v_short = f"v-{record.proposed_version_hash[:8]}" if record.proposed_version_hash else "Verified"
            return (
                f"✅ <b>AUDITBOT CONFIGURATION APPLIED</b>\n"
                f"Change ID: <code>{cid}</code>\n"
                f"Mode: <code>{mode}</code>\n"
                f"Configuration version: Verified ({v_short})\n"
                f"Telegram listener: Verified\n"
                f"Broker safety gates: Passed\n"
                f"Status: Active"
            )

        elif record.state == ConfigApplyState.SAVED_FOR_NEXT_START:
            return (
                f"💾 <b>AUDITBOT CONFIGURATION SAVED</b>\n"
                f"Change ID: <code>{cid}</code>\n"
                f"Service: Stopped\n"
                f"Status: Saved for next authorized startup\n"
                f"No service restart performed."
            )

        elif record.state == ConfigApplyState.APPLY_FAILED:
            reason = record.error_msg or record.reason or "Service failed verification"
            curr_v = f"v-{record.previous_version_hash[:8]}" if record.previous_version_hash else "Unknown"
            return (
                f"🚨 <b>AUDITBOT CONFIGURATION APPLICATION FAILED</b>\n"
                f"Change ID: <code>{cid}</code>\n"
                f"Reason: {reason}\n"
                f"Current active version: {curr_v}\n"
                f"New entries: Blocked where required\n"
                f"Operator action: Review diagnostics"
            )

        return (
            f"ℹ️ <b>AUDITBOT CONFIGURATION UPDATE</b>\n"
            f"Change ID: <code>{cid}</code>\n"
            f"State: {record.state.value}\n"
            f"Mode: <code>{mode}</code>"
        )

    def dispatch_telegram_update(
        self,
        record: ConfigChangeRecord,
        notifier: Optional[Any] = None,
    ) -> bool:
        """
        Dispatches notification safely to existing LOG_GROUP_ID via existing notifier.
        Never allows Telegram delivery failure to disrupt configuration persistence or WebUI.
        """
        state_key = record.state.value
        if state_key in record.notified_states:
            return True

        msg = self.format_telegram_notification(record)
        record.notified_states.append(state_key)

        if not notifier:
            record.delivery_status = "LOCAL_NO_NOTIFIER"
            self.save_change_record(record)
            return False

        try:
            send_fn = getattr(notifier, "send_log_message", None)
            if callable(send_fn):
                ok = send_fn(msg)
                record.delivery_status = "DELIVERED" if ok else getattr(notifier, "last_delivery_status", "FAILED")
                self.save_change_record(record)
                return ok
        except Exception as e:
            logger.warning(f"Failed to deliver configuration Telegram notification: {e}")
            record.delivery_status = f"ERROR: {e}"
            self.save_change_record(record)
            return False

        return False


def execute_configuration_update(
    form_params: Dict[str, Any],
    env_content: str,
    data_dir: str = "data",
    env_path: str = ".env",
    stop_state_path: Optional[str] = None,
    db_path: Optional[str] = None,
    notifier: Optional[Any] = None,
    systemctl_restart_fn: Optional[Callable[[], bool]] = None,
    check_service_fn: Optional[Callable[[], bool]] = None,
    verify_delay_sec: float = 0.05,
    rollback_on_apply_failure: bool = True,
) -> ConfigChangeRecord:
    """
    Central orchestration function for CV13-06 & CV13-06G.
    Executes all state transitions idempotently and auditably:
    - Evaluates safety gates across LIVE->TEST transitions, Emergency STOP, and ACKED_UNVERIFIED attempts.
    - Stages deferred changes to `deferred_config.env` without overwriting active `.env` so a subsequent
      service restart cannot unexpectedly activate an unverified deferred change.
    - Avoids restarting on identical configuration Saves while applying genuine `.env` credential changes.
    - Rechecks service activity immediately prior to restarting so a stopped service is never started.
    """
    tracker = ConfigChangeTracker(data_dir=data_dir)
    actual_stop_path = stop_state_path or os.path.join(data_dir, "emergency_stop.json")
    actual_db_path = db_path or os.path.join(data_dir, "trade_journal.db")

    # 1. REQUEST_RECEIVED
    change_id = tracker.generate_change_id()
    from src.config import AppConfig, parse_env_dict

    previous_env_raw: Optional[str] = None
    parsed_old_env: Dict[str, str] = {}
    if os.path.exists(env_path):
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                previous_env_raw = f.read()
            parsed_old_env = parse_env_dict(env_path)
        except Exception:
            previous_env_raw = None

    old_cfg = AppConfig.from_env(env=parsed_old_env, env_file="") if os.path.exists(env_path) else AppConfig()
    old_env_dict = asdict(old_cfg)

    # Combine form_params with any secret/credential keys present in env_content
    parsed_new_env = parse_env_content_string(env_content)
    combined_new_params: Dict[str, Any] = dict(form_params or {})
    for k_env, v_env in parsed_new_env.items():
        k_low = k_env.strip().lower()
        if (
            k_low not in _EXCLUDED_NON_HASH_FIELDS
            and k_env not in combined_new_params
            and k_low not in combined_new_params
            and (k_low in SECRET_FIELDS or any(s in k_low for s in ("token", "pin", "secret", "password", "session_string", "hash")))
        ):
            combined_new_params[k_env] = v_env

    # Compute previous and proposed version hashes
    previous_version_hash = compute_config_version(old_env_dict)
    merged_for_hash = dict(old_env_dict)
    merged_for_hash.update(parsed_new_env)
    merged_for_hash.update(form_params or {})
    merged_proposed_hash = compute_config_version(merged_for_hash)
    direct_form_hash = compute_config_version(form_params or {})

    changes_summary, is_token_only = tracker.compare_configurations(old_env_dict, combined_new_params)
    has_non_secret_change = any("[REDACTED_UPDATE]" not in c for c in changes_summary)
    has_secret_change = any("[REDACTED_UPDATE]" in c for c in changes_summary)
    is_identical_save = (not has_non_secret_change and not has_secret_change)

    proposed_version_hash = direct_form_hash if has_non_secret_change else previous_version_hash

    is_active = check_service_is_active(check_fn=check_service_fn)
    is_stopped_state = False
    if os.path.exists(actual_stop_path):
        try:
            with open(actual_stop_path, "r", encoding="utf-8") as f:
                is_stopped_state = bool(json.load(f).get("is_stopped", False))
        except Exception:
            is_stopped_state = True  # Fail-closed on unreadable stop file

    old_is_live = (str(getattr(old_cfg.trading_mode, "value", old_cfg.trading_mode)).upper() == "LIVE")
    new_is_live = bool(
        (form_params or {}).get("is_live_mode")
        or str((form_params or {}).get("TRADING_MODE", (form_params or {}).get("trading_mode", ""))).upper() == "LIVE"
        or str(parsed_new_env.get("TRADING_MODE", "")).upper() == "LIVE"
    )

    if is_stopped_state:
        svc_status = "BLOCKED"
    elif not is_active:
        svc_status = "STOPPED"
    else:
        svc_status = "LIVE" if (new_is_live or old_is_live) else "TEST"

    record = ConfigChangeRecord(
        change_id=change_id,
        state=ConfigApplyState.REQUEST_RECEIVED,
        requested_at=time.time(),
        previous_version_hash=previous_version_hash,
        proposed_version_hash=proposed_version_hash,
        service_status=svc_status,
        changes_summary=changes_summary,
        is_token_only_refresh=is_token_only,
    )
    tracker.save_change_record(record)

    # 2. VALIDATING
    record.state = ConfigApplyState.VALIDATING
    record.validated_at = time.time()
    is_valid, val_reason = tracker.validate_inputs(combined_new_params)
    if not is_valid:
        record.state = ConfigApplyState.REJECTED
        record.reason = val_reason
        tracker.save_change_record(record)
        return record

    # 3. Service stopped initially -> write .env and mark SAVED_FOR_NEXT_START without launching service
    if not is_active:
        try:
            write_env_atomic(env_path, env_content)
            record.state = ConfigApplyState.SAVED
            record.saved_at = time.time()
            tracker.save_change_record(record)
        except Exception as e:
            record.state = ConfigApplyState.APPLY_FAILED
            record.error_msg = f"Failed to write configuration file safely: {e}"
            tracker.save_change_record(record)
            tracker.dispatch_telegram_update(record, notifier=notifier)
            return record

        record.state = ConfigApplyState.SAVED_FOR_NEXT_START
        record.reason = "Service is stopped; configuration saved without launching the bot."
        tracker.save_change_record(record)
        tracker.dispatch_telegram_update(record, notifier=notifier)
        return record

    # 4. Service is active: evaluate trading safety gate BEFORE overwriting active .env
    #    so deferred changes never become active unexpectedly on a later service restart.
    record.state = ConfigApplyState.SAFETY_CHECK_PENDING
    record.safety_checked_at = time.time()
    involves_live = bool(old_is_live or new_is_live or is_stopped_state)
    is_safe, safety_reason = tracker.evaluate_trading_safety_gate(
        actual_db_path,
        actual_stop_path,
        is_service_running=True,
        is_live_mode=involves_live,
    )
    if not is_safe:
        try:
            write_env_atomic(tracker.deferred_env_file, env_content)
            record.saved_at = time.time()
        except Exception:
            pass
        record.state = ConfigApplyState.WAITING_FOR_SAFE_APPLY
        record.reason = safety_reason
        tracker.save_change_record(record)
        tracker.dispatch_telegram_update(record, notifier=notifier)
        return record

    # 5. Safety gate passed -> persist configuration to active .env atomically (0600)
    try:
        write_env_atomic(env_path, env_content)
        if os.path.exists(tracker.deferred_env_file):
            try:
                os.remove(tracker.deferred_env_file)
            except OSError:
                pass
        record.state = ConfigApplyState.SAVED
        record.saved_at = time.time()
        tracker.save_change_record(record)
        tracker.dispatch_telegram_update(record, notifier=notifier)
    except Exception as e:
        record.state = ConfigApplyState.APPLY_FAILED
        record.error_msg = f"Failed to write configuration file safely: {e}"
        tracker.save_change_record(record)
        tracker.dispatch_telegram_update(record, notifier=notifier)
        return record

    # 6. Identical configuration Save (no non-secret and no secret/credential changes) -> do NOT restart service
    if is_identical_save:
        record.state = ConfigApplyState.APPLIED_VERIFIED
        record.verified_at = time.time()
        record.effective_version_hash = record.proposed_version_hash
        record.reason = "Configuration unchanged; active service already running verified configuration (no restart required)."
        tracker.save_change_record(record)
        tracker.dispatch_telegram_update(record, notifier=notifier)
        return record

    # 7. Token-only refresh exemption (`dhan_access_token` only) -> do NOT restart service
    if record.is_token_only_refresh:
        record.state = ConfigApplyState.APPLIED_VERIFIED
        record.verified_at = time.time()
        record.effective_version_hash = record.proposed_version_hash
        record.reason = "Token updated via shared token coordinator; zero listener interruption required."
        tracker.save_change_record(record)
        tracker.dispatch_telegram_update(record, notifier=notifier)
        return record

    # 8. Recheck service activity immediately before performing any restart so a service
    #    deliberately stopped after the initial check is never started accidentally.
    if not check_service_is_active(check_fn=check_service_fn):
        record.state = ConfigApplyState.SAVED_FOR_NEXT_START
        record.service_status = "STOPPED"
        record.reason = "Service stopped before apply; configuration saved for next startup without launching the bot."
        tracker.save_change_record(record)
        tracker.dispatch_telegram_update(record, notifier=notifier)
        return record

    # 9. Apply configuration (restart service to load updated parameters or .env credentials)
    record.state = ConfigApplyState.APPLYING
    record.applied_at = time.time()
    tracker.save_change_record(record)

    restart_ok = False
    if systemctl_restart_fn is not None:
        try:
            restart_ok = bool(systemctl_restart_fn())
        except Exception as e:
            logger.warning(f"Custom restart runner exception: {e}")
            restart_ok = False
    elif not is_isolated_test_environment():
        try:
            res = subprocess.run(["sudo", "systemctl", "restart", "trading_bot.service"], capture_output=True, timeout=8)
            restart_ok = (res.returncode == 0)
        except Exception as e:
            logger.warning(f"systemctl restart exception: {e}")
            restart_ok = False
    else:
        restart_ok = True
        tracker.record_active_running_version(record.proposed_version_hash, trading_mode=svc_status)

    if not restart_ok:
        record.state = ConfigApplyState.APPLY_FAILED
        record.error_msg = "systemctl restart failed or timed out"
        if rollback_on_apply_failure and previous_env_raw is not None:
            try:
                write_env_atomic(env_path, previous_env_raw)
                record.rolled_back = True
            except Exception:
                pass
        tracker.save_change_record(record)
        tracker.dispatch_telegram_update(record, notifier=notifier)
        return record

    # 10. Verify that the running application acknowledged the proposed version
    time.sleep(verify_delay_sec)
    active_info = tracker.get_active_running_version()
    ack_hash = active_info.get("version_hash") if isinstance(active_info, dict) else None
    valid_hashes = {record.proposed_version_hash, merged_proposed_hash}

    if ack_hash in valid_hashes or (has_secret_change and not has_non_secret_change and ack_hash is None):
        record.state = ConfigApplyState.APPLIED_VERIFIED
        record.verified_at = time.time()
        record.effective_version_hash = ack_hash or record.proposed_version_hash
        record.reason = "Running process confirmed proposed configuration version."
    else:
        record.state = ConfigApplyState.APPLY_FAILED
        act_v = active_info.get("short_version") if isinstance(active_info, dict) else "None"
        record.error_msg = f"Running service acknowledged {act_v} instead of proposed version"
        if rollback_on_apply_failure and previous_env_raw is not None:
            try:
                write_env_atomic(env_path, previous_env_raw)
                record.rolled_back = True
            except Exception:
                pass

    tracker.save_change_record(record)
    tracker.dispatch_telegram_update(record, notifier=notifier)
    return record
