"""Advisory token recovery notifications; no order or reconciliation mutations."""
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import threading
import time
from pathlib import Path

from src.broker.dhan_token import locked_token_file, read_token, write_token_atomic

logger = logging.getLogger(__name__)
EVENT = "DHAN_TOKEN_RECOVERY_VERIFIED"


def fingerprint(token):
    return hashlib.sha256(token.encode("utf-8")).hexdigest() if token else ""


def _path(cfg):
    return Path(cfg.data_dir) / "dhan_token_recovery_pending.json"


def _scope(cfg):
    return fingerprint(str(cfg.dhan_client_id) + ":" + str(cfg.instance_name))


def _load(cfg):
    path = _path(cfg)
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"scope": _scope(cfg), "pending": False}
    if not isinstance(state, dict) or state.get("scope") != _scope(cfg):
        raise ValueError("Recovery marker identity unverified")
    return state


def _save(cfg, state):
    write_token_atomic(json.dumps(state, sort_keys=True), str(_path(cfg)))


def update_scheduled_recovery(cfg, *, renewed, ready, token_problem,
                              prior_fingerprint, prior_expiration, current_token, now):
    """Keep scheduled failure evidence; suppress recovery for a clean renewal."""
    try:
        with locked_token_file(str(_path(cfg))):
            state = _load(cfg)
            if renewed and ready and current_token:
                state.update(pending=False,
                             last_notified_token_fingerprint=fingerprint(current_token))
            elif not renewed or token_problem:
                state.update(pending=True, failed_at=float(now),
                             prior_token_expiration_ist=str(prior_expiration or ""),
                             prior_token_fingerprint=prior_fingerprint,
                             instance_name=str(cfg.instance_name))
            else:
                return
            _save(cfg, state)
    except Exception as exc:
        logger.warning("Token recovery marker update unavailable: %s", type(exc).__name__)


def _verified_card(cfg, broker, db, gate, token, now):
    """Pin both GETs to the published credential; never trigger authentication."""
    from src.broker.dhan_client import BASE_URL
    from src.broker.read_governor import RECONCILIATION_PACING
    from src.cli import format_morning_health_report
    from src.instruments.resolver import InstrumentResolver

    pacing = RECONCILIATION_PACING.set(True)
    try:
        profile = broker.verify_profile_and_token_expiry(
            allow_refresh=False, candidate_token=token, now_ts=now)
        if (not isinstance(profile, dict)
                or profile.get("identity_status") != "VERIFIED"
                or profile.get("identity_verified") is not True
                or profile.get("profile_client_id") != str(cfg.dhan_client_id)
                or profile.get("token_expired") is not False):
            return None
        expiry = profile.get("token_expiration_ts")
        if isinstance(expiry, bool) or not isinstance(expiry, (float, int)):
            return None
        if not math.isfinite(expiry) or expiry <= now:
            return None
        response = broker._governed_get(
            BASE_URL + "/fundlimit", headers={
                "access-token": token, "client-id": str(cfg.dhan_client_id),
                "Content-Type": "application/json", "Accept": "application/json",
            }, timeout=5, priority="identity")
        if response.status_code != 200:
            return None
        funds = response.json()
        if not isinstance(funds, dict):
            return None
        if "dhanClientId" in funds and str(funds["dhanClientId"]) != str(cfg.dhan_client_id):
            return None
        balance = next((funds[k] for k in ("availabelBalance", "availableBalance", "sodLimit")
                        if funds.get(k) is not None), None)
        if balance is None or isinstance(balance, bool):
            return None
        balance = float(balance)
        if not math.isfinite(balance):
            return None
    finally:
        RECONCILIATION_PACING.reset(pacing)
    # Use the identical scheduled readiness policy, plus the current gate.
    # A successful token refresh alone does not mean trading is ready.
    intents = db.get_non_terminal_intents()
    unprotected = db.get_unprotected_orders()
    if not isinstance(intents, list) or not isinstance(unprotected, list):
        return None
    master_ok = InstrumentResolver().load_dhan_master(
        os.path.join(cfg.data_dir, "scrip_master.csv"),
        max_cache_age_sec=cfg.scrip_master_max_age_sec,
        allow_network_download=False)
    stopped = broker.stop_manager.is_stopped()
    report = format_morning_health_report(
        cfg, now_ts=now, token_verified=True, token_renewed=True,
        funds_verified=True, fallback_funds=False, available_balance=balance,
        master_loaded=master_ok, is_stopped=stopped,
        account_identity_status="VERIFIED", profile_client_id=str(cfg.dhan_client_id),
        token_expiration_ts=expiry,
        token_expiration_str=profile.get("token_expiration_str", ""),
        token_expired=False,
        reconciliation_ok=gate.is_accepting_entries is True,
        unresolved_intents_count=len(intents), unprotected_positions_count=len(unprotected),
        canonical=True)
    if ("Next trading session readiness: ✅ VERIFIED CHECKS" not in report
            or "Overall result: ✅ SUCCESS" not in report):
        return None
    return (report.replace("🕔 <b>AUDITBOT — 05:00 PM DHAN HEALTH CHECK</b>",
                           "✅ <b>AUDITBOT — DHAN TOKEN RECOVERY VERIFIED</b>", 1)
            .replace("Token renewal: ✅ SUCCESS", "Token renewal: ✅ AUTO-RECOVERED", 1)
            .replace("Next trading session readiness: ✅ VERIFIED CHECKS",
                     "Next trading session readiness: ✅ READY", 1)
            .replace("Overall result: ✅ SUCCESS",
                     "Overall result: ✅ RECOVERED — NO ACTION REQUIRED", 1))


class RuntimeTokenRecoveryNotice:
    """One periodic worker, with cross-process marker lock and durable outbox claim."""
    def __init__(self, cfg, broker, db, gate):
        self.cfg, self.broker, self.db, self.gate = cfg, broker, db, gate
        self._lock = threading.Lock()
        self._rejected = ""
        self._last_attempt = 0.0

    def observe_auth_recovery(self, rejected, fresh):
        # Called on broker path: no disk, network, TOTP or waiting for worker.
        if rejected and fresh and rejected != fresh:
            self._rejected = fingerprint(rejected)

    def poll(self, *, now=None):
        if not self._lock.acquire(blocking=False):
            return False
        try:
            return self._poll(time.time() if now is None else float(now))
        except Exception as exc:
            logger.warning("Recovery notification check deferred: %s", type(exc).__name__)
            return False
        finally:
            self._lock.release()

    def _poll(self, now):
        cfg = self.cfg
        if not getattr(cfg, "log_group_id", 0):
            return False
        if self._last_attempt and now - self._last_attempt < 30:
            return False
        self._last_attempt = now
        rejected = self._rejected
        with locked_token_file(str(_path(cfg)), timeout_sec=0):
            state = _load(cfg)
            token = read_token(cfg.dhan_token_file)
            digest = fingerprint(token)
            if not digest:
                return False
            if digest == state.get("last_notified_token_fingerprint"):
                if rejected == self._rejected:
                    self._rejected = ""
                return False
            if rejected and digest != rejected and not state.get("pending"):
                state.update(pending=True, failed_at=now, prior_token_fingerprint=rejected,
                             prior_token_expiration_ist="", instance_name=str(cfg.instance_name))
                _save(cfg, state)
            if not state.get("pending") or digest == state.get("prior_token_fingerprint"):
                return False
            card = _verified_card(cfg, self.broker, self.db, self.gate, token, now)
            if not card or read_token(cfg.dhan_token_file) != token:
                return False
            # Atomic event claim + enqueue; no direct send-before-claim crash window.
            # False means already queued by a previous process.
            self.db.enqueue_order_notification_once(
                "DHAN_RECOVERY:" + _scope(cfg) + ":" + digest, EVENT, card)
            state.update(pending=False, last_notified_token_fingerprint=digest)
            _save(cfg, state)
            if rejected == self._rejected:
                self._rejected = ""
            return True
