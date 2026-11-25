"""Durable, reporting-only Dhan token recovery confirmation.

No TOTP calls, trading mutations, or gate changes. One outbox claim per
account/instance/token; a retry after a crash cannot create another claim.
The existing Telegram outbox transport is at-least-once, not exactly-once.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import uuid
from pathlib import Path
import threading
import time

from src.broker.dhan_token import locked_token_file, read_token, write_token_atomic

logger = logging.getLogger(__name__)
MARKER = "dhan_token_recovery_pending.json"


def fingerprint(token):
    return hashlib.sha256(str(token).encode("utf-8")).hexdigest() if token else ""


def _path(cfg):
    return Path(cfg.data_dir) / MARKER


def _identity(cfg):
    return fingerprint(str(cfg.dhan_client_id) + "|" + str(cfg.instance_name))


def _load(cfg):
    path = _path(cfg)
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    if not isinstance(state, dict) or state.get("identity") != _identity(cfg):
        raise ValueError("Recovery marker identity invalid")
    return state


def _save(cfg, state):
    state = dict(state, identity=_identity(cfg))
    write_token_atomic(json.dumps(state, sort_keys=True), str(_path(cfg)))


def begin_scheduled_check(cfg, *, now):
    """Suppress runtime confirmations during a scheduled renewal attempt.

    A bounded lease permits recovery after a killed scheduled process.
    No lock is held during HTTP or Telegram operations.
    """
    check_id = uuid.uuid4().hex
    with locked_token_file(str(_path(cfg)), timeout_sec=2):
        state = _load(cfg)
        state.update(scheduled_check_id=check_id, scheduled_started_at=float(now))
        _save(cfg, state)
    return check_id


def _scheduled_active(state, now):
    started = state.get("scheduled_started_at")
    return (isinstance(started, (float, int)) and not isinstance(started, bool)
            and 0 <= now - started < 300 and bool(state.get("scheduled_check_id")))


def scheduled_recovery_state(cfg, *, renewed, ready, token_problem,
                             prior_token_fingerprint, active_token_fingerprint,
                             prior_token_expiration_ist, now, check_id=None,
                             keep_lease=False):
    """Scheduled health result; never write credentials to the marker."""
    path = str(_path(cfg))
    with locked_token_file(path, timeout_sec=2):
        state = _load(cfg)
        if check_id is not None and state.get("scheduled_check_id") != check_id:
            return
        if check_id is None and _scheduled_active(state, now):
            return
        if not keep_lease:
            state.pop("scheduled_check_id", None)
            state.pop("scheduled_started_at", None)
        if renewed and ready:
            # A normal green report resolves any pending recovery and
            # suppresses a concurrent runtime alert for this same token.
            state = {"pending": False,
                     "last_notified_token_fingerprint": active_token_fingerprint}
        elif not renewed or token_problem:
            state.update(pending=True, failed_at=float(now),
                         prior_token_expiration_ist=str(prior_token_expiration_ist or ""),
                         instance_name=str(cfg.instance_name),
                         prior_token_fingerprint=prior_token_fingerprint)
        _save(cfg, state)


def end_scheduled_check(cfg, *, check_id):
    """Release only our own lease; retain any durable pending recovery."""
    if check_id is None:
        return
    with locked_token_file(str(_path(cfg)), timeout_sec=2):
        state = _load(cfg)
        if state.get("scheduled_check_id") != check_id:
            return
        state.pop("scheduled_check_id", None)
        state.pop("scheduled_started_at", None)
        _save(cfg, state)


class TokenRecoveryConfirmation:
    """Single runtime worker, periodically retrying only pending recovery.

    Auth callbacks run on broker threads and just publish an in-memory hint;
    all file, network and SQLite work is off the trading/watcher threads.
    """
    def __init__(self, cfg, broker, db, stop_manager, gate, master_verified):
        self.cfg = cfg
        self.broker = broker
        self.db = db
        self.stop_manager = stop_manager
        self.gate = gate
        self.master_verified = master_verified
        self._hint_lock = threading.Lock()
        self._hint = None
        self._worker = threading.Lock()
        self._next_attempt = 0.0

    def auth_recovered(self, rejected, active):
        before, after = fingerprint(rejected), fingerprint(active)
        if before and after and before != after:
            with self._hint_lock:
                self._hint = (before, after)

    def _snapshot(self, active_digest, now):
        with locked_token_file(str(_path(self.cfg)), timeout_sec=0):
            state = _load(self.cfg)
            if _scheduled_active(state, now):
                return state
            with self._hint_lock:
                hint = self._hint
            if hint and hint[1] == active_digest:
                if (state.get("last_notified_token_fingerprint") != active_digest
                        and not state.get("pending")):
                    state.update(pending=True, failed_at=now,
                                 prior_token_fingerprint=hint[0],
                                 prior_token_expiration_ist="",
                                 instance_name=str(self.cfg.instance_name))
                    _save(self.cfg, state)
                    state = _load(self.cfg)
                # Keep the hint if persistence raised; never discard a newer
                # callback, or hold the callback lock during filesystem I/O.
                with self._hint_lock:
                    if self._hint == hint:
                        self._hint = None
            return state

    def poll(self, *, now=None, monotonic=None):
        if not getattr(self.cfg, "log_group_id", 0):
            return False
        if not self._worker.acquire(blocking=False):
            return False
        try:
            return self._poll(time.time() if now is None else now,
                              time.monotonic() if monotonic is None else monotonic)
        except Exception as exc:
            # Notification problems never mutate token, STOP or entry gate.
            logger.warning("Dhan recovery confirmation deferred: %s", type(exc).__name__)
            return False
        finally:
            self._worker.release()

    def _poll(self, now, monotonic):
        from src.broker.dhan_client import BASE_URL
        from src.cli import format_morning_health_report

        # Read the authoritative shared token, not a possibly stale client
        # attribute. Both probes use exactly these pinned credentials.
        active = read_token(self.cfg.dhan_token_file)
        digest = fingerprint(active)
        if not digest:
            return False
        state = self._snapshot(digest, now)
        if (_scheduled_active(state, now) or state.get("pending") is not True
                or digest == state.get("prior_token_fingerprint")
                or digest == state.get("last_notified_token_fingerprint")
                or monotonic < self._next_attempt):
            return False
        self._next_attempt = monotonic + 60.0

        profile = self.broker.verify_profile_and_token_expiry(
            allow_refresh=False, candidate_token=active, now_ts=now)
        if (not isinstance(profile, dict)
                or profile.get("identity_verified") is not True
                or profile.get("identity_status") != "VERIFIED"
                or profile.get("profile_client_id") != str(self.cfg.dhan_client_id)
                or profile.get("token_expired") is not False):
            return False
        expiry = profile.get("token_expiration_ts")
        if isinstance(expiry, bool) or not isinstance(expiry, (int, float)) or not math.isfinite(expiry) or expiry <= now:
            return False

        headers = {"access-token": active, "client-id": str(self.cfg.dhan_client_id),
                   "Content-Type": "application/json", "Accept": "application/json"}
        response = self.broker._governed_get(
            BASE_URL + "/fundlimit", headers=headers, timeout=5, priority="low")
        if response.status_code != 200:
            return False
        funds = response.json()
        if not isinstance(funds, dict):
            return False
        if "dhanClientId" in funds and str(funds["dhanClientId"]) != str(self.cfg.dhan_client_id):
            return False
        # Start-of-day limit is not evidence of currently available funds.
        balance = next((funds[k] for k in ("availabelBalance", "availableBalance")
                        if k in funds and funds[k] is not None), None)
        if balance is None or isinstance(balance, bool):
            return False
        balance = float(balance)
        if not math.isfinite(balance):
            return False

        # Same readiness formatter and stricter runtime gate evidence.
        # Do not present token recovery as overall readiness while STOP,
        # unresolved intents, missing master or protection blocks remain.
        def safety():
            stop = self.stop_manager.get_state()
            intents = self.db.get_non_terminal_intents()
            unprotected = self.db.get_unprotected_orders()
            return (isinstance(stop, dict) and stop.get("is_stopped") is False
                    and self.gate.is_accepting_entries is True
                    and isinstance(intents, list) and not intents
                    and isinstance(unprotected, list) and not unprotected
                    and self.master_verified() is True)

        if not safety():
            return False
        report = format_morning_health_report(
            self.cfg, now_ts=now, token_verified=True, token_renewed=True,
            funds_verified=True, fallback_funds=False, available_balance=balance,
            master_loaded=True, is_stopped=False, account_identity_status="VERIFIED",
            profile_client_id=profile["profile_client_id"],
            token_expiration_str=profile.get("token_expiration_str", ""),
            token_expiration_ts=expiry, token_expired=False,
            reconciliation_ok=True, recovery_confirmation=True,
        )
        if report is None:
            return False

        # Recheck the marker and authoritative token after network IO.
        # A scheduled green health check or a newer token supersedes us.
        with locked_token_file(str(_path(self.cfg)), timeout_sec=0):
            if _load(self.cfg) != state or fingerprint(read_token(self.cfg.dhan_token_file)) != digest:
                return False
            if not safety():
                return False
            # Atomic claim plus outbox INSERT; queued is not delivered.
            # If already claimed after a crash, clear the stale pending flag.
            identity = "DHAN_TOKEN_RECOVERY:" + _identity(self.cfg) + ":" + digest
            self.db.enqueue_order_notification_once(identity, "DHAN_TOKEN_RECOVERY_VERIFIED", report)
            state.update(pending=False, last_notified_token_fingerprint=digest)
            _save(self.cfg, state)
            return True
