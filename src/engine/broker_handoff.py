"""Account-scoped, cross-process broker mutation handoff for the opt-in tracker.

This module does not authorize LIVE trading. Every order-mutating Dhan HTTP call
must register under the same flock as the configuration apply barrier. A failed
or ambiguous request remains unresolved; never erase it to make a restart pass.
No broker GETs, service restarts, or secret material are performed or recorded.
"""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import secrets
import tempfile
import threading
import time
from typing import Any, Callable, Iterator, Optional

from src.engine.tracker_v2_safety import proc_starttime

_LOCAL_LOCK = threading.RLock()


class BrokerHandoffBlocked(RuntimeError):
    """No broker mutation can be started while evidence is absent or unsafe."""


class BrokerMutationNotSubmitted(BrokerHandoffBlocked):
    """The shared fence rejected mutation before any broker request was sent."""


def _paths(data_dir: str | os.PathLike[str]) -> tuple[Path, Path, Path]:
    root = Path(data_dir).resolve()
    return (
        root / "tracker_v2_broker.lock",
        root / "active_broker_mutations.json",
        root / "config_restart_v2" / "apply_barrier.json",
    )


def _read(path: Path, *, missing_ok: bool = False) -> Optional[dict[str, Any]]:
    if path.is_symlink():
        raise BrokerHandoffBlocked(f"Symlinked safety record: {path.name}")
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except FileNotFoundError:
        if missing_ok:
            return None
        raise BrokerHandoffBlocked(f"Missing safety record: {path.name}") from None
    try:
        with os.fdopen(fd, "rb") as f:
            fd = -1
            raw = f.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            raise BrokerHandoffBlocked(f"Oversized safety record: {path.name}")
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("Object expected")
        return data
    except (ValueError, UnicodeDecodeError, OSError) as exc:
        raise BrokerHandoffBlocked(f"Unreadable safety record {path.name}: {exc}") from None
    finally:
        if fd >= 0:
            os.close(fd)


def _write(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.is_symlink():
        raise BrokerHandoffBlocked(f"Symlinked output safety record: {path.name}")
    raw = json.dumps(data, sort_keys=True, separators=(",", ":")).encode("utf8")
    fd, temp = tempfile.mkstemp(prefix=".broker-handoff-", dir=str(path.parent))
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as f:
            fd = -1
            f.write(raw)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, path)
        df = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(df)
        finally:
            os.close(df)
        if _read(path) != data:
            raise BrokerHandoffBlocked(f"Safety write readback mismatch: {path.name}")
    finally:
        if fd >= 0:
            os.close(fd)
        if os.path.exists(temp):
            os.unlink(temp)


@contextmanager
def _locked(data_dir: str | os.PathLike[str]) -> Iterator[tuple[Path, Path]]:
    lock, ledger, barrier = _paths(data_dir)
    lock.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with _LOCAL_LOCK:
        if lock.is_symlink():
            raise BrokerHandoffBlocked("Symlinked broker interlock")
        fd = os.open(lock, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield ledger, barrier
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)


def _identity() -> tuple[int, int]:
    pid = os.getpid()
    stamp = proc_starttime(pid)
    if not isinstance(stamp, int) or stamp <= 0:
        raise BrokerHandoffBlocked("Cannot prove trading-process PID/starttime")
    return pid, stamp


def _validate_ledger(data: dict[str, Any], account: str, *, require_self: bool = False) -> None:
    if (data.get("schema_version") != 2 or
            data.get("account_id") != str(account) or
            data.get("handoff_verified") is not True or
            not isinstance(data.get("active"), list) or
            not isinstance(data.get("unresolved"), list)):
        raise BrokerHandoffBlocked("Broker ledger malformed, untrusted or wrong account")
    p, s = data.get("producer_pid"), data.get("producer_starttime")
    if type(p) is not int or type(s) is not int or p <= 0 or s <= 0:
        raise BrokerHandoffBlocked("Broker producer identity malformed")
    if require_self and (p, s) != _identity():
        raise BrokerHandoffBlocked("Broker mutator is not the registered trading process")
    if proc_starttime(p) != s:
        raise BrokerHandoffBlocked("Broker producer process identity stale")


def register_producer(data_dir: str | os.PathLike[str], account: str) -> None:
    """Called only from the real trading pipeline, never from a WebUI status read.

    A re-start cannot silently delete an unresolved earlier mutation. The new
    process can publish its identity while the barrier is active, but cannot
    submit an order until independently verified release.
    """
    account = str(account or "").strip()
    if not account:
        raise BrokerHandoffBlocked("No Dhan account identity")
    pid, start = _identity()
    with _locked(data_dir) as (ledger, _):
        previous = _read(ledger, missing_ok=True)
        if previous is not None:
            if (previous.get("schema_version") != 2 or
                    previous.get("account_id") != account or
                    not isinstance(previous.get("active"), list) or
                    not isinstance(previous.get("unresolved"), list)):
                raise BrokerHandoffBlocked("Existing broker ledger cannot be trusted")
            old_pid, old_start = previous.get("producer_pid"), previous.get("producer_starttime")
            if type(old_pid) is not int or type(old_start) is not int:
                raise BrokerHandoffBlocked("Previous broker producer identity malformed")
            if (old_pid, old_start) != (pid, start) and proc_starttime(old_pid) == old_start:
                raise BrokerHandoffBlocked("A second live trading process owns broker handoff")
            active = list(previous["active"])
            unresolved = list(previous["unresolved"])
            # An old process died with transport mutations in flight.
            # Preserve them as UNKNOWN; never claim broker cancellation.
            if (old_pid, old_start) != (pid, start) and active:
                unresolved.extend({
                    "id": item.get("id"), "method": item.get("method"),
                    "at": time.time(), "reason": "PRIOR_PROCESS_EXITED_IN_FLIGHT",
                } for item in active if isinstance(item, dict))
                active = []
        else:
            active, unresolved = [], []
        _write(ledger, {
            "schema_version": 2, "handoff_verified": True,
            "producer_pid": pid, "producer_starttime": start,
            "account_id": account, "active": active, "unresolved": unresolved,
            "registered_at": time.time(),
        })


def _barrier_clear(barrier: Path) -> None:
    status = _read(barrier, missing_ok=True)
    if status is not None and status.get("active") is not False:
        raise BrokerHandoffBlocked("Configuration apply barrier is active or malformed")


@contextmanager
def broker_mutation(data_dir: str | os.PathLike[str], account: str, kind: str) -> Iterator[Callable[[bool], None]]:
    """Register before POST/PUT/DELETE; explicit close only for known response.

    Unknown outcomes remain durably in 'unresolved', blocking configuration
    restarts until an operator reconciles the broker/journal evidence.
    """
    if kind not in ("POST", "PUT", "DELETE"):
        raise ValueError("Not an order mutation")
    mutation_id = secrets.token_hex(16)
    with _locked(data_dir) as (ledger, barrier):
        _barrier_clear(barrier)
        data = _read(ledger)
        _validate_ledger(data, account, require_self=True)
        # Unresolved historical outcomes block configuration restarts, but
        # must NOT disable fresh independently validated protective OCO/cancel
        # work. Dhan duplicate/retry policy remains with the existing BRAIN.
        entry = {"id": mutation_id, "method": kind, "started_at": time.time()}
        data["active"].append(entry)
        _write(ledger, data)

    outcome = {"known": False}
    def finish(known: bool) -> None:
        outcome["known"] = known is True
    try:
        yield finish
    finally:
        with _locked(data_dir) as (ledger, _):
            data = _read(ledger)
            _validate_ledger(data, account, require_self=True)
            ids = [x.get("id") for x in data["active"] if isinstance(x, dict)]
            if mutation_id not in ids:
                raise BrokerHandoffBlocked("Mutation disappeared before broker response finalized")
            data["active"] = [x for x in data["active"] if isinstance(x, dict) and x.get("id") != mutation_id]
            if not outcome["known"]:
                data["unresolved"].append({"id": mutation_id, "method": kind, "at": time.time()})
            _write(ledger, data)


def guarded_request(
    data_dir: str | os.PathLike[str], account: str, method: str,
    send: Callable[[], Any],
    before_send: Optional[Callable[[], Any]] = None,
) -> Any:
    """Register the broker fence before local admission, then attempt HTTP once.

    A failed local admission is provably NOT_SUBMITTED: the broker callback has
    not been entered. Finish the registered handoff as known/no-send so a
    synthetic local 429/503 never becomes an unresolved broker mutation.
    """
    attempted = False
    try:
        with broker_mutation(data_dir, account, method) as finish:
            if before_send is not None:
                try:
                    before_send()
                except Exception:
                    finish(True)  # No HTTP callback, no ambiguous broker outcome.
                    raise
            # Any failure from this point may have transmitted an order.
            attempted = True
            response = send()
            status = getattr(response, "status_code", None)
            # Definite broker responses; auth, 408/429 and server errors remain unknown.
            known = type(status) is int and (200 <= status < 400 or status in (400, 404, 409, 422))
            finish(known)
            return response
    except BrokerHandoffBlocked as exc:
        if not attempted:
            raise BrokerMutationNotSubmitted(str(exc)) from exc
        raise


def acquire_apply_barrier(
    data_dir: str | os.PathLike[str], account: str, change_id: str,
    expected_pid: int, expected_starttime: int,
) -> bool:
    """Atomically admit restart vs. mutations using the same flock."""
    with _locked(data_dir) as (ledger, barrier):
        data = _read(ledger)
        _validate_ledger(data, account)
        if data.get("producer_pid") != expected_pid or data.get("producer_starttime") != expected_starttime:
            raise BrokerHandoffBlocked("Wrong trading-process identity for restart")
        if data["active"] or data["unresolved"]:
            return False
        existing = _read(barrier, missing_ok=True)
        if existing is not None and existing.get("active") is not False:
            raise BrokerHandoffBlocked("Another configuration barrier is held")
        _write(barrier, {
            "active": True, "change_id": str(change_id),
            "owner_pid": os.getpid(), "owner_starttime": _identity()[1],
            "account_id": str(account), "created_at": time.time(),
        })
        return True


def release_apply_barrier(
    data_dir: str | os.PathLike[str], account: str, change_id: str,
    expected_new_pid: int, expected_new_starttime: int,
) -> bool:
    """Only the coordinator may call after exact trusted runtime ACK checks."""
    with _locked(data_dir) as (ledger, barrier):
        data = _read(ledger)
        _validate_ledger(data, account)
        if (data.get("producer_pid"), data.get("producer_starttime")) != (
            expected_new_pid, expected_new_starttime
        ):
            return False
        if data["active"] or data["unresolved"]:
            return False
        state = _read(barrier)
        if (state.get("active") is not True or state.get("change_id") != change_id or
                state.get("account_id") != str(account)):
            return False
        state["active"] = False
        state["released_at"] = time.time()
        _write(barrier, state)
        return True
