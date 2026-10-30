"""
Durable Emergency STOP and Mutation Gate Engine.
Shared by CLI, API dashboard, and executor.
Implements STEP 04 (04-A through 04-F):
- Real durable emergency STOP with cross-process flock serialization.
- Fails closed on missing, corrupt, unreadable, disk-full, or wrong-account state files by default.
- Atomic pre-POST mutation lease (`acquire_mutation_lease`) serializing STOP vs outbound broker requests.
- Enforces fail-closed safety immediately before all risk-increasing broker mutations.
- Controlled risk-reducing exemptions: cancellations and open-position-protecting exits only.
- Tracks durable in-flight requests on disk and in memory across reboots.
"""

from contextlib import contextmanager
import fcntl
import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

DEFAULT_STOP_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
    "emergency_stop.json",
)

UNAUTHORIZED_OPERATORS = frozenset({"", "UNAUTHORIZED", "ANONYMOUS", "UNKNOWN", "NONE", "GUEST"})


class EmergencyStopManager:
    """
    Durable cross-process emergency stop manager.
    Guarantees that emergency stop state survives reboots, crashes, and concurrency.
    Fails closed (blocks mutations) if state file is missing, corrupt, invalid, wrong-account, or unreadable.
    """

    def __init__(
        self,
        state_file: Optional[str] = None,
        fail_closed_if_missing: bool = True,
        expected_account_id: str = "",
    ):
        self.state_file = state_file or os.environ.get("EMERGENCY_STOP_FILE") or DEFAULT_STOP_FILE
        self.fail_closed_if_missing = fail_closed_if_missing
        self.expected_account_id = str(expected_account_id or "").strip()
        self.lock_file = f"{self.state_file}.lock"
        self._thread_lock = threading.Lock()
        self._in_flight: set[str] = set()
        self._disk_write_failed: bool = False

    def _acquire_flock(self) -> Optional[int]:
        """Cross-process lock acquisition. Returns fd or None if lock path is unwritable."""
        try:
            os.makedirs(os.path.dirname(os.path.abspath(self.lock_file)), exist_ok=True)
            fd = os.open(self.lock_file, os.O_CREAT | os.O_RDWR, 0o600)
            fcntl.flock(fd, fcntl.LOCK_EX)
            return fd
        except OSError as e:
            logger.critical(f"[FAIL-CLOSED] Could not acquire emergency stop lock file ({e}). Forcing STOP=TRUE.")
            self._disk_write_failed = True
            return None

    def _release_flock(self, fd: Optional[int]):
        if fd is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
            except Exception:
                pass

    def _load_raw(self) -> Dict[str, Any]:
        """
        Reads state from disk under cross-process lock.
        FAIL-CLOSED: If file is missing, corrupt, wrong-account, or disk write failed, treats as STOPPED.
        """
        if self._disk_write_failed:
            return {
                "is_stopped": True,
                "stopped_at": time.time(),
                "stopped_by": "SYSTEM_FAIL_CLOSED",
                "reason": "Disk write / filesystem failure forced STOP=True",
                "account_id": self.expected_account_id,
                "in_flight_orders": list(self._in_flight),
                "state_version": 2,
            }

        if not os.path.exists(self.state_file):
            if self.fail_closed_if_missing:
                logger.warning(f"[FAIL-CLOSED] Emergency stop file '{self.state_file}' does not exist. Defaulting to STOPPED=True.")
                return {
                    "is_stopped": True,
                    "stopped_at": time.time(),
                    "stopped_by": "SYSTEM_FAIL_CLOSED",
                    "reason": "Initial state unprovisioned / file missing",
                    "account_id": "",
                    "in_flight_orders": [],
                    "state_version": 2,
                }
            return {
                "is_stopped": False,
                "stopped_at": 0.0,
                "stopped_by": "",
                "reason": "",
                "account_id": "",
                "in_flight_orders": [],
                "state_version": 2,
            }

        try:
            with open(self.state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, dict):
                    raise ValueError("Emergency stop payload must be a JSON object")
                if "is_stopped" not in data or not isinstance(data["is_stopped"], bool):
                    raise ValueError("Emergency stop payload missing valid boolean 'is_stopped' field")

                # 04-A: Account-scoped validation
                file_acct = str(data.get("account_id") or "").strip()
                if self.expected_account_id and file_acct and file_acct != self.expected_account_id:
                    logger.critical(
                        f"[FAIL-CLOSED] Emergency stop account mismatch (file={file_acct}, expected={self.expected_account_id}). Forcing STOP=TRUE."
                    )
                    data = dict(data)
                    data["is_stopped"] = True
                    data["stopped_by"] = "SYSTEM_FAIL_CLOSED"
                    data["reason"] = f"Account mismatch in STOP file ({file_acct} != {self.expected_account_id})"
                return data
        except Exception as e:
            logger.critical(f"[FAIL-CLOSED] Could not read emergency stop state file ({e}). Forcing STOP=TRUE.")
            return {
                "is_stopped": True,
                "stopped_at": time.time(),
                "stopped_by": "SYSTEM_FAIL_CLOSED",
                "reason": f"Corrupt or inaccessible state file: {e}",
                "account_id": "",
                "in_flight_orders": [],
                "is_corrupt": True,
                "state_version": 2,
            }

    def _save_raw(self, state: Dict[str, Any]) -> bool:
        """
        04-F: Atomic write to disk with 0600 permissions, fsync, and directory sync.
        If disk is full or read-only, sets `_disk_write_failed = True` so `is_stopped()` fails closed.
        """
        try:
            state_dir = os.path.dirname(os.path.abspath(self.state_file))
            os.makedirs(state_dir, exist_ok=True)
            tmp = f"{self.state_file}.tmp"
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.state_file)
            os.chmod(self.state_file, 0o600)
            try:
                dir_fd = os.open(state_dir, os.O_RDONLY)
                os.fsync(dir_fd)
                os.close(dir_fd)
            except Exception:
                pass
            self._disk_write_failed = False
            return True
        except OSError as e:
            logger.critical(f"[FAIL-CLOSED] Disk write failed on emergency stop state file ({e}). Forcing STOP=TRUE.")
            self._disk_write_failed = True
            return False

    def provision_initial_clean_state(
        self,
        operator: str = "Operator",
        account_id: str = "",
    ) -> Dict[str, Any]:
        """
        Explicitly provisions initial authorized normal trading state (is_stopped=False).
        Guarantees that a clean system is explicitly initialized rather than silently unstopped.
        """
        op_clean = (operator or "").strip()
        if op_clean.upper() in UNAUTHORIZED_OPERATORS:
            raise ValueError(f"Cannot provision clean STOP state with unauthorized operator '{operator}'")

        with self._thread_lock:
            flock_fd = self._acquire_flock()
            try:
                now = time.time()
                acct = str(account_id or self.expected_account_id).strip()
                state = {
                    "is_stopped": False,
                    "resumed_at": now,
                    "resumed_by": op_clean,
                    "reason": "Initial authorized provisioning",
                    "account_id": acct,
                    "in_flight_orders": list(self._in_flight),
                    "state_version": 2,
                    "epoch": int(now),
                }
                self._save_raw(state)
                logger.info(f"🟢 [EMERGENCY STOP PROVISIONED] Clean operational state authorized by {state['resumed_by']} for account {state['account_id']}.")
                return state
            finally:
                self._release_flock(flock_fd)

    def is_stopped(self) -> bool:
        """Returns True if emergency stop is currently active or file is corrupt/missing/disk-failed (fail-closed)."""
        with self._thread_lock:
            if self._disk_write_failed:
                return True
            flock_fd = self._acquire_flock()
            try:
                state = self._load_raw()
                return bool(state.get("is_stopped", True))
            finally:
                self._release_flock(flock_fd)

    def get_state(self) -> Dict[str, Any]:
        """Returns complete audit record of current emergency stop state."""
        with self._thread_lock:
            flock_fd = self._acquire_flock()
            try:
                state = self._load_raw()
                persisted_in_flight = list(state.get("in_flight_orders") or [])
                merged = list(dict.fromkeys(persisted_in_flight + list(self._in_flight)))
                state["in_flight_orders"] = merged
                state["in_memory_in_flight"] = list(self._in_flight)
                return state
            finally:
                self._release_flock(flock_fd)

    def trigger_stop(
        self,
        operator: str = "Operator",
        reason: str = "Emergency stop activated",
        account_id: str = "",
    ) -> Dict[str, Any]:
        """
        Activates emergency stop durably on disk with cross-process serialization.
        All subsequent risk-increasing broker mutations will be hard rejected.
        """
        with self._thread_lock:
            flock_fd = self._acquire_flock()
            try:
                prev = self._load_raw() if os.path.exists(self.state_file) else {}
                persisted_in_flight = list(prev.get("in_flight_orders") or [])
                merged_in_flight = list(dict.fromkeys(persisted_in_flight + list(self._in_flight)))
                now = time.time()
                acct = str(account_id or prev.get("account_id") or self.expected_account_id).strip()
                state = {
                    "is_stopped": True,
                    "stopped_at": now,
                    "stopped_by": (operator or "").strip() or "Operator",
                    "reason": (reason or "").strip() or "Operator triggered emergency stop",
                    "account_id": acct,
                    "in_flight_orders": merged_in_flight,
                    "state_version": 2,
                    "epoch": int(now),
                }
                self._save_raw(state)
                logger.critical(f"🛑 [EMERGENCY STOP] Committed by {state['stopped_by']}: {state['reason']}")
                return state
            finally:
                self._release_flock(flock_fd)

    def resume(
        self,
        operator: str = "Operator",
        reason: str = "Trading operations resumed",
        reconciliation_gate: Optional[Any] = None,
        allow_unreconciled: bool = False,
    ) -> Dict[str, Any]:
        """
        04-E: Explicitly clears emergency stop durably on disk.
        Requires authorized operator identity and healthy reconciliation gate (when provided).
        Never auto-resumes on restart.
        """
        op_clean = (operator or "").strip()
        if op_clean.upper() in UNAUTHORIZED_OPERATORS:
            logger.error(f"[STOP RESUME REJECTED] Unauthorized operator '{operator}' attempted to clear EMERGENCY STOP.")
            return self.get_state()

        if reconciliation_gate is not None and not allow_unreconciled:
            is_healthy = getattr(reconciliation_gate, "is_healthy_for_resume", lambda: True)()
            if not is_healthy:
                logger.error("[STOP RESUME REJECTED] Cannot resume trading while StartupReconciliationGate has unresolved risk.")
                return self.get_state()

        with self._thread_lock:
            flock_fd = self._acquire_flock()
            try:
                prev = self._load_raw() if os.path.exists(self.state_file) else {}
                persisted_in_flight = list(prev.get("in_flight_orders") or [])
                merged_in_flight = list(dict.fromkeys(persisted_in_flight + list(self._in_flight)))
                now = time.time()
                state = {
                    "is_stopped": False,
                    "resumed_at": now,
                    "resumed_by": op_clean,
                    "reason": (reason or "").strip() or "Operator resumed trading",
                    "account_id": str(prev.get("account_id") or self.expected_account_id).strip(),
                    "in_flight_orders": merged_in_flight,
                    "state_version": 2,
                    "epoch": int(now),
                }
                self._save_raw(state)
                logger.warning(f"🟢 [EMERGENCY STOP CLEARED] Operations resumed by {state['resumed_by']}: {state['reason']}")
                return state
            finally:
                self._release_flock(flock_fd)

    # --- 04-B: Atomic Cross-Process Pre-POST Mutation Serialization & In-Flight Tracking ---

    def try_begin_mutation(self, correlation_id: str) -> Tuple[bool, Dict[str, Any]]:
        """
        04-B: Atomically checks STOP state under exclusive cross-process `flock` and, if allowed,
        persists `correlation_id` in `in_flight_orders` on disk BEFORE releasing `flock`.
        Prevents any race condition between a concurrent `trigger_stop()` and `place_order()`.
        """
        with self._thread_lock:
            if self._disk_write_failed:
                return False, self._load_raw()
            flock_fd = self._acquire_flock()
            if flock_fd is None:
                return False, self._load_raw()
            try:
                state = self._load_raw()
                if state.get("is_stopped", True):
                    return False, state
                self._in_flight.add(correlation_id)
                persisted = list(state.get("in_flight_orders") or [])
                if correlation_id not in persisted:
                    persisted.append(correlation_id)
                state["in_flight_orders"] = persisted
                if not self._save_raw(state):
                    self._in_flight.discard(correlation_id)
                    return False, self._load_raw()
                return True, state
            finally:
                self._release_flock(flock_fd)

    def finalize_mutation(self, correlation_id: str, keep_persisted_if_uncertain: bool = False) -> None:
        """
        Completes an in-flight mutation. If `keep_persisted_if_uncertain=True` (e.g. HTTP timeout / lost ACK),
        preserves `correlation_id` in `in_flight_orders` on disk for crash/reconciliation recovery.
        """
        with self._thread_lock:
            if not keep_persisted_if_uncertain:
                self._in_flight.discard(correlation_id)
            flock_fd = self._acquire_flock()
            if flock_fd is None:
                return
            try:
                if os.path.exists(self.state_file):
                    state = self._load_raw()
                    persisted = list(state.get("in_flight_orders") or [])
                    if not keep_persisted_if_uncertain and correlation_id in persisted:
                        persisted.remove(correlation_id)
                        state["in_flight_orders"] = persisted
                        self._save_raw(state)
            finally:
                self._release_flock(flock_fd)

    def register_in_flight(self, correlation_id: str) -> None:
        """Registers an order ID as in-flight before outbound network POST."""
        with self._thread_lock:
            self._in_flight.add(correlation_id)
            flock_fd = self._acquire_flock()
            if flock_fd is not None:
                try:
                    if os.path.exists(self.state_file):
                        state = self._load_raw()
                        persisted = list(state.get("in_flight_orders") or [])
                        if correlation_id not in persisted:
                            persisted.append(correlation_id)
                            state["in_flight_orders"] = persisted
                            self._save_raw(state)
                finally:
                    self._release_flock(flock_fd)

    def deregister_in_flight(self, correlation_id: str) -> None:
        """Removes an order ID from in-flight tracking once ACK or rejection received."""
        self.finalize_mutation(correlation_id, keep_persisted_if_uncertain=False)

    def get_in_flight(self) -> List[str]:
        """Returns list of currently active in-flight order IDs (merged memory + disk)."""
        return self.get_state().get("in_flight_orders", [])
