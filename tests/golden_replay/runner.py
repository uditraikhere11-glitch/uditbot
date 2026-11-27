"""Production-path Golden Replay runner with derived policy & per-scenario metrics (P3-C15, P3-C16)."""
from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import date
import hashlib
import json
from pathlib import Path
import tempfile
import time
from typing import Any, Mapping

from src.broker.broker_snapshot import (
    LIVE_BOOKS,
    NS,
    BrokerSnapshot,
    ContractKey,
    EvidenceRequest,
    Purpose,
    SessionContext,
    SessionPhase,
)
from src.broker.dhan_client import DhanClient
from src.broker.dhan_snapshot_source import DhanSnapshotSource
from src.broker.read_governor import ReadGovernor
from src.broker.snapshot_coordinator import CrossProcessReadCoordinator
from src.config import AppConfig, TradingMode
from src.engine.broker_ledger_reconciler import (
    AccountVerdict,
    BrokerLedgerReconciler,
    PolicyContext,
    ReconciliationVerdict,
)
from src.engine.journal_view import JournalView, read_journal_view
from src.journal.db import JournalDB

from .clocks import DeterministicClock
from .journal_seed import seed_journal_from_spec
from .transport import ScriptedResponse, SimulatedHTTPTransport

FIXTURES_ROOT = Path(__file__).resolve().parent.parent / "fixtures" / "golden_replay"


def make_synthetic_jwt(account_id: str, *, exp_unix: int, token_epoch: int = 1) -> str:
    """Create a syntactically valid synthetic JWT for offline DhanClient token verification."""
    hdr = base64.urlsafe_b64encode(json.dumps({"alg": "HS256", "typ": "JWT"}).encode()).rstrip(b"=").decode()
    pay = base64.urlsafe_b64encode(
        json.dumps({"dhanClientId": str(account_id), "exp": int(exp_unix), "epoch": int(token_epoch)}).encode()
    ).rstrip(b"=").decode()
    sig = base64.urlsafe_b64encode(f"synthetic-sig-{token_epoch}".encode()).rstrip(b"=").decode()
    return f"{hdr}.{pay}.{sig}"


@dataclass
class ReplayMetrics:
    scenario_id: str
    variant: str
    logical_read_requests: int = 0
    actual_http_sends: int = 0
    coalesced_requests: int = 0
    cache_reuses: int = 0
    profile_reads: int = 0
    historical_page_reads: int = 0
    local_pacing_deferrals: int = 0
    true_throttles: int = 0
    half_open_probes: int = 0
    independent_confirmations: int = 0
    mutation_attempts: int = 0
    actual_mutation_sends: int = 0
    generation_invalidations: int = 0
    acquisition_span_ms: float = 0.0
    receipt_skew_ms: float = 0.0
    virtual_recovery_s: float = 0.0
    real_runtime_ms: float = 0.0


class ReplayEnvironment:
    """Isolated offline production-path environment for a single Golden Replay run."""

    def __init__(
        self,
        *,
        account_id: str,
        clock: DeterministicClock,
        journal_spec: Mapping[str, Any] | None = None,
        token_epoch: int = 1,
        token_ttl_seconds: int = 3600,
    ) -> None:
        self._tmpdir = tempfile.TemporaryDirectory(prefix="uditbot_p3_")
        self._t0 = time.monotonic()
        try:
            self._init_inner(
                account_id=account_id,
                clock=clock,
                journal_spec=journal_spec,
                token_epoch=token_epoch,
                token_ttl_seconds=token_ttl_seconds,
            )
        except Exception:
            self._tmpdir.cleanup()
            raise

    def _init_inner(
        self,
        *,
        account_id: str,
        clock: DeterministicClock,
        journal_spec: Mapping[str, Any] | None,
        token_epoch: int,
        token_ttl_seconds: int,
    ) -> None:
        self.root = Path(self._tmpdir.name)
        self.data_dir = self.root / "data"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / "trade_journal.db"
        self.token_path = self.root / ".dhan_token"
        self.emergency_stop_path = self.data_dir / "emergency_stop.json"
        self.owner_state_path = self.data_dir / "service_control.json"
        self.account_id = str(account_id)
        self.clock = clock
        self.token_epoch = int(token_epoch)
        self._token_ttl = int(token_ttl_seconds)

        # Write synthetic private token file
        self.current_token = make_synthetic_jwt(
            self.account_id,
            exp_unix=int(self.clock.time()) + self._token_ttl,
            token_epoch=self.token_epoch,
        )
        self.token_path.write_text(self.current_token, encoding="utf-8")
        self.token_path.chmod(0o600)

        # Write real authority state files on disk (for P3-C15 derived policy)
        self.mutation_ledger_path = self.data_dir / "active_broker_mutations.json"
        self.set_mutation_ledger(active=[], unresolved=[], handoff_verified=True)
        self.set_emergency_stop(active=False)
        self.set_owner_state(desired="RUNNING", observed="RUNNING", deferred_stop_pending=False)

        # Seed real production-schema SQLite WAL journal
        self.db: JournalDB = seed_journal_from_spec(
            self.db_path, account_id=self.account_id, spec=journal_spec or {}
        )

        # Build real AppConfig & real DhanClient with SimulatedHTTPTransport
        self.cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id=self.account_id,
            dhan_access_token=self.current_token,
            dhan_token_file=str(self.token_path),
            data_dir=str(self.data_dir),
            journal_db_path=str(self.db_path),
        )
        self.transport = SimulatedHTTPTransport(
            self.clock,
            account_id=self.account_id,
            expected_token=self.current_token,
            token_epoch_label=f"epoch-{self.token_epoch}",
        )
        self.governor = ReadGovernor(
            self.data_dir / "broker_read_control",
            self.account_id,
            clock=self.clock.time,
            monotonic_clock=self.clock.monotonic,
            boot_id=self.clock.boot_id,
        )
        self.client = DhanClient(self.cfg, auto_generate_token=False)
        self.client.session.close()
        self.client.session = self.transport
        self.client._read_governor = self.governor

        self._session_context: SessionContext = self.clock.session_context(
            phase=SessionPhase.SETTLED, calendar_verified=True
        )
        self.source = DhanSnapshotSource(
            self.client,
            lambda: self._session_context,
            utc=self.clock.time_ns,
            mono=self.clock.monotonic_ns,
        )
        self.coordinator: CrossProcessReadCoordinator = self.source.coordinator()
        self.reconciler = BrokerLedgerReconciler()
        self.metrics = ReplayMetrics(scenario_id="", variant="")
        self._last_snapshot_ids: set[str] = set()

    def set_session(self, session: SessionContext) -> None:
        self._session_context = session

    def set_emergency_stop(self, *, active: bool) -> None:
        self.emergency_stop_path.write_text(
            json.dumps({"stopped": bool(active), "account_id": self.account_id}),
            encoding="utf-8",
        )

    def set_owner_state(self, *, desired: str, observed: str, deferred_stop_pending: bool = False) -> None:
        self.owner_state_path.write_text(
            json.dumps({
                "desired_service_state": desired,
                "observed_state": observed,
                "deferred_stop_pending": bool(deferred_stop_pending),
            }),
            encoding="utf-8",
        )

    def rotate_token(self, *, new_epoch: int, valid_on_transport: bool = True, ttl_seconds: int = 3600) -> str:
        self.token_epoch = int(new_epoch)
        self.current_token = make_synthetic_jwt(
            self.account_id,
            exp_unix=int(self.clock.time()) + int(ttl_seconds),
            token_epoch=self.token_epoch,
        )
        self.token_path.write_text(self.current_token, encoding="utf-8")
        self.token_path.chmod(0o600)
        self.cfg.dhan_access_token = self.current_token
        if valid_on_transport:
            self.transport.set_token_expectation(self.current_token, epoch_label=f"epoch-{self.token_epoch}")
        return self.current_token

    def set_mutation_ledger(
        self,
        *,
        active: list[dict[str, Any]],
        unresolved: list[dict[str, Any]],
        handoff_verified: bool = True,
    ) -> None:
        payload = {
            "schema_version": 2,
            "account_id": self.account_id,
            "handoff_verified": bool(handoff_verified),
            "active": list(active),
            "unresolved": list(unresolved),
        }
        self.mutation_ledger_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def register_books(
        self,
        *,
        profile: Any = None,
        holdings: Any = None,
        positions: Any = None,
        trade_book: Any = None,
        order_book: Any = None,
        forever_orders: Any = None,
        super_orders: Any = None,
        funds: Any = None,
        history_pages: Mapping[str, Any] | None = None,
        order_statuses: Mapping[str, Any] | None = None,
        trades_for_order: Mapping[str, Any] | None = None,
    ) -> None:
        def _wrap(val: Any, default: Any) -> ScriptedResponse:
            if isinstance(val, ScriptedResponse):
                return val
            return ScriptedResponse(status_code=200, payload=default if val is None else val)

        self.transport.register("/v2/profile", _wrap(profile, {"dhanClientId": self.account_id}))
        self.transport.register("/v2/holdings", _wrap(holdings, []))
        self.transport.register("/v2/positions", _wrap(positions, []))
        self.transport.register("/v2/trades", _wrap(trade_book, []))
        self.transport.register("/v2/orders", _wrap(order_book, []))
        self.transport.register("/v2/forever/orders", _wrap(forever_orders, []))
        self.transport.register("/v2/super/orders", _wrap(super_orders, []))
        self.transport.register("/v2/fundlimit", _wrap(funds, {"availabelBalance": 100000.0}))

        for path, val in (history_pages or {}).items():
            ep = path if path.startswith("/v2/") else f"/v2/trades/{path}"
            self.transport.register(ep, _wrap(val, []))
        for oid, val in (order_statuses or {}).items():
            self.transport.register(f"/v2/orders/{oid}", _wrap(val, {}))
        for oid, val in (trades_for_order or {}).items():
            self.transport.register(f"/v2/trades/{oid}", _wrap(val, []))

    def acquire_snapshot(
        self,
        *,
        purpose: Purpose = Purpose.ENTRY,
        required_books: frozenset[str] = LIVE_BOOKS,
        history_ranges: tuple[tuple[date, date], ...] = (),
        order_status_ids: tuple[str, ...] = (),
        trade_order_ids: tuple[str, ...] = (),
        min_capture_mono_ns: int = 0,
        independent_after_snapshot_id: str | None = None,
        deadline_offset_s: float = 10.0,
        priority: str = "normal",
    ) -> Any:
        self.metrics.logical_read_requests += 1
        before_sends = self.transport.count_sends()
        req = EvidenceRequest(
            account_id=self.account_id,
            purpose=purpose,
            required_books=required_books,
            history_ranges=history_ranges,
            order_status_ids=order_status_ids,
            trade_order_ids=trade_order_ids,
            min_capture_mono_ns=min_capture_mono_ns,
            independent_after_snapshot_id=independent_after_snapshot_id,
            deadline_mono_ns=self.clock.monotonic_ns() + int(deadline_offset_s * NS),
            priority=priority,
        )
        res = self.coordinator.acquire(req)
        after_sends = self.transport.count_sends()
        self.metrics.actual_http_sends = after_sends
        self.metrics.profile_reads = self.transport.count_sends(endpoint="/v2/profile")
        self.metrics.historical_page_reads = sum(
            1 for h in self.transport.history() if h.endpoint.startswith("/v2/trades/") and h.endpoint.count("/") >= 5
        )
        self.metrics.actual_mutation_sends = len(self.transport.mutation_sends)
        if isinstance(res, BrokerSnapshot):
            if res.snapshot_id in self._last_snapshot_ids and after_sends == before_sends:
                self.metrics.cache_reuses += 1
            else:
                self._last_snapshot_ids.add(res.snapshot_id)
            if independent_after_snapshot_id:
                self.metrics.independent_confirmations += 1
            self.metrics.acquisition_span_ms = (res.capture_finished_mono_ns - res.capture_started_mono_ns) / 1e6
            live_times = [
                b.received_mono_ns
                for b in (res.holdings, res.positions, res.trade_book, res.order_book, res.forever_orders, res.super_orders)
                if b.received_mono_ns is not None
            ]
            if len(live_times) >= 2:
                self.metrics.receipt_skew_ms = (max(live_times) - min(live_times)) / 1e6
        self.metrics.real_runtime_ms = (time.monotonic() - self._t0) * 1000.0
        return res

    def read_journal_view(self, *, journal_epoch: int = 1) -> JournalView:
        return read_journal_view(
            self.db_path,
            account_id=self.account_id,
            journal_epoch=journal_epoch,
            read_utc_ns=self.clock.time_ns(),
        )

    def derive_policy_from_environment(
        self,
        *,
        purpose: Purpose,
        journal_view: JournalView,
        owner_intent_epoch: int,
        entry_fence_active: bool = False,
        transition_active: bool = False,
        min_capture_mono_ns: int = 0,
        holdings_confirmation: BrokerSnapshot | None = None,
        service_identity_verified: bool = True,
        watcher_liveness_verified: bool = True,
        risk_funds_mode_checks_passed: bool = True,
    ) -> PolicyContext:
        """P3-C15: Derive PolicyContext from actual on-disk authority state files & JournalView."""
        mut_data = json.loads(self.mutation_ledger_path.read_text(encoding="utf-8")) if self.mutation_ledger_path.is_file() else {}
        mut_verified = bool(mut_data.get("handoff_verified") is True and mut_data.get("account_id") == self.account_id)
        active_muts = tuple(str(m.get("id", m)) if isinstance(m, dict) else str(m) for m in mut_data.get("active", []))
        unknown_muts = tuple(str(m.get("id", m)) if isinstance(m, dict) else str(m) for m in mut_data.get("unresolved", []))

        stop_data = json.loads(self.emergency_stop_path.read_text(encoding="utf-8")) if self.emergency_stop_path.is_file() else {"stopped": True}
        emergency_stopped = bool(stop_data.get("stopped", True))

        owner_data = json.loads(self.owner_state_path.read_text(encoding="utf-8")) if self.owner_state_path.is_file() else {}
        desired = str(owner_data.get("desired_service_state") or "UNKNOWN")
        observed = str(owner_data.get("observed_state") or "UNKNOWN")
        deferred = bool(owner_data.get("deferred_stop_pending", False))

        return PolicyContext(
            mode=self.cfg.trading_mode.value,
            purpose=purpose,
            owner_desired_state=desired,
            owner_observed_state=observed,
            deferred_stop_pending=deferred,
            owner_intent_epoch=owner_intent_epoch,
            entry_fence_active=entry_fence_active,
            emergency_stop_active=emergency_stopped,
            transition_active=transition_active,
            mutation_fence_verified=mut_verified,
            active_mutation_ids=active_muts,
            unknown_mutation_ids=unknown_muts,
            service_identity_verified=service_identity_verified,
            watcher_liveness_verified=watcher_liveness_verified,
            risk_funds_mode_checks_passed=risk_funds_mode_checks_passed,
            journal_readback_verified=bool(journal_view.read_complete and journal_view.digest_sha256),
            min_capture_mono_ns=min_capture_mono_ns,
            holdings_confirmation=holdings_confirmation,
        )

    def default_policy(self, **kwargs: Any) -> PolicyContext:
        """Backward-compatible helper delegating to environment-derived policy."""
        jv = self.read_journal_view(journal_epoch=1)
        return self.derive_policy_from_environment(
            purpose=kwargs.get("purpose", Purpose.ENTRY),
            journal_view=jv,
            owner_intent_epoch=kwargs.get("owner_intent_epoch", 0),
            entry_fence_active=kwargs.get("entry_fence_active", False),
            transition_active=kwargs.get("transition_active", False),
            min_capture_mono_ns=kwargs.get("min_capture_mono_ns", 0),
            holdings_confirmation=kwargs.get("holdings_confirmation", None),
        )

    def reconcile_security(
        self,
        snapshot: BrokerSnapshot,
        contract: ContractKey,
        *,
        policy: PolicyContext | None = None,
        journal_view: JournalView | None = None,
    ) -> ReconciliationVerdict:
        jv = journal_view or self.read_journal_view(journal_epoch=snapshot.generation.journal_epoch)
        pol = policy or self.derive_policy_from_environment(
            purpose=Purpose.ENTRY, journal_view=jv, owner_intent_epoch=snapshot.generation.owner_epoch
        )
        return self.reconciler.reconcile_security(snapshot, jv, contract, pol, self.clock.monotonic_ns())

    def reconcile_account(
        self,
        snapshot: BrokerSnapshot,
        *,
        policy: PolicyContext | None = None,
        journal_view: JournalView | None = None,
    ) -> AccountVerdict:
        jv = journal_view or self.read_journal_view(journal_epoch=snapshot.generation.journal_epoch)
        pol = policy or self.derive_policy_from_environment(
            purpose=Purpose.ENTRY, journal_view=jv, owner_intent_epoch=snapshot.generation.owner_epoch
        )
        return self.reconciler.reconcile_account(snapshot, jv, pol, self.clock.monotonic_ns())

    def close(self) -> None:
        try:
            self.client.session.close()
        finally:
            self._tmpdir.cleanup()

    def __enter__(self) -> "ReplayEnvironment":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def load_scenario_bundle(scenario_id: str) -> dict[str, Any]:
    sdir = FIXTURES_ROOT / scenario_id
    if not sdir.is_dir():
        raise FileNotFoundError(f"Golden Replay fixture directory missing: {sdir}")
    return {
        "dir": sdir,
        "scenario": json.loads((sdir / "scenario.json").read_text(encoding="utf-8")),
        "provenance": json.loads((sdir / "provenance.json").read_text(encoding="utf-8")),
        "expected": json.loads((sdir / "expected_results.json").read_text(encoding="utf-8")),
        "journal_seed": json.loads((sdir / "journal_seed" / "seed.json").read_text(encoding="utf-8")),
        "owner_state": json.loads((sdir / "owner_state" / "state.json").read_text(encoding="utf-8")),
        "governor_seed": json.loads((sdir / "governor_seed" / "seed.json").read_text(encoding="utf-8")),
        "mutation_ledger": json.loads((sdir / "mutation_ledger" / "ledger.json").read_text(encoding="utf-8")),
        "captures": json.loads((sdir / "captures" / "bundle.json").read_text(encoding="utf-8")),
    }


def verify_fixture_checksums(fixtures_root: Path = FIXTURES_ROOT) -> tuple[bool, list[str]]:
    errors: list[str] = []
    manifest_path = fixtures_root / "manifest.json"
    if not manifest_path.is_file():
        return False, ["Missing manifest.json"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for sc in manifest.get("scenarios", []):
        sid = sc["scenario_id"]
        sums_path = fixtures_root / sid / "SHA256SUMS.txt"
        if not sums_path.is_file():
            errors.append(f"{sid}: missing SHA256SUMS.txt")
            continue
        for line in sums_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            expected_hash, rel_path = line.split("  ", 1)
            target = fixtures_root / sid / rel_path
            if not target.is_file():
                errors.append(f"{sid}: missing {rel_path}")
                continue
            actual = hashlib.sha256(target.read_bytes()).hexdigest()
            if actual != expected_hash:
                errors.append(f"{sid}: checksum mismatch on {rel_path}")
    return len(errors) == 0, errors
