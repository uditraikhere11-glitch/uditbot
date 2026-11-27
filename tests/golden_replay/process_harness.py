"""Real multi-process concurrency harness for Phase 3 (P3-C12: both coordinator & full DhanClient stack)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import multiprocessing
import os
from pathlib import Path
import time
from typing import Any

from src.broker.broker_snapshot import (
    LIVE_BOOKS,
    NS,
    BrokerSnapshot,
    EvidenceRequest,
    Purpose,
    SessionContext,
    SessionPhase,
)
from src.broker.dhan_client import DhanClient
from src.broker.dhan_snapshot_source import DhanSnapshotSource
from src.broker.read_governor import ReadGovernor
from src.config import AppConfig, TradingMode

from .clocks import IST, DeterministicClock
from .transport import ScriptedResponse, SimulatedHTTPTransport


@dataclass(frozen=True)
class ProcessWorkerResult:
    worker_name: str
    pid: int
    ok: bool
    snapshot_id: str | None
    collected_locally: bool
    http_sends: int
    error: str | None
    elapsed_ms: float


def _real_dhan_stack_worker(
    queue: Any,
    worker_name: str,
    data_dir: str,
    db_path: str,
    token_path: str,
    account_id: str,
    purpose_name: str,
    collect_delay_s: float,
    independent_after_sid: str | None,
    read_only_display: bool,
    kill_during_collect: bool,
    wait_for_leader_event: Any = None,
    leader_entered_event: Any = None,
    follower_ready_event: Any = None,
    release_leader_event: Any = None,
) -> None:
    """P3-C12: Real OS process using actual DhanClient -> ReadGovernor -> DhanSnapshotSource -> CrossProcessReadCoordinator."""
    t0 = time.monotonic()
    http_sends = 0
    try:
        token = Path(token_path).read_text(encoding="utf-8").strip()
        cfg = AppConfig(
            trading_mode=TradingMode.LIVE,
            dhan_client_id=account_id,
            dhan_access_token=token,
            dhan_token_file=token_path,
            data_dir=data_dir,
            journal_db_path=db_path,
        )
        clk = DeterministicClock(initial_utc=datetime(2026, 11, 26, 10, 0, 0, tzinfo=IST))
        transport = SimulatedHTTPTransport(clk, account_id=account_id, expected_token=token)
        valid_until = (datetime.now(IST) + timedelta(hours=12)).strftime("%d/%m/%Y %H:%M")

        def _profile_handler(*_a):
            if leader_entered_event is not None:
                leader_entered_event.set()
            if release_leader_event is not None:
                release_leader_event.wait(timeout=5.0)
            elif collect_delay_s > 0:
                time.sleep(collect_delay_s)
            if kill_during_collect:
                os._exit(137)
            return ScriptedResponse(
                status_code=200,
                payload={"dhanClientId": account_id, "tokenValidity": valid_until},
                latency_seconds=0.0,
            )

        transport.register("/v2/profile", _profile_handler)
        for ep in ("/v2/holdings", "/v2/positions", "/v2/trades", "/v2/orders", "/v2/forever/orders", "/v2/super/orders"):
            transport.register(ep, ScriptedResponse(status_code=200, payload=[], latency_seconds=0.0))

        gov = ReadGovernor(Path(data_dir) / "broker_read_control", account_id, boot_id="shared-p3-boot")
        client = DhanClient(cfg, auto_generate_token=False)
        client.session.close()
        client.session = transport
        client._read_governor = gov

        source = DhanSnapshotSource(
            client,
            lambda: SessionContext(SessionPhase.SETTLED, date(2026, 11, 26), time.time_ns(), "a" * 64, True),
            utc=time.time_ns,
            mono=time.monotonic_ns,
        )
        coord = source.coordinator()
        try:
            if read_only_display:
                snap = coord.peek_display(coord.account_key)
                http_sends = transport.count_sends()
                queue.put(
                    ProcessWorkerResult(
                        worker_name=worker_name,
                        pid=os.getpid(),
                        ok=True,
                        snapshot_id=snap.snapshot_id if snap is not None else None,
                        collected_locally=False,
                        http_sends=http_sends,
                        error=None,
                        elapsed_ms=(time.monotonic() - t0) * 1000.0,
                    )
                )
                return

            if wait_for_leader_event is not None:
                wait_for_leader_event.wait(timeout=5.0)
            if follower_ready_event is not None:
                follower_ready_event.set()

            req = EvidenceRequest(
                account_id=account_id,
                purpose=Purpose[purpose_name],
                required_books=LIVE_BOOKS,
                independent_after_snapshot_id=independent_after_sid,
                deadline_mono_ns=time.monotonic_ns() + 10 * NS,
            )
            res = coord.acquire(req)
            http_sends = transport.count_sends()
            sid = getattr(res, "snapshot_id", None)
            ok = isinstance(res, BrokerSnapshot) and bool(res.complete)
            err = None if ok else f"Non-snapshot result: {type(res).__name__} ({res})"
            queue.put(
                ProcessWorkerResult(
                    worker_name=worker_name,
                    pid=os.getpid(),
                    ok=ok,
                    snapshot_id=sid,
                    collected_locally=(http_sends > 0),
                    http_sends=http_sends,
                    error=err,
                    elapsed_ms=(time.monotonic() - t0) * 1000.0,
                )
            )
        finally:
            client.session.close()
    except Exception as exc:
        queue.put(
            ProcessWorkerResult(
                worker_name=worker_name,
                pid=os.getpid(),
                ok=False,
                snapshot_id=None,
                collected_locally=(http_sends > 0),
                http_sends=http_sends,
                error=f"{type(exc).__name__}: {exc}",
                elapsed_ms=(time.monotonic() - t0) * 1000.0,
            )
        )


def run_real_dhan_multiprocess(
    *,
    data_dir: Path | str,
    db_path: Path | str,
    token_path: Path | str,
    account_id: str,
    specs: list[dict[str, Any]],
    timeout_seconds: float = 15.0,
    deterministic_coalesce: bool = False,
) -> list[ProcessWorkerResult]:
    """Launch real OS processes running the full DhanClient -> DhanSnapshotSource -> CrossProcessReadCoordinator stack.

    When deterministic_coalesce=True and len(specs) == 2, uses explicit multiprocessing.Event
    synchronization so the leader holds the cross-process flock before the follower enters acquire(),
    eliminating timing-dependent sleep races.
    """
    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()
    leader_entered = ctx.Event() if (deterministic_coalesce and len(specs) == 2) else None
    follower_ready = ctx.Event() if (deterministic_coalesce and len(specs) == 2) else None
    release_leader = ctx.Event() if (deterministic_coalesce and len(specs) == 2) else None

    procs: list[tuple[str, Any]] = []
    for idx, sp in enumerate(specs):
        is_leader = bool(deterministic_coalesce and len(specs) == 2 and idx == 0)
        is_follower = bool(deterministic_coalesce and len(specs) == 2 and idx == 1)
        p = ctx.Process(
            target=_real_dhan_stack_worker,
            args=(
                queue,
                sp["name"],
                str(data_dir),
                str(db_path),
                str(token_path),
                sp.get("account_id", account_id),
                sp.get("purpose", "WATCHER"),
                float(sp.get("collect_delay_s", 0.0)),
                sp.get("independent_after_sid", None),
                bool(sp.get("read_only_display", False)),
                bool(sp.get("kill_during_collect", False)),
                leader_entered if is_follower else None,
                leader_entered if is_leader else None,
                follower_ready if is_follower else None,
                release_leader if is_leader else None,
            ),
        )
        p.start()
        procs.append((sp["name"], p))
        if not deterministic_coalesce and sp.get("stagger_after_s", 0.0) > 0:
            time.sleep(float(sp["stagger_after_s"]))

    if deterministic_coalesce and leader_entered is not None and follower_ready is not None and release_leader is not None:
        leader_entered.wait(timeout=5.0)
        follower_ready.wait(timeout=5.0)
        # Brief bounded yield so follower enters flock wait before leader releases/exits
        time.sleep(0.05)
        release_leader.set()

    results: list[ProcessWorkerResult] = []
    for name, p in procs:
        p.join(timeout=timeout_seconds)
        if p.is_alive():
            p.kill()
            p.join(timeout=5.0)
            results.append(
                ProcessWorkerResult(
                    worker_name=name,
                    pid=p.pid or -1,
                    ok=False,
                    snapshot_id=None,
                    collected_locally=False,
                    http_sends=0,
                    error="TIMEOUT_DEADLOCK",
                    elapsed_ms=timeout_seconds * 1000.0,
                )
            )
        elif p.exitcode == 137:
            results.append(
                ProcessWorkerResult(
                    worker_name=name,
                    pid=p.pid or -1,
                    ok=False,
                    snapshot_id=None,
                    collected_locally=True,
                    http_sends=1,
                    error="KILLED_137",
                    elapsed_ms=0.0,
                )
            )

    while not queue.empty():
        results.append(queue.get_nowait())
    queue.close()
    queue.join_thread()
    return results
