"""Phase 2D: C01/C05/C10-safe atomic durable ledger issue writer & readback verifier.

Uses the real `system_events(event_type, description, payload, created_at)` schema and
enforces the exact Phase 2C `durable_ledger_issues.py` contract:
- `system_events.description` MUST equal `issue_id`.
- Opening payloads are validated against `issue_identity(contract, reason_code, scope, subject_id)`,
  account binding, version=1, durable classification, and valid evidence_refs.
- Resolution payloads are validated against the ACTUAL unresolved `DurableOccurrence`
  replayed via `read_durable_issues(read_journal_view(...))` inside the journal/generation
  fence — nonexistent opening event IDs, mismatched digests/contracts/scopes/subjects,
  same-snapshot resolutions, or malformed ledgers are REJECTED before any write.
- Post-commit verification replays `read_durable_issues(read_journal_view(...))` directly
  (not merely SQL row existence) to guarantee Phase 2C ledger replay integrity.
- All SQLite connections are deterministically closed via `contextlib.closing(...)`.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Mapping, Optional, Tuple

from src.broker.broker_snapshot import ContractKey, EvidenceRef, payload_digest, thaw_json
from src.broker.evidence_events import note_event
from src.engine.durable_ledger_issues import (
    OPENED,
    RESOLVED,
    issue_identity,
    read_durable_issues,
)
from src.engine.journal_view import read_journal_view

PERSISTENCE_RISK_FLAGS = (
    "persistence_uncertain",
    "last_persistence_error",
    "owner_readback_failed",
)


@dataclass(frozen=True)
class DurableEventCommitResult:
    ok: bool
    event_id: Optional[int]
    event_type: str
    readback_verified: bool
    duplicate: bool = False
    error: str = ""
    payload_sha256: str = ""


def _is_valid_sha256(val: Any) -> bool:
    return isinstance(val, str) and len(val) == 64 and all(c in "0123456789abcdef" for c in val)


def _validate_contract(raw: Any, expected_account_id: str) -> Tuple[Optional[ContractKey], str]:
    if not isinstance(raw, Mapping) or set(raw.keys()) != {
        "account_id",
        "security_id",
        "exchange_segment",
        "product_type",
    }:
        return None, "INVALID_CONTRACT_PAYLOAD"
    try:
        ck = ContractKey(**dict(raw))
    except (TypeError, ValueError) as exc:
        return None, f"INVALID_CONTRACT:{exc}"
    if ck.account_id != expected_account_id:
        return None, "CONTRACT_ACCOUNT_MISMATCH"
    return ck, "OK"


def _validate_evidence_refs(raw_refs: Any) -> Tuple[bool, str]:
    if not isinstance(raw_refs, (list, tuple)) or not raw_refs:
        return False, "MISSING_EVIDENCE_REFS"
    seen = set()
    for item in raw_refs:
        if not isinstance(item, Mapping):
            return False, "MALFORMED_EVIDENCE_REF"
        try:
            ref = EvidenceRef(**dict(item))
        except (TypeError, ValueError) as exc:
            return False, f"INVALID_EVIDENCE_REF:{exc}"
        if ref in seen:
            return False, "DUPLICATE_EVIDENCE_REF"
        seen.add(ref)
    return True, "OK"


def _canonical_json(obj: Mapping[str, Any]) -> str:
    return json.dumps(thaw_json(obj), sort_keys=True, separators=(",", ":"))


def commit_durable_issue_open(
    db_path: str | Path,
    *,
    account_id: str,
    opening: Mapping[str, Any],
    expected_journal_digest: Optional[str] = None,
    now_ts: Optional[float] = None,
    data_dir: Optional[str | Path] = None,
) -> DurableEventCommitResult:
    """C01/C10: Atomically append OWNER_LOT_LEDGER_ISSUE_OPENED conforming to Phase 2C replay."""
    path = Path(db_path)
    if not path.is_file():
        return DurableEventCommitResult(False, None, OPENED, False, error="JOURNAL_DB_MISSING")
    if not isinstance(opening, Mapping):
        return DurableEventCommitResult(False, None, OPENED, False, error="INVALID_OPENING_PAYLOAD")
    if type(opening.get("version")) is not int or opening["version"] != 1:
        return DurableEventCommitResult(False, None, OPENED, False, error="INVALID_EVENT_VERSION")

    contract, cerr = _validate_contract(opening.get("contract"), account_id)
    if contract is None:
        return DurableEventCommitResult(False, None, OPENED, False, error=cerr)

    reason = opening.get("reason_code")
    scope = opening.get("scope")
    subject = opening.get("subject_id")
    classification = opening.get("classification")
    source = opening.get("source")
    first_seen = opening.get("first_seen_utc_ns")
    snap_id = opening.get("snapshot_id")
    j_digest = opening.get("journal_digest")
    if (
        not isinstance(reason, str) or not reason.strip()
        or not isinstance(scope, str) or scope not in ("ACCOUNT", "CONTRACT", "PARENT", "ORDER", "EXECUTION")
        or not isinstance(subject, str) or not subject.strip()
        or classification not in ("DURABLE_UNRESOLVED", "CONTRADICTION")
        or not isinstance(source, str) or not source.strip()
        or type(first_seen) is not int or first_seen < 0
        or not isinstance(snap_id, str) or not snap_id.strip()
        or not _is_valid_sha256(j_digest)
    ):
        return DurableEventCommitResult(False, None, OPENED, False, error="INVALID_OPENING_FIELDS")

    expected_ident = issue_identity(contract, reason, scope, subject)
    if opening.get("issue_id") != expected_ident:
        return DurableEventCommitResult(False, None, OPENED, False, error="ISSUE_IDENTITY_MISMATCH")

    ok_refs, rerr = _validate_evidence_refs(opening.get("evidence_refs"))
    if not ok_refs:
        return DurableEventCommitResult(False, None, OPENED, False, error=rerr)

    # Pre-write Phase 2C replay & journal digest fence
    jv_before = read_journal_view(path, account_id=account_id, journal_epoch=1, read_utc_ns=time.time_ns())
    if not jv_before.read_complete:
        return DurableEventCommitResult(False, None, OPENED, False, error="JOURNAL_VIEW_INCOMPLETE")
    if expected_journal_digest is not None and jv_before.digest_sha256 != expected_journal_digest:
        return DurableEventCommitResult(False, None, OPENED, False, error="JOURNAL_DIGEST_CHANGED")

    state_before = read_durable_issues(jv_before)
    if state_before.malformed:
        return DurableEventCommitResult(False, None, OPENED, False, error="LEDGER_DURABLE_ISSUES_MALFORMED")
    for existing in state_before.unresolved:
        if existing.issue_id == expected_ident:
            return DurableEventCommitResult(
                True,
                existing.event_id,
                OPENED,
                True,
                duplicate=True,
                payload_sha256=existing.opening_sha256,
            )

    payload_str = _canonical_json(opening)
    expected_sha = payload_digest(json.loads(payload_str))
    ts = float(now_ts if now_ts is not None else time.time())

    try:
        with closing(sqlite3.connect(str(path), timeout=5.0)) as conn:
            conn.row_factory = sqlite3.Row
            cols = {r[1] for r in conn.execute("PRAGMA table_info(system_events)").fetchall()}
            if not {"event_type", "description", "payload", "created_at"}.issubset(cols):
                return DurableEventCommitResult(False, None, OPENED, False, error="SYSTEM_EVENTS_SCHEMA_MISMATCH")
            conn.execute("BEGIN IMMEDIATE")
            try:
                cur = conn.execute(
                    "INSERT INTO system_events (event_type, description, payload, created_at) VALUES (?, ?, ?, ?)",
                    (OPENED, expected_ident, payload_str, ts),
                )
                eid = int(cur.lastrowid)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
    except (sqlite3.Error, OSError) as exc:
        return DurableEventCommitResult(False, None, OPENED, False, error=f"SQLITE_WRITE_ERROR:{exc}")

    # Post-commit independent Phase 2C durable ledger replay verification
    jv_after = read_journal_view(path, account_id=account_id, journal_epoch=1, read_utc_ns=time.time_ns())
    state_after = read_durable_issues(jv_after)
    matched = [o for o in state_after.unresolved if o.issue_id == expected_ident and o.event_id == eid]
    if state_after.malformed or len(matched) != 1 or matched[0].opening_sha256 != expected_sha:
        return DurableEventCommitResult(False, eid, OPENED, False, error="POST_COMMIT_REPLAY_VERIFICATION_FAILED")

    try:
        note_event(Path(data_dir) if data_dir else path.resolve().parent, account_id, "JOURNAL_UPDATED")
    except Exception as exc:
        return DurableEventCommitResult(False, eid, OPENED, True, duplicate=False, error=f"INVALIDATION_FAILED:{exc}", payload_sha256=expected_sha)
    return DurableEventCommitResult(True, eid, OPENED, True, duplicate=False, payload_sha256=expected_sha)


def commit_durable_issue_resolution(
    db_path: str | Path,
    *,
    account_id: str,
    resolution: Mapping[str, Any],
    expected_journal_digest: Optional[str] = None,
    now_ts: Optional[float] = None,
    data_dir: Optional[str | Path] = None,
) -> DurableEventCommitResult:
    """C10: Validate against actual unresolved DurableOccurrence, append, and verify via Phase 2C replay."""
    path = Path(db_path)
    if not path.is_file():
        return DurableEventCommitResult(False, None, RESOLVED, False, error="JOURNAL_DB_MISSING")
    if not isinstance(resolution, Mapping):
        return DurableEventCommitResult(False, None, RESOLVED, False, error="INVALID_RESOLUTION_PAYLOAD")
    if type(resolution.get("version")) is not int or resolution["version"] != 1:
        return DurableEventCommitResult(False, None, RESOLVED, False, error="INVALID_EVENT_VERSION")

    contract, cerr = _validate_contract(resolution.get("contract"), account_id)
    if contract is None:
        return DurableEventCommitResult(False, None, RESOLVED, False, error=cerr)

    reason = resolution.get("reason_code")
    scope = resolution.get("scope")
    subject = resolution.get("subject_id")
    snap_id = resolution.get("snapshot_id")
    j_digest = resolution.get("journal_digest")
    opening_eid = resolution.get("opening_event_id")
    opening_sha = resolution.get("opening_sha256")
    resolved_utc_ns = resolution.get("resolved_utc_ns")
    proof_kind = resolution.get("proof_kind")
    parent_rb_sha = resolution.get("parent_readback_sha256")

    if (
        not isinstance(reason, str) or not reason.strip()
        or not isinstance(scope, str) or scope not in ("ACCOUNT", "CONTRACT", "PARENT", "ORDER", "EXECUTION")
        or not isinstance(subject, str) or not subject.strip()
        or not isinstance(snap_id, str) or not snap_id.strip()
        or not _is_valid_sha256(j_digest)
        or type(opening_eid) is not int or opening_eid < 0
        or not _is_valid_sha256(opening_sha)
        or type(resolved_utc_ns) is not int or resolved_utc_ns < 0
        or proof_kind != "EXACT_PARENT_LEDGER_READBACK"
        or not _is_valid_sha256(parent_rb_sha)
    ):
        return DurableEventCommitResult(False, None, RESOLVED, False, error="INVALID_RESOLUTION_FIELDS")

    expected_ident = issue_identity(contract, reason, scope, subject)
    if resolution.get("issue_id") != expected_ident:
        return DurableEventCommitResult(False, None, RESOLVED, False, error="ISSUE_IDENTITY_MISMATCH")

    ok_refs, rerr = _validate_evidence_refs(resolution.get("evidence_refs"))
    if not ok_refs:
        return DurableEventCommitResult(False, None, RESOLVED, False, error=rerr)

    # C10: Replay actual durable issues from SQLite and verify the target occurrence actually exists & matches!
    jv_before = read_journal_view(path, account_id=account_id, journal_epoch=1, read_utc_ns=time.time_ns())
    if not jv_before.read_complete:
        return DurableEventCommitResult(False, None, RESOLVED, False, error="JOURNAL_VIEW_INCOMPLETE")
    if expected_journal_digest is not None and jv_before.digest_sha256 != expected_journal_digest:
        return DurableEventCommitResult(False, None, RESOLVED, False, error="JOURNAL_DIGEST_CHANGED")

    state_before = read_durable_issues(jv_before)
    if state_before.malformed:
        return DurableEventCommitResult(False, None, RESOLVED, False, error="LEDGER_DURABLE_ISSUES_MALFORMED")

    matches = [o for o in state_before.unresolved if o.issue_id == expected_ident]
    if len(matches) != 1:
        return DurableEventCommitResult(False, None, RESOLVED, False, error="OPENING_OCCURRENCE_NOT_FOUND")
    target = matches[0]

    # Validate exact occurrence linkage before writing a single byte
    if (
        target.event_id != opening_eid
        or target.opening_sha256 != opening_sha
        or target.contract != contract
        or target.reason_code != reason
        or target.scope != scope
        or target.subject_id != subject
        or snap_id == target.opened_snapshot_id
        or resolved_utc_ns < target.first_seen_utc_ns
    ):
        return DurableEventCommitResult(False, None, RESOLVED, False, error="OPENING_LINKAGE_MISMATCH")

    # Dry-run Phase 2C replay validation on the proposed resolution row BEFORE committing
    payload_str = _canonical_json(resolution)
    expected_sha = payload_digest(json.loads(payload_str))
    ts = float(now_ts if now_ts is not None else time.time())

    try:
        with closing(sqlite3.connect(str(path), timeout=5.0)) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN IMMEDIATE")
            try:
                cur = conn.execute(
                    "INSERT INTO system_events (event_type, description, payload, created_at) VALUES (?, ?, ?, ?)",
                    (RESOLVED, expected_ident, payload_str, ts),
                )
                eid = int(cur.lastrowid)
                # Verify via Phase 2C replay on a temporary in-transaction view before committing!
                # Commit first only after dry-checking, OR rollback if replay fails:
                # Since read_journal_view opens a separate ro connection, commit and if replay reports malformed,
                # we validate _verify_execution_refs BEFORE commit via read_durable_issues on synthetic appended list.
                from types import SimpleNamespace as _NS
                synthetic_row = {
                    "id": eid,
                    "event_type": RESOLVED,
                    "description": expected_ident,
                    "payload": payload_str,
                    "created_at": ts,
                }
                jv_sim = _NS(
                    account_id=jv_before.account_id,
                    fills=jv_before.fills,
                    exit_allocations=jv_before.exit_allocations,
                    relevant_system_events=jv_before.relevant_system_events + (synthetic_row,),
                )
                sim_state = read_durable_issues(jv_sim)
                if sim_state.malformed or any(o.issue_id == expected_ident for o in sim_state.unresolved):
                    conn.rollback()
                    return DurableEventCommitResult(False, None, RESOLVED, False, error="PHASE2C_REPLAY_REJECTED_RESOLUTION")
                conn.commit()
            except Exception:
                conn.rollback()
                raise
    except (sqlite3.Error, OSError) as exc:
        return DurableEventCommitResult(False, None, RESOLVED, False, error=f"SQLITE_WRITE_ERROR:{exc}")

    # Post-commit independent Phase 2C replay readback
    jv_after = read_journal_view(path, account_id=account_id, journal_epoch=1, read_utc_ns=time.time_ns())
    state_after = read_durable_issues(jv_after)
    if state_after.malformed or any(o.issue_id == expected_ident for o in state_after.unresolved):
        return DurableEventCommitResult(False, eid, RESOLVED, False, error="POST_COMMIT_REPLAY_VERIFICATION_FAILED")

    try:
        note_event(Path(data_dir) if data_dir else path.resolve().parent, account_id, "JOURNAL_UPDATED")
    except Exception as exc:
        return DurableEventCommitResult(False, eid, RESOLVED, True, duplicate=False, error=f"INVALIDATION_FAILED:{exc}", payload_sha256=expected_sha)
    return DurableEventCommitResult(True, eid, RESOLVED, True, duplicate=False, payload_sha256=expected_sha)


def verify_operation_readback_and_clear_persistence_risk(
    db_path: str | Path,
    record: dict[str, Any],
    *,
    expected_order_id: str,
    expected_position_state: str,
    expected_filled_qty: int,
    expected_account_id: Optional[str] = None,
    expected_fill_ids: Optional[Sequence[str]] = None,
    expected_sell_filled_qty: Optional[int] = None,
    expected_confirmed_open_qty: Optional[int] = None,
    expected_protection_status: Optional[str] = None,
    expected_protection_order_id: Optional[str] = None,
    expected_event_type: Optional[str] = None,
    expected_issue_id: Optional[str] = None,
    expected_security_id: Optional[str] = None,
    expected_segment: Optional[str] = None,
    expected_product_type: Optional[str] = None,
) -> Tuple[bool, str]:
    """C05 + C48 + C53 + C56 + C59: Clear persistence risks ONLY after exact, mandatory
    operation-bound SQLite readback across watcher_orders, positions_lifecycle, verified fills,
    exit_allocations, protection_orders, and durable system_events.

    C59 invariant: Absence of ANY required evidence table or column MUST fail closed —
    missing tables can NEVER be treated as successful verification.
    """
    if not isinstance(record, dict):
        return False, "INVALID_RECORD"
    if (
        not expected_order_id
        or not expected_position_state
        or isinstance(expected_filled_qty, bool)
        or type(expected_filled_qty) is not int
        or expected_filled_qty < 0
    ):
        return False, "INVALID_EXPECTED_FIELDS"
    if expected_sell_filled_qty is not None and (
        isinstance(expected_sell_filled_qty, bool)
        or type(expected_sell_filled_qty) is not int
        or expected_sell_filled_qty < 0
    ):
        return False, "INVALID_EXPECTED_SELL_QTY"
    if expected_confirmed_open_qty is not None and (
        isinstance(expected_confirmed_open_qty, bool)
        or type(expected_confirmed_open_qty) is not int
        or expected_confirmed_open_qty < 0
    ):
        return False, "INVALID_EXPECTED_OPEN_QTY"

    path = Path(db_path)
    if not path.is_file():
        return False, "JOURNAL_DB_MISSING"

    def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
        return {str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}

    try:
        uri = f"file:{path.resolve()}?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=5.0, isolation_level=None)) as conn:
            conn.row_factory = sqlite3.Row
            # C65: Coherent read transaction snapshot across all evidence tables
            conn.execute("BEGIN DEFERRED")
            tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}

            # C59: watcher_orders and positions_lifecycle are ALWAYS mandatory
            for _req_tbl in ("watcher_orders", "positions_lifecycle"):
                if _req_tbl not in tables:
                    return False, f"MANDATORY_TABLE_MISSING:{_req_tbl}"

            # 1. Verify watcher_orders (exactly 1 row, valid JSON, exact state & qty)
            wo_cols = _table_columns(conn, "watcher_orders")
            for _c in ("order_id", "record_json"):
                if _c not in wo_cols:
                    return False, f"MANDATORY_COLUMN_MISSING:watcher_orders.{_c}"
            wo_rows = conn.execute(
                "SELECT order_id, record_json FROM watcher_orders WHERE order_id = ?",
                (expected_order_id,),
            ).fetchall()
            if len(wo_rows) == 0:
                return False, "TARGET_RECORD_NOT_FOUND_IN_WATCHER_ORDERS"
            if len(wo_rows) > 1:
                return False, "CONTRADICTORY_DUPLICATE_WATCHER_ORDER_ROWS"
            try:
                stored = json.loads(wo_rows[0]["record_json"] or "")
            except (ValueError, TypeError):
                return False, "TARGET_RECORD_JSON_MALFORMED"
            if not isinstance(stored, dict):
                return False, "TARGET_RECORD_NOT_DICT"
            if (
                str(stored.get("order_id") or "") != expected_order_id
                or str(stored.get("position_state") or "") != expected_position_state
                or isinstance(stored.get("filled_qty"), bool)
                or type(stored.get("filled_qty")) is not int
                or stored.get("filled_qty") != expected_filled_qty
            ):
                return False, "TARGET_RECORD_READBACK_MISMATCH"

            # 2. Verify positions_lifecycle (mandatory, exactly 1 row, exact account/contract/state/qtys)
            pl_cols = _table_columns(conn, "positions_lifecycle")
            for _c in ("parent_order_id", "account_id", "position_state", "buy_filled_qty", "sell_filled_qty", "confirmed_open_qty"):
                if _c not in pl_cols:
                    return False, f"MANDATORY_COLUMN_MISSING:positions_lifecycle.{_c}"
            _pl_select_extra = ""
            if expected_security_id or expected_segment or expected_product_type or (expected_sell_filled_qty is not None and int(expected_sell_filled_qty) > 0):
                for _c in ("security_id", "exchange_segment", "product_type"):
                    if _c not in pl_cols:
                        return False, f"MANDATORY_COLUMN_MISSING:positions_lifecycle.{_c}"
                _pl_select_extra = ", security_id, exchange_segment, product_type"
            pl_rows = conn.execute(
                f"SELECT account_id, position_state, buy_filled_qty, sell_filled_qty, confirmed_open_qty{_pl_select_extra} "
                "FROM positions_lifecycle WHERE parent_order_id = ?",
                (expected_order_id,),
            ).fetchall()
            if len(pl_rows) == 0:
                return False, "TARGET_LIFECYCLE_NOT_COMMITTED"
            if len(pl_rows) > 1:
                return False, "CONTRADICTORY_DUPLICATE_LIFECYCLE_ROWS"
            pl_row = pl_rows[0]
            if expected_account_id and str(pl_row["account_id"] or "").strip() != str(expected_account_id).strip():
                return False, "TARGET_LIFECYCLE_ACCOUNT_MISMATCH"
            if expected_security_id and str(pl_row["security_id"] or "").strip() != str(expected_security_id).strip():
                return False, "TARGET_LIFECYCLE_CONTRACT_MISMATCH:security_id"
            if expected_segment and str(pl_row["exchange_segment"] or "").strip().upper() != str(expected_segment).strip().upper():
                return False, "TARGET_LIFECYCLE_CONTRACT_MISMATCH:exchange_segment"
            if expected_product_type and str(pl_row["product_type"] or "").strip().upper() != str(expected_product_type).strip().upper():
                return False, "TARGET_LIFECYCLE_CONTRACT_MISMATCH:product_type"
            if str(pl_row["position_state"] or "").strip() != expected_position_state:
                return False, "TARGET_LIFECYCLE_STATE_MISMATCH"
            if int(pl_row["buy_filled_qty"] or 0) != expected_filled_qty:
                return False, "TARGET_LIFECYCLE_BUY_QTY_MISMATCH"
            if expected_sell_filled_qty is not None and int(pl_row["sell_filled_qty"] or 0) != int(expected_sell_filled_qty):
                return False, "TARGET_LIFECYCLE_SELL_QTY_MISMATCH"
            if expected_confirmed_open_qty is not None and int(pl_row["confirmed_open_qty"] or 0) != int(expected_confirmed_open_qty):
                return False, "TARGET_LIFECYCLE_OPEN_QTY_MISMATCH"

            # 3. C59 + C53: Mandatory fills verification when filled_qty > 0 or expected_fill_ids provided
            if expected_filled_qty > 0 or expected_fill_ids:
                if "fills" not in tables:
                    return False, "MANDATORY_TABLE_MISSING:fills"
                f_cols = _table_columns(conn, "fills")
                for _c in ("broker_order_id", "parent_order_id", "traded_quantity", "transaction_type", "is_verified_trade", "exchange_trade_id", "security_id", "exchange_segment", "product_type"):
                    if _c not in f_cols:
                        return False, f"MANDATORY_COLUMN_MISSING:fills.{_c}"
                buy_rows = conn.execute(
                    "SELECT traded_quantity, security_id, exchange_segment, product_type FROM fills "
                    "WHERE (broker_order_id = ? OR parent_order_id = ?) "
                    "AND UPPER(transaction_type) = 'BUY' AND is_verified_trade = 1",
                    (expected_order_id, expected_order_id),
                ).fetchall()
                buy_sum = 0
                for _br in buy_rows:
                    if expected_security_id and str(_br["security_id"] or "").strip() != str(expected_security_id).strip():
                        continue
                    if expected_segment and str(_br["exchange_segment"] or "").strip().upper() != str(expected_segment).strip().upper():
                        continue
                    if expected_product_type and str(_br["product_type"] or "").strip().upper() != str(expected_product_type).strip().upper():
                        continue
                    buy_sum += int(_br["traded_quantity"] or 0)
                if expected_filled_qty > 0 and buy_sum != expected_filled_qty:
                    return False, f"TARGET_FILLS_BUY_QTY_MISMATCH:{buy_sum}!={expected_filled_qty}"
                if expected_fill_ids:
                    parent_fill_rows = conn.execute(
                        "SELECT exchange_trade_id, security_id, exchange_segment, product_type "
                        "FROM fills WHERE (broker_order_id = ? OR parent_order_id = ?) "
                        "AND is_verified_trade = 1 AND exchange_trade_id IS NOT NULL",
                        (expected_order_id, expected_order_id),
                    ).fetchall()
                    committed_eids = set()
                    for _fr in parent_fill_rows:
                        if expected_security_id and str(_fr["security_id"] or "").strip() != str(expected_security_id).strip():
                            continue
                        if expected_segment and str(_fr["exchange_segment"] or "").strip().upper() != str(expected_segment).strip().upper():
                            continue
                        if expected_product_type and str(_fr["product_type"] or "").strip().upper() != str(expected_product_type).strip().upper():
                            continue
                        committed_eids.add(str(_fr["exchange_trade_id"]))
                    missing_eids = [eid for eid in expected_fill_ids if str(eid) not in committed_eids]
                    if missing_eids:
                        return False, f"TARGET_FILL_IDS_MISSING_FOR_PARENT:{missing_eids}"

            # 4. C59 + C64 + C65: Mandatory contract-bound SELL fills & exact execution-to-allocation linkage
            if expected_sell_filled_qty is not None and int(expected_sell_filled_qty) > 0:
                if "fills" not in tables:
                    return False, "MANDATORY_TABLE_MISSING:fills"
                if "exit_allocations" not in tables:
                    return False, "MANDATORY_TABLE_MISSING:exit_allocations"
                f_cols_s = _table_columns(conn, "fills")
                for _c in ("broker_order_id", "parent_order_id", "traded_quantity", "transaction_type", "is_verified_trade", "exchange_trade_id", "security_id", "exchange_segment", "product_type"):
                    if _c not in f_cols_s:
                        return False, f"MANDATORY_COLUMN_MISSING:fills.{_c}"
                ea_cols = _table_columns(conn, "exit_allocations")
                for _c in ("parent_order_id", "sell_order_id", "exchange_trade_id", "allocated_qty"):
                    if _c not in ea_cols:
                        return False, f"MANDATORY_COLUMN_MISSING:exit_allocations.{_c}"

                # Resolve authoritative contract from expected params or positions_lifecycle
                _exp_sec = str(expected_security_id or (pl_row["security_id"] if "security_id" in pl_row.keys() else "") or "").strip()
                _exp_seg = str(expected_segment or (pl_row["exchange_segment"] if "exchange_segment" in pl_row.keys() else "") or "").strip().upper()
                _exp_prod = str(expected_product_type or (pl_row["product_type"] if "product_type" in pl_row.keys() else "") or "").strip().upper()

                def _fill_matches_contract(_r) -> bool:
                    if _exp_sec and str(_r["security_id"] or "").strip() != _exp_sec:
                        return False
                    if _exp_seg and str(_r["exchange_segment"] or "").strip().upper() != _exp_seg:
                        return False
                    if _exp_prod and str(_r["product_type"] or "").strip().upper() != _exp_prod:
                        return False
                    return True

                # 4a. Direct verified SELL fills bound to expected_order_id
                direct_rows = conn.execute(
                    "SELECT broker_order_id, exchange_trade_id, traded_quantity, security_id, exchange_segment, product_type "
                    "FROM fills WHERE parent_order_id = ? AND UPPER(transaction_type) = 'SELL' AND is_verified_trade = 1",
                    (expected_order_id,),
                ).fetchall()
                direct_by_eid: dict[str, int] = {}
                for _dr in direct_rows:
                    if not _fill_matches_contract(_dr):
                        return False, "TARGET_SELL_FILL_CONTRACT_MISMATCH"
                    _eid = str(_dr["exchange_trade_id"] or "").strip()
                    _dq = int(_dr["traded_quantity"] or 0)
                    if not _eid or _dq <= 0:
                        return False, "TARGET_SELL_FILL_INVALID_EXECUTION"
                    if _eid in direct_by_eid:
                        return False, f"DUPLICATE_DIRECT_SELL_EXECUTION:{_eid}"
                    direct_by_eid[_eid] = _dq

                # 4b. Exit allocations bound to expected_order_id — every row MUST link to a verified same-contract SELL fill
                alloc_rows = conn.execute(
                    "SELECT sell_order_id, exchange_trade_id, allocated_qty "
                    "FROM exit_allocations WHERE parent_order_id = ?",
                    (expected_order_id,),
                ).fetchall()
                alloc_by_eid: dict[str, int] = {}
                for _ar in alloc_rows:
                    _aeid = str(_ar["exchange_trade_id"] or "").strip()
                    _asoid = str(_ar["sell_order_id"] or "").strip()
                    _aq = int(_ar["allocated_qty"] or 0)
                    if not _aeid or not _asoid or _aq <= 0:
                        return False, "INVALID_EXIT_ALLOCATION_ROW"
                    if _aeid in alloc_by_eid:
                        return False, f"DUPLICATE_EXIT_ALLOCATION_EXECUTION:{_aeid}"
                    backing = conn.execute(
                        "SELECT broker_order_id, parent_order_id, traded_quantity, transaction_type, is_verified_trade, "
                        "security_id, exchange_segment, product_type FROM fills WHERE exchange_trade_id = ?",
                        (_aeid,),
                    ).fetchall()
                    if len(backing) != 1:
                        return False, f"UNVERIFIED_OR_AMBIGUOUS_EXIT_ALLOCATION_FILL:{_aeid}"
                    _bf = backing[0]
                    if int(_bf["is_verified_trade"] or 0) != 1 or str(_bf["transaction_type"] or "").strip().upper() != "SELL":
                        return False, f"EXIT_ALLOCATION_FILL_NOT_VERIFIED_SELL:{_aeid}"
                    if str(_bf["broker_order_id"] or "").strip() != _asoid:
                        return False, f"EXIT_ALLOCATION_ORDER_ID_MISMATCH:{_aeid}"
                    if not _fill_matches_contract(_bf):
                        return False, f"EXIT_ALLOCATION_CONTRACT_MISMATCH:{_aeid}"
                    _bf_parent = str(_bf["parent_order_id"] or "").strip()
                    if _bf_parent and _bf_parent != expected_order_id:
                        return False, f"EXIT_ALLOCATION_WRONG_PARENT_FILL:{_aeid}"
                    _tot_alloc_for_eid = int(conn.execute(
                        "SELECT COALESCE(SUM(allocated_qty), 0) FROM exit_allocations WHERE exchange_trade_id = ?",
                        (_aeid,),
                    ).fetchone()[0] or 0)
                    if _tot_alloc_for_eid > int(_bf["traded_quantity"] or 0):
                        return False, f"EXIT_ALLOCATION_EXCEEDS_FILL_QTY:{_aeid}:{_tot_alloc_for_eid}>{_bf['traded_quantity']}"
                    if _aeid in direct_by_eid and direct_by_eid[_aeid] != _aq:
                        return False, f"CONTRADICTORY_DIRECT_VS_ALLOCATED_SELL_QTY:{_aeid}:{direct_by_eid[_aeid]}!={_aq}"
                    alloc_by_eid[_aeid] = _aq

                # 4c. Exact deduplicated union of verified direct + allocated external SELL executions (zero max() substitution, zero double counting)
                verified_sell_by_eid = dict(direct_by_eid)
                for _aeid, _aq in alloc_by_eid.items():
                    if _aeid not in verified_sell_by_eid:
                        verified_sell_by_eid[_aeid] = _aq
                total_verified_sell = sum(verified_sell_by_eid.values())
                if total_verified_sell != int(expected_sell_filled_qty):
                    return False, f"TARGET_EXIT_ALLOCATION_MISMATCH:{total_verified_sell}!={expected_sell_filled_qty}"

            # 5. C59: Mandatory protection_orders verification when expected_protection_status or order_id required
            if expected_protection_status is not None or expected_protection_order_id is not None:
                if "protection_orders" not in tables:
                    return False, "MANDATORY_TABLE_MISSING:protection_orders"
                po_cols = _table_columns(conn, "protection_orders")
                for _c in ("parent_order_id", "status"):
                    if _c not in po_cols:
                        return False, f"MANDATORY_COLUMN_MISSING:protection_orders.{_c}"
                if expected_protection_order_id is not None and "forever_order_id" not in po_cols:
                    return False, "MANDATORY_COLUMN_MISSING:protection_orders.forever_order_id"
                if expected_protection_order_id is not None and expected_protection_status is not None:
                    po_row = conn.execute(
                        "SELECT status FROM protection_orders WHERE parent_order_id = ? AND forever_order_id = ? AND status = ? LIMIT 1",
                        (expected_order_id, str(expected_protection_order_id), str(expected_protection_status)),
                    ).fetchone()
                elif expected_protection_order_id is not None:
                    po_row = conn.execute(
                        "SELECT status FROM protection_orders WHERE parent_order_id = ? AND forever_order_id = ? LIMIT 1",
                        (expected_order_id, str(expected_protection_order_id)),
                    ).fetchone()
                else:
                    po_row = conn.execute(
                        "SELECT status FROM protection_orders WHERE parent_order_id = ? AND status = ? LIMIT 1",
                        (expected_order_id, str(expected_protection_status)),
                    ).fetchone()
                if po_row is None:
                    return False, f"TARGET_PROTECTION_STATUS_NOT_COMMITTED:{expected_protection_status or expected_protection_order_id}"

            # 6. C59 + C53: Mandatory system_events verification when expected_event_type or expected_issue_id required
            if expected_event_type or expected_issue_id:
                if "system_events" not in tables:
                    return False, "MANDATORY_TABLE_MISSING:system_events"
                se_cols = _table_columns(conn, "system_events")
                for _c in ("event_type", "description", "payload"):
                    if _c not in se_cols:
                        return False, f"MANDATORY_COLUMN_MISSING:system_events.{_c}"
                ev_rows = conn.execute(
                    "SELECT event_type, description, payload FROM system_events "
                    + ("WHERE event_type = ?" if expected_event_type else ""),
                    ((str(expected_event_type),) if expected_event_type else ()),
                ).fetchall()
                if not ev_rows:
                    return False, f"TARGET_SYSTEM_EVENT_NOT_COMMITTED:{expected_event_type}"
                if expected_issue_id:
                    found_ev = False
                    for _er in ev_rows:
                        try:
                            _ep = json.loads(_er["payload"] or "")
                        except (ValueError, TypeError):
                            continue
                        if isinstance(_ep, dict) and str(_ep.get("issue_id") or "").strip() == str(expected_issue_id).strip():
                            found_ev = True
                            break
                    if not found_ev:
                        return False, f"TARGET_ISSUE_LINKAGE_NOT_COMMITTED:{expected_issue_id}"
    except (sqlite3.Error, OSError) as exc:
        return False, f"READBACK_SQLITE_ERROR:{exc}"

    for fl in PERSISTENCE_RISK_FLAGS:
        record.pop(fl, None)
    return True, "EXACT_READBACK_VERIFIED"
