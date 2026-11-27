"""Single SQLite read transaction for the immutable ledger evidence boundary.

The byte digest matches the Phase 2B acquisition digest. No migrations, write
connections, recovery, or broker calls are performed here.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass, fields
import hashlib
import json
from pathlib import Path
import sqlite3
from collections.abc import Mapping
from src.broker.broker_snapshot import freeze_json, thaw_json
from src.broker.dhan_snapshot_source import JOURNAL_TABLES

# Preserve the existing acquisition order (including retry reservations).
_TABLE_FIELDS = dict(zip(JOURNAL_TABLES, (
    'order_intents', 'retry_reservations', 'orders', 'fills', 'exit_allocations',
    'positions_lifecycle', 'protection_orders', 'protection_attempts',
    'watcher_orders', 'manual_risk_acknowledgements', 'historical_fill_attestations',
    'relevant_system_events', 'outbox_evidence',
)))


# Read completeness includes schema identity, even if a damaged table is empty.
_REQUIRED_COLUMNS = {
    'order_intents': 'correlation_id account_id security_id exchange_segment product_type quantity state broker_order_id created_at',
    'retry_reservations': 'root_order_id parent_order_id child_correlation_id reserved_at',
    'orders': 'correlation_id broker_order_id trading_mode security_id exchange_segment product_type quantity filled_quantity status',
    'fills': 'id broker_order_id exchange_trade_id traded_price traded_quantity traded_at transaction_type security_id exchange_segment product_type parent_order_id is_verified_trade evidence_source',
    'exit_allocations': 'parent_order_id sell_order_id exchange_trade_id allocated_qty traded_price traded_at',
    'positions_lifecycle': 'parent_order_id account_id security_id exchange_segment product_type protection_route position_state entry_qty buy_filled_qty sell_filled_qty protected_qty confirmed_open_qty triggered_leg manual_exit_detected orphaned_legs_detected',
    'protection_orders': 'parent_order_id forever_order_id quantity sl_trigger target_price status protection_route',
    'protection_attempts': 'attempt_id parent_order_id root_order_id account_id security_id exchange_segment product_type state',
    'watcher_orders': 'order_id is_terminal record_json updated_at',
    'manual_risk_acknowledgements': 'parent_order_id correlation_id account_id security_id quantity acknowledged_by reason status',
    'broker_attested_historical_fills': 'account_id broker_order_id correlation_id exchange_order_id evidence_fingerprint evidence_json',
    'system_events': 'id event_type description payload created_at',
    'outbox': 'event_type payload status created_at',
}


@dataclass(frozen=True, slots=True)
class JournalView:
    account_id: str
    journal_epoch: int
    read_utc_ns: int
    digest_sha256: str | None
    order_intents: tuple[Mapping, ...]
    orders: tuple[Mapping, ...]
    fills: tuple[Mapping, ...]
    exit_allocations: tuple[Mapping, ...]
    positions_lifecycle: tuple[Mapping, ...]
    protection_orders: tuple[Mapping, ...]
    protection_attempts: tuple[Mapping, ...]
    watcher_orders: tuple[Mapping, ...]
    manual_risk_acknowledgements: tuple[Mapping, ...]
    historical_fill_attestations: tuple[Mapping, ...]
    relevant_system_events: tuple[Mapping, ...]
    outbox_evidence: tuple[Mapping, ...]
    retry_reservations: tuple[Mapping, ...]
    read_complete: bool
    table_columns: Mapping
    issues: tuple[str, ...] = ()

    def __post_init__(self):
        if type(self.account_id) is not str or not self.account_id or self.account_id.strip() != self.account_id:
            raise ValueError('Exact account required')
        if any(type(v) is not int or v < 0 for v in (self.journal_epoch, self.read_utc_ns)):
            raise ValueError('Invalid journal observation')
        if type(self.read_complete) is not bool:
            raise ValueError('Unknown journal completeness')
        for name in _TABLE_FIELDS.values():
            rows = tuple(freeze_json(r) for r in getattr(self, name))
            if any(not isinstance(r, Mapping) for r in rows):
                raise ValueError('Malformed journal row')
            object.__setattr__(self, name, rows)
        object.__setattr__(self, 'table_columns', freeze_json(self.table_columns))
        object.__setattr__(self, 'issues', tuple(self.issues))
        if self.read_complete and (self.issues or not self.digest_sha256 or self.computed_digest() != self.digest_sha256):
            raise ValueError('Complete journal digest mismatch')

    def computed_digest(self):
        if set(self.table_columns) != set(_TABLE_FIELDS):
            raise ValueError('Journal table inventory incomplete')
        digest = hashlib.sha256()
        for table, name in _TABLE_FIELDS.items():
            cols = self.table_columns[table]
            if not cols or len(cols) != len(set(cols)) or not set(_REQUIRED_COLUMNS[table].split()) <= set(cols):
                raise ValueError('Invalid journal column inventory')
            digest.update(json.dumps([table, list(cols)]).encode())
            for row in getattr(self, name):
                if set(row) != set(cols):
                    raise ValueError('Journal record columns differ')
                digest.update(json.dumps([thaw_json(row[c]) for c in cols], ensure_ascii=False,
                                         allow_nan=False, separators=(',', ':')).encode())
                digest.update(b'\n')
        return digest.hexdigest()


def read_journal_view(path, *, account_id, journal_epoch, read_utc_ns):
    """Read all required tables or return an explicitly incomplete view.

    The caller supplies pinned account and journal epoch and must revalidate
    them against the sidecar after acquisition; a digest alone is not authority.
    """
    rows = dict.fromkeys(_TABLE_FIELDS.values(), ())
    columns = {}
    digest = hashlib.sha256()
    try:
        path = Path(path).absolute()
        if any(p.is_symlink() for p in (path, *path.parents)) or not path.is_file():
            raise ValueError('JOURNAL_UNAVAILABLE')
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=.15)) as conn:
            conn.execute('PRAGMA query_only=ON')
            conn.execute('BEGIN')
            for table, name in _TABLE_FIELDS.items():
                cursor = conn.execute('SELECT * FROM ' + table + ' ORDER BY rowid')
                cols = [d[0] for d in cursor.description]
                columns[table] = cols
                digest.update(json.dumps([table, cols]).encode())
                records = []
                for row in cursor:
                    digest.update(json.dumps(row, ensure_ascii=False, allow_nan=False,
                                             separators=(',', ':')).encode())
                    digest.update(b'\n')
                    records.append(dict(zip(cols, row)))
                rows[name] = tuple(records)
            # These JSON values are part of the immutable raw digest; parse
            # only to reject malformed legacy evidence, never rewrite them.
            for name, key in (('watcher_orders','record_json'),
                              ('historical_fill_attestations','evidence_json')):
                for row in rows[name]:
                    if not isinstance(json.loads(row[key]), dict):
                        raise ValueError('JOURNAL_JSON_MALFORMED')
        return JournalView(account_id, journal_epoch, read_utc_ns, digest.hexdigest(),
                           **rows, read_complete=True, table_columns=columns)
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError):
        return JournalView(account_id, journal_epoch, read_utc_ns, None,
                           **dict.fromkeys(_TABLE_FIELDS.values(), ()), read_complete=False,
                           table_columns={}, issues=('JOURNAL_READ_INCOMPLETE',))
