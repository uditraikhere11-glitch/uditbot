"""
DISABLED BY INDEPENDENT AUDIT REMEDIATION (P0).
Blanket SQLite state rewrites across journal tables are strictly prohibited.
Use broker-authoritative historical fill reconciliation (`GET /v2/trades/{from-date}/{to-date}/{page}`).
"""

raise RuntimeError(
    "bypass_reconciliation.py is permanently disabled: blanket SQLite state rewrites "
    "are prohibited. Use StartupReconciliationGate / JournalDB.apply_verified_historical_fills."
)
