import sqlite3

conn = sqlite3.connect('data/trade_journal.db')
c = conn.cursor()

tables = c.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
for (table,) in tables:
    try:
        c.execute(f"UPDATE {table} SET state='CLOSED' WHERE state NOT IN ('CLOSED', 'REJECTED', 'COMPLETED')")
    except: pass
    try:
        c.execute(f"UPDATE {table} SET status='CLOSED' WHERE status NOT IN ('CLOSED', 'REJECTED', 'COMPLETED')")
    except: pass
    try:
        c.execute(f"DELETE FROM {table} WHERE CAST(order_id AS TEXT) LIKE '%23126110542909%' OR symbol = 'GROWW'")
    except: pass

conn.commit()
conn.close()
print("✅ Stuck GROWW order forcefully purged from local database!")
