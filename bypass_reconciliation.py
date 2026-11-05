import sqlite3

conn = sqlite3.connect('data/trade_journal.db')
c = conn.cursor()

# Find all tables and forcefully close any 'OPEN' or 'PENDING' states
tables = c.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
for (table,) in tables:
    try:
        c.execute(f"UPDATE {table} SET state='CLOSED' WHERE state NOT IN ('CLOSED', 'REJECTED', 'COMPLETED')")
    except: pass
    try:
        c.execute(f"UPDATE {table} SET status='CLOSED' WHERE status NOT IN ('CLOSED', 'REJECTED', 'COMPLETED')")
    except: pass

conn.commit()
conn.close()
print("✅ Local database cleared of pending LIVE state.")
