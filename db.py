import sqlite3

DB_PATH = "ims.sqlite3"

def get_conn():
    return sqlite3.connect(DB_PATH)

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # respondents
    cur.execute("""
    CREATE TABLE IF NOT EXISTS respondents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        response_id TEXT,
        email TEXT,
        name TEXT,
        processed_at TEXT,
        status TEXT
    )
    """)

    # scores
    cur.execute("""
    CREATE TABLE IF NOT EXISTS scores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        respondent_id INTEGER,
        overall REAL,
        f1 REAL,
        f2 REAL,
        f3 REAL,
        f4 REAL,
        f5 REAL
    )
    """)

    # feedback
    cur.execute("""
    CREATE TABLE IF NOT EXISTS feedback (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        respondent_id INTEGER,
        improve_text TEXT,
        strength_text TEXT
    )
    """)

    conn.commit()
    conn.close()
