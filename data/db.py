"""
db.py — SQLite schema creation and upsert helpers.

Design notes:
  • session_id (MD5 hash) is the primary key for nursing/pump/expressed/formula
    tables.  This makes ingestion fully idempotent via INSERT OR REPLACE.
  • nursing_ml = NULL means "no note at all" (pre-tracking session).
    nursing_ml = 0.0 means "note exists, no reliable ml value".
    nursing_ml > 0   means a confirmed transfer volume.
    This distinction is critical for correct aggregate statistics.
  • daily_summary is a materialized cache — dropped and rebuilt on every
    data load.  No partial-update risk.
"""

import sqlite3
from pathlib import Path

import pandas as pd


# ─────────────────────────────────────────────
# Schema DDL
# ─────────────────────────────────────────────

_SCHEMA_SQL = """
-- ── Raw event tables ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS nursing_sessions (
    session_id   TEXT PRIMARY KEY,
    datetime     TEXT NOT NULL,
    date         TEXT NOT NULL,
    start_side   TEXT,
    left_min     REAL DEFAULT 0,
    right_min    REAL DEFAULT 0,
    total_min    REAL DEFAULT 0,
    note         TEXT,
    nursing_ml   REAL            -- NULL=no note; 0=no ml found; >0=transfer vol
);
CREATE INDEX IF NOT EXISTS idx_nursing_date ON nursing_sessions(date);

CREATE TABLE IF NOT EXISTS pump_sessions (
    session_id   TEXT PRIMARY KEY,
    datetime     TEXT NOT NULL,
    date         TEXT NOT NULL,
    left_min     REAL DEFAULT 0,
    right_min    REAL DEFAULT 0,
    total_min    REAL DEFAULT 0,
    left_ml      REAL DEFAULT 0,
    right_ml     REAL DEFAULT 0,
    total_ml     REAL DEFAULT 0,
    note         TEXT
);
CREATE INDEX IF NOT EXISTS idx_pump_date ON pump_sessions(date);

CREATE TABLE IF NOT EXISTS expressed_sessions (
    session_id   TEXT PRIMARY KEY,
    datetime     TEXT NOT NULL,
    date         TEXT NOT NULL,
    amount_ml    REAL DEFAULT 0,
    note         TEXT
);
CREATE INDEX IF NOT EXISTS idx_expressed_date ON expressed_sessions(date);

CREATE TABLE IF NOT EXISTS formula_sessions (
    session_id   TEXT PRIMARY KEY,
    datetime     TEXT NOT NULL,
    date         TEXT NOT NULL,
    amount_ml    REAL DEFAULT 0,
    note         TEXT
);
CREATE INDEX IF NOT EXISTS idx_formula_date ON formula_sessions(date);

CREATE TABLE IF NOT EXISTS diaper_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    datetime     TEXT NOT NULL UNIQUE,   -- unique on exact timestamp
    date         TEXT NOT NULL,
    status       TEXT NOT NULL,
    note         TEXT
);
CREATE INDEX IF NOT EXISTS idx_diaper_date ON diaper_events(date);

CREATE TABLE IF NOT EXISTS sleep_sessions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    datetime     TEXT NOT NULL UNIQUE,
    date         TEXT NOT NULL,
    duration_min REAL DEFAULT 0,
    note         TEXT
);
CREATE INDEX IF NOT EXISTS idx_sleep_date ON sleep_sessions(date);

CREATE TABLE IF NOT EXISTS growth_records (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    datetime     TEXT NOT NULL UNIQUE,
    date         TEXT NOT NULL,
    weight_lbs   REAL,
    length_in    REAL,
    head_in      REAL,
    note         TEXT
);
CREATE INDEX IF NOT EXISTS idx_growth_date ON growth_records(date);

CREATE TABLE IF NOT EXISTS milestone_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    datetime     TEXT NOT NULL UNIQUE,
    date         TEXT NOT NULL,
    milestone    TEXT NOT NULL,
    note         TEXT
);

CREATE TABLE IF NOT EXISTS other_activities (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    datetime     TEXT NOT NULL UNIQUE,
    date         TEXT NOT NULL,
    activity     TEXT,
    duration_min REAL DEFAULT 0,
    note         TEXT
);

-- ── Materialized daily summary ────────────────────────────────────

CREATE TABLE IF NOT EXISTS daily_summary (
    date                         TEXT PRIMARY KEY,

    -- (a) Total intake
    total_intake_ml              REAL DEFAULT 0,
    -- (b) Breast milk intake = nursing transfer + expressed fed
    bm_intake_ml                 REAL DEFAULT 0,
    -- (b1) Nursing transfer volume (weighed)
    nursing_vol_ml               REAL DEFAULT 0,
    -- (b2) Expressed breast milk fed to baby
    expressed_ml                 REAL DEFAULT 0,
    -- (c) Formula
    formula_ml                   REAL DEFAULT 0,
    -- (d) BM % of intake
    bm_pct                       REAL,
    -- (e) BM supply = nursing transfer + pump output
    bm_supply_ml                 REAL DEFAULT 0,
    -- (g) Pump total
    pump_total_ml                REAL DEFAULT 0,
    -- (f) Nursing % of supply
    nursing_pct_of_supply        REAL,

    -- Pump detail
    pump_sessions_count          INTEGER DEFAULT 0,
    pump_vol_highest_ml          REAL,
    pump_vol_lowest_ml           REAL,

    -- Nursing detail
    nursing_sessions_count       INTEGER DEFAULT 0,
    nursing_total_min            REAL DEFAULT 0,
    nursing_transfer_highest_ml  REAL,
    nursing_transfer_lowest_ml   REAL,

    -- Supplementary
    diaper_wet_count             INTEGER DEFAULT 0,
    diaper_dirty_count           INTEGER DEFAULT 0,
    diaper_mixed_count           INTEGER DEFAULT 0,
    sleep_total_min              REAL DEFAULT 0,
    weight_lbs                   REAL,
    weight_note                  TEXT,

    updated_at                   TEXT DEFAULT (datetime('now'))
);

-- ── Gmail ingestion log ───────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ingestion_log (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    gmail_message_id TEXT UNIQUE,
    filename         TEXT NOT NULL,
    received_at      TEXT NOT NULL,
    processed_at     TEXT,
    rows_added       INTEGER DEFAULT 0,
    rows_updated     INTEGER DEFAULT 0,
    status           TEXT DEFAULT 'pending',
    error_message    TEXT
);
"""


# ─────────────────────────────────────────────
# Connection helper
# ─────────────────────────────────────────────

def get_connection(db_path: Path) -> sqlite3.Connection:
    """Open (or create) the SQLite database and return a connection."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    """Create all tables and indexes (idempotent — uses IF NOT EXISTS)."""
    conn.executescript(_SCHEMA_SQL)
    conn.commit()


# ─────────────────────────────────────────────
# Upsert helpers
# ─────────────────────────────────────────────

def _upsert_df(df: pd.DataFrame, table: str, conn: sqlite3.Connection) -> tuple[int, int]:
    """
    Upsert a DataFrame into a table that has a session_id primary key.
    Uses INSERT OR REPLACE — idempotent for rows with the same session_id.

    Returns (rows_added, rows_replaced).
    """
    if df.empty:
        return 0, 0

    # Count existing rows before upsert for bookkeeping
    existing_ids = set(
        row[0] for row in conn.execute(
            f"SELECT session_id FROM {table} WHERE session_id IN ({','.join('?'*len(df))})",
            df["session_id"].tolist(),
        )
    )
    replaced = len(existing_ids)
    added    = len(df) - replaced

    # Convert datetimes to ISO strings for SQLite TEXT storage
    df = df.copy()
    for col in df.select_dtypes(include="datetime64").columns:
        df[col] = df[col].dt.isoformat()

    df.to_sql(table, conn, if_exists="append", index=False, method="multi")
    # INSERT OR REPLACE is the default for tables that have PK constraints.
    # pandas to_sql uses INSERT, so we need to handle conflicts ourselves.
    # We'll use executemany with INSERT OR REPLACE instead:
    conn.rollback()  # undo the failed insert attempt above

    cols   = ", ".join(df.columns)
    placeholders = ", ".join(["?"] * len(df.columns))
    sql = f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({placeholders})"
    conn.executemany(sql, df.itertuples(index=False, name=None))
    conn.commit()

    return added, replaced


def _upsert_simple(df: pd.DataFrame, table: str, unique_col: str,
                   conn: sqlite3.Connection) -> tuple[int, int]:
    """
    Upsert a DataFrame into a table using a single UNIQUE column
    (no session_id PK).  Used for diaper, sleep, growth, etc.
    """
    if df.empty:
        return 0, 0

    df = df.copy()
    for col in df.select_dtypes(include="datetime64").columns:
        df[col] = df[col].dt.isoformat()

    existing = set(
        row[0] for row in conn.execute(f"SELECT {unique_col} FROM {table}")
    )
    added = replaced = 0
    for row in df.itertuples(index=False):
        row_dict = row._asdict()
        if row_dict[unique_col] in existing:
            # Update non-key columns
            set_clause = ", ".join(
                f"{k}=?" for k in row_dict if k != unique_col
            )
            vals = [v for k, v in row_dict.items() if k != unique_col]
            vals.append(row_dict[unique_col])
            conn.execute(
                f"UPDATE {table} SET {set_clause} WHERE {unique_col}=?", vals
            )
            replaced += 1
        else:
            cols = ", ".join(row_dict.keys())
            placeholders = ", ".join(["?"] * len(row_dict))
            conn.execute(
                f"INSERT INTO {table} ({cols}) VALUES ({placeholders})",
                list(row_dict.values()),
            )
            added += 1
    conn.commit()
    return added, replaced


# ─────────────────────────────────────────────
# High-level upsert entry points (called by loader/merger)
# ─────────────────────────────────────────────

def upsert_nursing(df: pd.DataFrame, conn: sqlite3.Connection):
    return _upsert_df(df, "nursing_sessions", conn)


def upsert_pump(df: pd.DataFrame, conn: sqlite3.Connection):
    return _upsert_df(df, "pump_sessions", conn)


def upsert_expressed(df: pd.DataFrame, conn: sqlite3.Connection):
    return _upsert_df(df, "expressed_sessions", conn)


def upsert_formula(df: pd.DataFrame, conn: sqlite3.Connection):
    return _upsert_df(df, "formula_sessions", conn)


def upsert_diaper(df: pd.DataFrame, conn: sqlite3.Connection):
    # datetime is the unique key for deduplication
    df = df.copy()
    for col in df.select_dtypes(include="datetime64").columns:
        df[col] = df[col].dt.isoformat()
    conn.executemany(
        "INSERT OR IGNORE INTO diaper_events (datetime, date, status, note) VALUES (?,?,?,?)",
        df[["datetime", "date", "status", "note"]].itertuples(index=False, name=None),
    )
    conn.commit()


def upsert_sleep(df: pd.DataFrame, conn: sqlite3.Connection):
    df = df.copy()
    for col in df.select_dtypes(include="datetime64").columns:
        df[col] = df[col].dt.isoformat()
    conn.executemany(
        "INSERT OR IGNORE INTO sleep_sessions (datetime, date, duration_min, note) VALUES (?,?,?,?)",
        df[["datetime", "date", "duration_min", "note"]].itertuples(index=False, name=None),
    )
    conn.commit()


def upsert_growth(df: pd.DataFrame, conn: sqlite3.Connection):
    df = df.copy()
    for col in df.select_dtypes(include="datetime64").columns:
        df[col] = df[col].dt.isoformat()
    conn.executemany(
        """INSERT OR REPLACE INTO growth_records
           (datetime, date, weight_lbs, length_in, head_in, note)
           VALUES (?,?,?,?,?,?)""",
        df[["datetime", "date", "weight_lbs", "length_in", "head_in", "note"]
           ].itertuples(index=False, name=None),
    )
    conn.commit()


def upsert_milestone(df: pd.DataFrame, conn: sqlite3.Connection):
    df = df.copy()
    for col in df.select_dtypes(include="datetime64").columns:
        df[col] = df[col].dt.isoformat()
    conn.executemany(
        "INSERT OR IGNORE INTO milestone_events (datetime, date, milestone, note) VALUES (?,?,?,?)",
        df[["datetime", "date", "milestone", "note"]].itertuples(index=False, name=None),
    )
    conn.commit()


def upsert_other_activity(df: pd.DataFrame, conn: sqlite3.Connection):
    df = df.copy()
    for col in df.select_dtypes(include="datetime64").columns:
        df[col] = df[col].dt.isoformat()
    conn.executemany(
        """INSERT OR IGNORE INTO other_activities
           (datetime, date, activity, duration_min, note) VALUES (?,?,?,?,?)""",
        df[["datetime", "date", "activity", "duration_min", "note"]
           ].itertuples(index=False, name=None),
    )
    conn.commit()
