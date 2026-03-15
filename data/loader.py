"""
loader.py — Orchestrates CSV → SQLite database population.

Entry points:
  initialize_db_if_empty()  — called on app startup; creates schema and
                               loads all rawdata CSVs if the DB is empty.
  reload_from_rawdata()     — force-reloads all raw CSVs (useful after
                               manually updating rawdata/ files).
"""

import sqlite3
import logging

from config import DB_PATH, CSV_FILES
from data.db import (
    get_connection, create_schema,
    upsert_nursing, upsert_pump, upsert_expressed, upsert_formula,
    upsert_diaper, upsert_sleep, upsert_growth, upsert_milestone,
    upsert_other_activity,
)
from data.parser import (
    parse_nursing, parse_pump, parse_expressed, parse_formula,
    parse_diaper, parse_sleep, parse_growth, parse_milestone,
    parse_other_activity,
)
from data.aggregator import rebuild_daily_summary

log = logging.getLogger(__name__)

# Maps CSV key → (parser_fn, upsert_fn)
_HANDLERS = {
    "nursing":   (parse_nursing,        upsert_nursing),
    "pump":      (parse_pump,           upsert_pump),
    "expressed": (parse_expressed,      upsert_expressed),
    "formula":   (parse_formula,        upsert_formula),
    "diaper":    (parse_diaper,         upsert_diaper),
    "sleep":     (parse_sleep,          upsert_sleep),
    "growth":    (parse_growth,         upsert_growth),
    "milestone": (parse_milestone,      upsert_milestone),
    "other":     (parse_other_activity, upsert_other_activity),
}


def _load_csv(key: str, conn: sqlite3.Connection) -> dict:
    path = CSV_FILES[key]
    if not path.exists():
        log.warning("CSV not found, skipping: %s", path)
        return {"key": key, "rows": 0, "status": "skipped"}

    parse_fn, upsert_fn = _HANDLERS[key]
    df = parse_fn(path)
    upsert_fn(df, conn)
    log.info("Loaded %s: %d rows from %s", key, len(df), path.name)
    return {"key": key, "rows": len(df), "status": "ok"}


def load_all_csvs(conn: sqlite3.Connection) -> list[dict]:
    """Parse and upsert all known CSV files, then rebuild daily_summary."""
    results = []
    for key in _HANDLERS:
        results.append(_load_csv(key, conn))
    rebuild_daily_summary(conn)
    return results


def initialize_db_if_empty() -> sqlite3.Connection:
    """
    Open (or create) the database, ensure schema exists, and populate
    from raw CSVs if the nursing_sessions table is empty.

    Returns the open connection (caller should close it or use as context).
    """
    conn = get_connection(DB_PATH)
    create_schema(conn)

    row_count = conn.execute(
        "SELECT COUNT(*) FROM nursing_sessions"
    ).fetchone()[0]

    if row_count == 0:
        log.info("Database is empty — loading from rawdata/")
        load_all_csvs(conn)
    else:
        log.info("Database already has %d nursing sessions.", row_count)

    return conn


def reload_from_rawdata() -> sqlite3.Connection:
    """
    Force-reload all CSVs regardless of existing data.
    Existing rows are upserted (INSERT OR REPLACE), so no data is lost.
    """
    conn = get_connection(DB_PATH)
    create_schema(conn)
    load_all_csvs(conn)
    return conn


def reload_from_icloud() -> sqlite3.Connection:
    """
    Read the latest .btbk backup from iCloud and load into the database.
    Uses the same upsert functions as CSV loading — fully idempotent.
    """
    from ingestion.icloud_reader import read_latest_btbk

    conn = get_connection(DB_PATH)
    create_schema(conn)

    dataframes = read_latest_btbk()

    # Map DataFrame keys to upsert functions
    _UPSERT_MAP = {
        "nursing":   upsert_nursing,
        "pump":      upsert_pump,
        "expressed": upsert_expressed,
        "formula":   upsert_formula,
        "diaper":    upsert_diaper,
        "sleep":     upsert_sleep,
        "growth":    upsert_growth,
        "milestone": upsert_milestone,
        "other":     upsert_other_activity,
    }

    for key, df in dataframes.items():
        if df.empty:
            log.info("iCloud %s: 0 rows, skipping", key)
            continue
        upsert_fn = _UPSERT_MAP[key]
        upsert_fn(df, conn)
        log.info("iCloud %s: %d rows upserted", key, len(df))

    rebuild_daily_summary(conn)
    log.info("Daily summary rebuilt from iCloud data")
    return conn
