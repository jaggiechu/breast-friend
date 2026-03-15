"""
icloud_reader.py — Read the latest Baby Tracker .btbk backup from iCloud.

The .btbk file is a zip archive containing EasyLog.db (SQLite).
This module extracts the DB, parses all tables into DataFrames that
match the existing schema used by data/db.py upsert functions.
"""

import sqlite3
import tempfile
import zipfile
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from config import ICLOUD_BTBK_DIR
from data.nursing_extractor import extract_nursing_ml

log = logging.getLogger(__name__)

# Diaper status int → string mapping
_DIAPER_STATUS = {0: "Wet", 1: "Dirty", 2: "Mixed"}

# Nursing FinishSide int → start_side string
# FinishSide records which side was used last; we map to start_side
_FINISH_SIDE = {0: "Left", 1: "Right", 2: "Both"}


def _find_latest_btbk(btbk_dir: Path = ICLOUD_BTBK_DIR) -> Path:
    """Find the most recently modified .btbk file."""
    files = sorted(btbk_dir.glob("*.btbk"), key=lambda p: p.stat().st_mtime)
    if not files:
        raise FileNotFoundError(f"No .btbk files found in {btbk_dir}")
    latest = files[-1]
    log.info("Using latest .btbk: %s (%.0f KB)", latest.name, latest.stat().st_size / 1024)
    return latest


def _unix_to_iso(ts: float) -> str:
    """Convert unix epoch timestamp to ISO format string for SQLite TEXT storage."""
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _extract_easylog(btbk_path: Path, tmpdir: str) -> sqlite3.Connection:
    """Unzip .btbk and open EasyLog.db."""
    with zipfile.ZipFile(btbk_path, "r") as zf:
        zf.extract("EasyLog.db", tmpdir)
    db_path = Path(tmpdir) / "EasyLog.db"
    return sqlite3.connect(str(db_path))


def _parse_nursing(conn: sqlite3.Connection) -> pd.DataFrame:
    """Parse Nursing table → nursing_sessions schema."""
    df = pd.read_sql("SELECT * FROM Nursing", conn)
    if df.empty:
        return pd.DataFrame(columns=["session_id", "datetime", "date", "start_side",
                                      "left_min", "right_min", "total_min", "note", "nursing_ml"])

    df["datetime"] = df["Time"].apply(_unix_to_iso)
    df["date"] = df["datetime"].str[:10]
    df["start_side"] = df["FinishSide"].map(_FINISH_SIDE)
    df["left_min"] = df["LeftDuration"].fillna(0).astype(float)
    df["right_min"] = df["RightDuration"].fillna(0).astype(float)
    df["total_min"] = df["left_min"] + df["right_min"] + df["BothDuration"].fillna(0).astype(float)
    df["note"] = df["Note"].where(df["Note"].notna() & (df["Note"].str.strip() != ""), None)
    df["nursing_ml"] = df["note"].apply(extract_nursing_ml)
    df["session_id"] = df["ID"]

    return df[["session_id", "datetime", "date", "start_side",
               "left_min", "right_min", "total_min", "note", "nursing_ml"]]


def _parse_pump(conn: sqlite3.Connection) -> pd.DataFrame:
    """Parse Pump table → pump_sessions schema."""
    df = pd.read_sql("SELECT * FROM Pump", conn)
    if df.empty:
        return pd.DataFrame(columns=["session_id", "datetime", "date",
                                      "left_min", "right_min", "total_min",
                                      "left_ml", "right_ml", "total_ml", "note"])

    df["datetime"] = df["Time"].apply(_unix_to_iso)
    df["date"] = df["datetime"].str[:10]
    df["left_min"] = df["LeftDuration"].fillna(0).astype(float)
    df["right_min"] = df["RightDuration"].fillna(0).astype(float)
    df["total_min"] = df["left_min"] + df["right_min"]
    df["left_ml"] = df["LeftAmount"].fillna(0).astype(float)
    df["right_ml"] = df["RightAmount"].fillna(0).astype(float)

    # total_ml: use Amount if available, else left + right
    total_raw = pd.to_numeric(df["Amount"], errors="coerce")
    computed = df["left_ml"] + df["right_ml"]
    df["total_ml"] = total_raw.where(total_raw.notna() & (total_raw > 0), computed)

    df["note"] = df["Note"].where(df["Note"].notna() & (df["Note"].str.strip() != ""), None)
    df["session_id"] = df["ID"]

    return df[["session_id", "datetime", "date",
               "left_min", "right_min", "total_min",
               "left_ml", "right_ml", "total_ml", "note"]]


def _parse_expressed(conn: sqlite3.Connection) -> pd.DataFrame:
    """Parse Pumped table → expressed_sessions schema (BM fed to baby via bottle)."""
    df = pd.read_sql("SELECT * FROM Pumped", conn)
    if df.empty:
        return pd.DataFrame(columns=["session_id", "datetime", "date", "amount_ml", "note"])

    df["datetime"] = df["Time"].apply(_unix_to_iso)
    df["date"] = df["datetime"].str[:10]
    df["amount_ml"] = df["Amount"].fillna(0).astype(float)
    df["note"] = df["Note"].where(df["Note"].notna() & (df["Note"].str.strip() != ""), None)
    df["session_id"] = df["ID"]

    return df[["session_id", "datetime", "date", "amount_ml", "note"]]


def _parse_formula(conn: sqlite3.Connection) -> pd.DataFrame:
    """Parse Formula table → formula_sessions schema."""
    df = pd.read_sql("SELECT * FROM Formula", conn)
    if df.empty:
        return pd.DataFrame(columns=["session_id", "datetime", "date", "amount_ml", "note"])

    df["datetime"] = df["Time"].apply(_unix_to_iso)
    df["date"] = df["datetime"].str[:10]
    df["amount_ml"] = df["Amount"].fillna(0).astype(float)
    df["note"] = df["Note"].where(df["Note"].notna() & (df["Note"].str.strip() != ""), None)
    df["session_id"] = df["ID"]

    return df[["session_id", "datetime", "date", "amount_ml", "note"]]


def _parse_diaper(conn: sqlite3.Connection) -> pd.DataFrame:
    """Parse Diaper table → diaper_events schema."""
    df = pd.read_sql("SELECT * FROM Diaper", conn)
    if df.empty:
        return pd.DataFrame(columns=["datetime", "date", "status", "note"])

    df["datetime"] = df["Time"].apply(_unix_to_iso)
    df["date"] = df["datetime"].str[:10]
    df["status"] = df["Status"].map(_DIAPER_STATUS).fillna("Wet")
    df["note"] = df["Note"].where(df["Note"].notna() & (df["Note"].str.strip() != ""), None)

    return df[["datetime", "date", "status", "note"]]


def _parse_sleep(conn: sqlite3.Connection) -> pd.DataFrame:
    """Parse Sleep table → sleep_sessions schema."""
    df = pd.read_sql("SELECT * FROM Sleep", conn)
    if df.empty:
        return pd.DataFrame(columns=["datetime", "date", "duration_min", "note"])

    df["datetime"] = df["Time"].apply(_unix_to_iso)
    df["date"] = df["datetime"].str[:10]
    df["duration_min"] = df["Duration"].fillna(0).astype(float)
    df["note"] = df["Note"].where(df["Note"].notna() & (df["Note"].str.strip() != ""), None)

    return df[["datetime", "date", "duration_min", "note"]]


def _parse_growth(conn: sqlite3.Connection) -> pd.DataFrame:
    """Parse Growth table → growth_records schema."""
    df = pd.read_sql("SELECT * FROM Growth", conn)
    if df.empty:
        return pd.DataFrame(columns=["datetime", "date", "weight_lbs", "length_in", "head_in", "note"])

    df["datetime"] = df["Time"].apply(_unix_to_iso)
    df["date"] = df["datetime"].str[:10]

    # Weight: if IsEnglishWeightScale=1, already in lbs; else convert kg→lbs
    df["weight_lbs"] = df.apply(
        lambda r: r["Weight"] if r.get("IsEnglishWeightScale", 1) == 1
                  else r["Weight"] * 2.20462 if r["Weight"] else None,
        axis=1,
    )
    # Length/Head: if IsEnglishLengthScale=1, already in inches; else convert cm→in
    for col_src, col_dst in [("Length", "length_in"), ("Head", "head_in")]:
        df[col_dst] = df.apply(
            lambda r, s=col_src: r[s] if r.get("IsEnglishLengthScale", 1) == 1
                      else r[s] * 0.393701 if r[s] else None,
            axis=1,
        )
    df["note"] = df["Note"].where(df["Note"].notna() & (df["Note"].str.strip() != ""), None)

    return df[["datetime", "date", "weight_lbs", "length_in", "head_in", "note"]]


def _parse_milestone(conn: sqlite3.Connection) -> pd.DataFrame:
    """Parse Milestone table → milestone_events schema."""
    df = pd.read_sql("""
        SELECT m.*, ms.Name as MilestoneName
        FROM Milestone m
        LEFT JOIN MilestoneSelection ms ON m.MilestoneSelectionID = ms.ID
    """, conn)
    if df.empty:
        return pd.DataFrame(columns=["datetime", "date", "milestone", "note"])

    df["datetime"] = df["Time"].apply(_unix_to_iso)
    df["date"] = df["datetime"].str[:10]
    df["milestone"] = df["MilestoneName"].fillna("Unknown")
    df["note"] = df["Note"].where(df["Note"].notna() & (df["Note"].str.strip() != ""), None)

    return df[["datetime", "date", "milestone", "note"]]


def _parse_other_activity(conn: sqlite3.Connection) -> pd.DataFrame:
    """Parse OtherActivity table → other_activities schema."""
    df = pd.read_sql("""
        SELECT o.*, od.Name as ActivityName
        FROM OtherActivity o
        LEFT JOIN OtherActivityDesc od ON o.DescID = od.ID
    """, conn)
    if df.empty:
        return pd.DataFrame(columns=["datetime", "date", "activity", "duration_min", "note"])

    df["datetime"] = df["Time"].apply(_unix_to_iso)
    df["date"] = df["datetime"].str[:10]
    df["activity"] = df["ActivityName"].fillna("")
    df["duration_min"] = df["Duration"].fillna(0).astype(float)
    df["note"] = df["Note"].where(df["Note"].notna() & (df["Note"].str.strip() != ""), None)

    return df[["datetime", "date", "activity", "duration_min", "note"]]


def read_latest_btbk(btbk_dir: Path = ICLOUD_BTBK_DIR) -> dict[str, pd.DataFrame]:
    """
    Read the latest .btbk file and return all parsed DataFrames.

    Returns dict keyed by handler name (matching loader._HANDLERS keys):
        nursing, pump, expressed, formula, diaper, sleep, growth, milestone, other
    """
    btbk_path = _find_latest_btbk(btbk_dir)

    with tempfile.TemporaryDirectory() as tmpdir:
        easylog_conn = _extract_easylog(btbk_path, tmpdir)
        try:
            result = {
                "nursing":   _parse_nursing(easylog_conn),
                "pump":      _parse_pump(easylog_conn),
                "expressed": _parse_expressed(easylog_conn),
                "formula":   _parse_formula(easylog_conn),
                "diaper":    _parse_diaper(easylog_conn),
                "sleep":     _parse_sleep(easylog_conn),
                "growth":    _parse_growth(easylog_conn),
                "milestone": _parse_milestone(easylog_conn),
                "other":     _parse_other_activity(easylog_conn),
            }
        finally:
            easylog_conn.close()

    for key, df in result.items():
        log.info("Parsed %s: %d rows", key, len(df))

    return result
