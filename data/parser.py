"""
parser.py — Parse each raw CSV file into a clean pandas DataFrame.

All parsers use engine='python' + quotechar='"' to handle multi-line
quoted note fields (e.g. "44ml\n" embedded newlines).

Time format in all files: "M/D/YY, H:MM AM/PM"  e.g. "2/26/26, 11:02 AM"
"""

import hashlib
import math
from pathlib import Path

import pandas as pd

from config import CSV_TIME_FORMAT
from data.nursing_extractor import extract_nursing_ml


# ─────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────

def _read_csv(path: Path) -> pd.DataFrame:
    """Read CSV with settings that handle quoted multi-line notes."""
    return pd.read_csv(
        path,
        quotechar='"',
        engine="python",
        skipinitialspace=True,
        dtype=str,        # read everything as str first, parse types explicitly
    )


def _parse_time(series: pd.Series) -> pd.Series:
    """Parse the 'Time' column (strips surrounding quotes if present)."""
    cleaned = series.str.strip().str.strip('"')
    return pd.to_datetime(cleaned, format=CSV_TIME_FORMAT)


def _make_id(*parts: str) -> str:
    """12-char MD5 hash from concatenated string parts — stable session ID."""
    key = "_".join(str(p) for p in parts)
    return hashlib.md5(key.encode()).hexdigest()[:12]


def _note(series: pd.Series) -> pd.Series:
    """Replace NaN notes with None, strip whitespace from others."""
    def clean(v):
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return None
        s = str(v).strip()
        return s if s else None
    return series.apply(clean)


def _to_float(series: pd.Series, default=0.0) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(default)


# ─────────────────────────────────────────────
# Public parsers
# ─────────────────────────────────────────────

def parse_nursing(path: Path) -> pd.DataFrame:
    """
    Parse liz_nursing.csv.

    Returns columns:
        session_id, datetime, date, start_side,
        left_min, right_min, total_min, note, nursing_ml
    """
    df = _read_csv(path)
    df.columns = df.columns.str.strip()

    df["datetime"] = _parse_time(df["Time"])
    df["date"] = df["datetime"].dt.date.astype(str)

    df["start_side"] = df["Start Side"].apply(
        lambda v: str(v).strip() if pd.notna(v) and str(v).strip() else None
    )
    df["left_min"]  = _to_float(df["Left duration (min)"])
    df["right_min"] = _to_float(df["Right duration (min)"])
    df["total_min"] = _to_float(df["Total Duration (min)"])

    df["note"]       = _note(df["Note"])
    df["nursing_ml"] = df["note"].apply(extract_nursing_ml)

    # Stable ID: baby name + ISO datetime string
    df["session_id"] = df.apply(
        lambda r: _make_id("nursing", r["datetime"].isoformat()), axis=1
    )

    return df[["session_id", "datetime", "date", "start_side",
               "left_min", "right_min", "total_min", "note", "nursing_ml"]]


def parse_pump(path: Path) -> pd.DataFrame:
    """
    Parse pump.csv.

    Key logic: Total amount (ml) is sometimes blank even when Left+Right are
    filled.  We compute total_ml = coalesce(Total, Left + Right).

    Returns columns:
        session_id, datetime, date,
        left_min, right_min, total_min,
        left_ml, right_ml, total_ml, note
    """
    df = _read_csv(path)
    df.columns = df.columns.str.strip()

    df["datetime"] = _parse_time(df["Time"])
    df["date"] = df["datetime"].dt.date.astype(str)

    df["left_min"]  = _to_float(df["Left duration (min)"])
    df["right_min"] = _to_float(df["Right duration (min)"])
    df["total_min"] = _to_float(df["Total Duration (min)"])

    df["left_ml"]  = _to_float(df["Left amount (ml)"])
    df["right_ml"] = _to_float(df["Right amount (ml)"])

    total_raw = pd.to_numeric(df["Total amount (ml)"], errors="coerce")
    computed  = df["left_ml"] + df["right_ml"]
    df["total_ml"] = total_raw.where(total_raw.notna(), computed)

    df["note"] = _note(df["Note"])

    df["session_id"] = df["datetime"].apply(
        lambda dt: _make_id("pump", dt.isoformat())
    )

    return df[["session_id", "datetime", "date",
               "left_min", "right_min", "total_min",
               "left_ml", "right_ml", "total_ml", "note"]]


def parse_expressed(path: Path) -> pd.DataFrame:
    """
    Parse liz_expressed.csv (expressed breast milk fed to baby).

    Returns columns:
        session_id, datetime, date, amount_ml, note
    """
    df = _read_csv(path)
    df.columns = df.columns.str.strip()

    df["datetime"]  = _parse_time(df["Time"])
    df["date"]      = df["datetime"].dt.date.astype(str)
    df["amount_ml"] = _to_float(df["Amount (ml)"])
    df["note"]      = _note(df["Note"])

    df["session_id"] = df.apply(
        lambda r: _make_id("expressed", r["datetime"].isoformat()), axis=1
    )
    return df[["session_id", "datetime", "date", "amount_ml", "note"]]


def parse_formula(path: Path) -> pd.DataFrame:
    """
    Parse liz_formula.csv (formula fed to baby).

    Returns columns:
        session_id, datetime, date, amount_ml, note
    """
    df = _read_csv(path)
    df.columns = df.columns.str.strip()

    df["datetime"]  = _parse_time(df["Time"])
    df["date"]      = df["datetime"].dt.date.astype(str)
    df["amount_ml"] = _to_float(df["Amount (ml)"])
    df["note"]      = _note(df["Note"])

    df["session_id"] = df.apply(
        lambda r: _make_id("formula", r["datetime"].isoformat()), axis=1
    )
    return df[["session_id", "datetime", "date", "amount_ml", "note"]]


def parse_diaper(path: Path) -> pd.DataFrame:
    """
    Parse liz_diaper.csv.

    Returns columns:
        datetime, date, status, note
    """
    df = _read_csv(path)
    df.columns = df.columns.str.strip()

    df["datetime"] = _parse_time(df["Time"])
    df["date"]     = df["datetime"].dt.date.astype(str)
    df["status"]   = df["Status"].str.strip()
    df["note"]     = _note(df["Note"])

    return df[["datetime", "date", "status", "note"]]


def parse_sleep(path: Path) -> pd.DataFrame:
    """
    Parse liz_sleep.csv.

    Returns columns:
        datetime, date, duration_min, note
    """
    df = _read_csv(path)
    df.columns = df.columns.str.strip()

    df["datetime"]     = _parse_time(df["Time"])
    df["date"]         = df["datetime"].dt.date.astype(str)
    df["duration_min"] = _to_float(df["Duration(minutes)"])
    df["note"]         = _note(df["Note"])

    return df[["datetime", "date", "duration_min", "note"]]


def parse_growth(path: Path) -> pd.DataFrame:
    """
    Parse liz_growth.csv.

    Returns columns:
        datetime, date, weight_lbs, length_in, head_in, note
    """
    df = _read_csv(path)
    df.columns = df.columns.str.strip()

    df["datetime"]   = _parse_time(df["Time"])
    df["date"]       = df["datetime"].dt.date.astype(str)
    df["weight_lbs"] = pd.to_numeric(df["Weight (lbs.)"],    errors="coerce")
    df["length_in"]  = pd.to_numeric(df["Length (inches)"],  errors="coerce")
    df["head_in"]    = pd.to_numeric(df["Head Size (inches)"], errors="coerce")
    df["note"]       = _note(df["Note"])

    return df[["datetime", "date", "weight_lbs", "length_in", "head_in", "note"]]


def parse_milestone(path: Path) -> pd.DataFrame:
    """
    Parse liz_milestone.csv.

    Returns columns:
        datetime, date, milestone, note
    """
    df = _read_csv(path)
    df.columns = df.columns.str.strip()

    df["datetime"]  = _parse_time(df["Time"])
    df["date"]      = df["datetime"].dt.date.astype(str)
    df["milestone"] = df["Milestone"].str.strip()
    df["note"]      = _note(df["Note"])

    return df[["datetime", "date", "milestone", "note"]]


def parse_other_activity(path: Path) -> pd.DataFrame:
    """
    Parse liz_other_activity.csv.

    Returns columns:
        datetime, date, activity, duration_min, note
    """
    df = _read_csv(path)
    df.columns = df.columns.str.strip()

    df["datetime"]     = _parse_time(df["Time"])
    df["date"]         = df["datetime"].dt.date.astype(str)
    df["activity"]     = df["Other activity"].str.strip()
    df["duration_min"] = _to_float(df["Duration(minutes)"])
    df["note"]         = _note(df["Note"])

    return df[["datetime", "date", "activity", "duration_min", "note"]]
