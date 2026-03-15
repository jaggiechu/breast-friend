"""
aggregator.py — Build the daily_summary table from raw event tables.

Called after any data ingestion.  Drops and rebuilds daily_summary from
scratch to avoid stale cached values.

Column mapping:
    (a) total_intake_ml         = nursing_vol_ml + expressed_ml + formula_ml
    (b) bm_intake_ml            = nursing_vol_ml + expressed_ml
    (b1) nursing_vol_ml         = sum of weighed transfer volumes
    (b2) expressed_ml           = sum of expressed feeds given to baby
    (c) formula_ml              = sum of formula given
    (d) bm_pct                  = bm_intake / total_intake × 100
    (e) bm_supply_ml            = nursing_vol_ml + pump_total_ml
    (g) pump_total_ml           = sum of all pump sessions
    (f) nursing_pct_of_supply   = nursing_vol / bm_supply × 100
"""

import sqlite3

import pandas as pd


# ─────────────────────────────────────────────
# SQL for each aggregate
# ─────────────────────────────────────────────

_NURSING_SQL = """
SELECT
    date,
    COUNT(*)                                                AS nursing_sessions_count,
    ROUND(SUM(total_min), 1)                               AS nursing_total_min,
    ROUND(
        COALESCE(SUM(CASE WHEN nursing_ml > 0 THEN nursing_ml ELSE 0 END), 0),
        2
    )                                                       AS nursing_vol_ml,
    -- HIGH / LOW: only for sessions where a transfer was recorded (nursing_ml > 0)
    -- Returns NULL for days with no weighed sessions
    MAX(CASE WHEN nursing_ml > 0 THEN nursing_ml END)       AS nursing_transfer_highest_ml,
    MIN(CASE WHEN nursing_ml > 0 THEN nursing_ml END)       AS nursing_transfer_lowest_ml
FROM nursing_sessions
GROUP BY date
"""

_PUMP_SQL = """
SELECT
    date,
    COUNT(*)                AS pump_sessions_count,
    ROUND(SUM(total_ml), 1) AS pump_total_ml,
    MAX(total_ml)           AS pump_vol_highest_ml,
    MIN(total_ml)           AS pump_vol_lowest_ml
FROM pump_sessions
WHERE total_ml > 0
GROUP BY date
"""

_EXPRESSED_SQL = """
SELECT date, ROUND(SUM(amount_ml), 1) AS expressed_ml
FROM expressed_sessions
GROUP BY date
"""

_FORMULA_SQL = """
SELECT date, ROUND(SUM(amount_ml), 1) AS formula_ml
FROM formula_sessions
GROUP BY date
"""

_DIAPER_SQL = """
SELECT
    date,
    SUM(CASE WHEN status = 'Wet'   THEN 1 ELSE 0 END) AS diaper_wet_count,
    SUM(CASE WHEN status = 'Dirty' THEN 1 ELSE 0 END) AS diaper_dirty_count,
    SUM(CASE WHEN status = 'Mixed' THEN 1 ELSE 0 END) AS diaper_mixed_count
FROM diaper_events
GROUP BY date
"""

_SLEEP_SQL = """
SELECT date, ROUND(SUM(duration_min), 0) AS sleep_total_min
FROM sleep_sessions
GROUP BY date
"""

# Latest weight + note for each date
_WEIGHT_SQL = """
SELECT g.date, g.weight_lbs, g.note AS weight_note
FROM growth_records g
JOIN (
    SELECT date, MAX(datetime) AS max_dt FROM growth_records
    WHERE weight_lbs IS NOT NULL
    GROUP BY date
) latest ON g.date = latest.date AND g.datetime = latest.max_dt
"""

# Union of all dates that appear in any feeding / pump table
_ALL_DATES_SQL = """
SELECT DISTINCT date FROM (
    SELECT date FROM nursing_sessions
    UNION SELECT date FROM pump_sessions
    UNION SELECT date FROM expressed_sessions
    UNION SELECT date FROM formula_sessions
)
ORDER BY date
"""

# Last record time per day across all tables — used to detect incomplete days
_LAST_RECORD_SQL = """
SELECT date, MAX(datetime) AS last_record_time FROM (
    SELECT date, datetime FROM nursing_sessions
    UNION ALL SELECT date, datetime FROM pump_sessions
    UNION ALL SELECT date, datetime FROM expressed_sessions
    UNION ALL SELECT date, datetime FROM formula_sessions
    UNION ALL SELECT date, datetime FROM diaper_events
)
GROUP BY date
"""


# ─────────────────────────────────────────────
# Main rebuild function
# ─────────────────────────────────────────────

def rebuild_daily_summary(conn: sqlite3.Connection) -> None:
    """Drop and rebuild the daily_summary table from raw event tables."""

    from config import DAY_COMPLETE_HOUR, NURSING_ML_RELIABLE_DATE

    nursing   = pd.read_sql(_NURSING_SQL,  conn)
    pump      = pd.read_sql(_PUMP_SQL,     conn)
    expressed = pd.read_sql(_EXPRESSED_SQL, conn)
    formula   = pd.read_sql(_FORMULA_SQL,  conn)
    diaper    = pd.read_sql(_DIAPER_SQL,   conn)
    sleep_    = pd.read_sql(_SLEEP_SQL,    conn)
    weight    = pd.read_sql(_WEIGHT_SQL,   conn)
    dates     = pd.read_sql(_ALL_DATES_SQL, conn)
    last_rec  = pd.read_sql(_LAST_RECORD_SQL, conn)

    # Merge everything onto the date spine
    df = dates.copy()
    df = df.merge(nursing,   on="date", how="left")
    df = df.merge(pump,      on="date", how="left")
    df = df.merge(expressed, on="date", how="left")
    df = df.merge(formula,   on="date", how="left")
    df = df.merge(diaper,    on="date", how="left")
    df = df.merge(sleep_,    on="date", how="left")
    df = df.merge(weight,    on="date", how="left")
    df = df.merge(last_rec,  on="date", how="left")

    # Fill count/sum columns with 0 (NULL means "no data" → treat as 0 for totals)
    zero_fill = [
        "nursing_sessions_count", "nursing_total_min", "nursing_vol_ml",
        "pump_sessions_count", "pump_total_ml",
        "expressed_ml", "formula_ml",
        "diaper_wet_count", "diaper_dirty_count", "diaper_mixed_count",
        "sleep_total_min",
    ]
    df[zero_fill] = df[zero_fill].fillna(0)

    # ── Derived columns (a)–(g) ──

    # (a) Total intake Liz received (known quantities only)
    df["total_intake_ml"] = df["nursing_vol_ml"] + df["expressed_ml"] + df["formula_ml"]

    # (b) BM intake
    df["bm_intake_ml"] = df["nursing_vol_ml"] + df["expressed_ml"]

    # (d) BM % of total intake
    df["bm_pct"] = df.apply(
        lambda r: round(r["bm_intake_ml"] / r["total_intake_ml"] * 100, 1)
                  if r["total_intake_ml"] > 0 else None,
        axis=1,
    )

    # (e) BM supply produced by mom
    df["bm_supply_ml"] = df["nursing_vol_ml"] + df["pump_total_ml"]

    # (f) Nursing % of milk supply
    df["nursing_pct_of_supply"] = df.apply(
        lambda r: round(r["nursing_vol_ml"] / r["bm_supply_ml"] * 100, 1)
                  if r["bm_supply_ml"] > 0 else None,
        axis=1,
    )

    # Round numeric display columns
    for col in ["total_intake_ml", "bm_intake_ml", "bm_supply_ml",
                "pump_total_ml", "nursing_vol_ml", "expressed_ml", "formula_ml",
                "nursing_total_min", "sleep_total_min"]:
        df[col] = df[col].round(1)

    # ── Day completeness flag ──
    # A day is "complete" if the last record is at or after DAY_COMPLETE_HOUR (23:00)
    def _is_complete(last_time):
        if pd.isna(last_time) or not last_time:
            return 0
        try:
            hour = int(str(last_time)[11:13])
            return 1 if hour >= DAY_COMPLETE_HOUR else 0
        except (ValueError, IndexError):
            return 0

    df["is_complete_day"] = df["last_record_time"].apply(_is_complete)

    # ── Nursing ml data availability flag ──
    # Days before NURSING_ML_RELIABLE_DATE have no nursing ml note data
    df["has_nursing_ml"] = (df["date"] >= NURSING_ML_RELIABLE_DATE).astype(int)

    # Final column selection / ordering
    df = df[[
        "date",
        "total_intake_ml",
        "bm_intake_ml",
        "nursing_vol_ml",
        "expressed_ml",
        "formula_ml",
        "bm_pct",
        "bm_supply_ml",
        "pump_total_ml",
        "nursing_pct_of_supply",
        "pump_sessions_count",
        "pump_vol_highest_ml",
        "pump_vol_lowest_ml",
        "nursing_sessions_count",
        "nursing_total_min",
        "nursing_transfer_highest_ml",
        "nursing_transfer_lowest_ml",
        "diaper_wet_count",
        "diaper_dirty_count",
        "diaper_mixed_count",
        "sleep_total_min",
        "weight_lbs",
        "weight_note",
        "last_record_time",
        "is_complete_day",
        "has_nursing_ml",
    ]]

    # Rebuild the table
    df.to_sql("daily_summary", conn, if_exists="replace", index=False)
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_ds_date ON daily_summary(date)"
    )
    conn.commit()


def get_daily_summary(conn: sqlite3.Connection) -> pd.DataFrame:
    """Return the daily_summary table as a DataFrame, newest first."""
    return pd.read_sql(
        "SELECT * FROM daily_summary ORDER BY date DESC", conn
    )
