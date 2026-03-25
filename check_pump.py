#!/usr/bin/env python3
"""
check_pump.py — Check if any pump sessions may have been missed today.

Compares today's pump sessions against the expected schedule based on
recent history, and flags any suspiciously large gaps between sessions.

Usage:
    python3 check_pump.py              # check today
    python3 check_pump.py 2026-03-20   # check a specific date
"""

import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

from config import DB_PATH

# ── Thresholds ──
GAP_WARNING_HOURS = 4.0      # flag gaps larger than this (excluding sleep)
SLEEP_START_HOUR = 23        # assume sleep window 11pm–6am
SLEEP_END_HOUR = 6
RECENT_DAYS = 7              # look back this many complete days for avg


def get_pump_sessions(conn, date_str):
    """Return list of (datetime_str, total_ml) for pump sessions on a date."""
    rows = conn.execute(
        "SELECT datetime, total_ml FROM pump_sessions "
        "WHERE date = ? ORDER BY datetime",
        (date_str,),
    ).fetchall()
    return rows


def get_recent_avg(conn, target_date_str):
    """Average pump session count over recent complete days."""
    rows = conn.execute(
        "SELECT pump_sessions_count FROM daily_summary "
        "WHERE date < ? AND is_complete_day = 1 AND pump_sessions_count > 0 "
        "ORDER BY date DESC LIMIT ?",
        (target_date_str, RECENT_DAYS),
    ).fetchall()
    if not rows:
        return None
    return sum(r[0] for r in rows) / len(rows)


def find_gaps(sessions):
    """Find gaps > GAP_WARNING_HOURS between consecutive pump sessions,
    excluding the overnight sleep window."""
    gaps = []
    for i in range(1, len(sessions)):
        t_prev = datetime.fromisoformat(sessions[i - 1][0])
        t_curr = datetime.fromisoformat(sessions[i][0])
        gap_hours = (t_curr - t_prev).total_seconds() / 3600

        # Skip if the gap spans the sleep window
        if t_prev.hour >= SLEEP_START_HOUR or t_curr.hour <= SLEEP_END_HOUR:
            continue

        if gap_hours >= GAP_WARNING_HOURS:
            gaps.append((t_prev, t_curr, gap_hours))
    return gaps


def check_since_last(sessions, now):
    """Check if too much time has passed since the last pump."""
    if not sessions:
        return None
    last_time = datetime.fromisoformat(sessions[-1][0])
    hours_since = (now - last_time).total_seconds() / 3600
    # Only flag during waking hours
    if now.hour >= SLEEP_END_HOUR and now.hour < SLEEP_START_HOUR:
        if hours_since >= GAP_WARNING_HOURS:
            return (last_time, hours_since)
    return None


def main():
    now = datetime.now()

    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = now.strftime("%Y-%m-%d")

    conn = sqlite3.connect(str(DB_PATH))

    sessions = get_pump_sessions(conn, target_date)
    avg_count = get_recent_avg(conn, target_date)

    print(f"=== 泵奶记录检查: {target_date} ===\n")

    # ── Session list ──
    if not sessions:
        print("⚠️  今天还没有泵奶记录！")
        if avg_count:
            print(f"   (最近 {RECENT_DAYS} 天平均每天 {avg_count:.1f} 次)")
        conn.close()
        return

    print(f"已记录 {len(sessions)} 次泵奶:")
    total_ml = 0
    for dt_str, ml in sessions:
        t = datetime.fromisoformat(dt_str)
        total_ml += ml or 0
        print(f"  {t.strftime('%H:%M')}  —  {ml:.0f} ml")
    print(f"  合计: {total_ml:.0f} ml\n")

    # ── Compare with recent avg ──
    if avg_count:
        print(f"最近 {RECENT_DAYS} 天完整日平均: {avg_count:.1f} 次/天")
        diff = avg_count - len(sessions)
        if diff >= 2:
            print(f"⚠️  今天比平均少 {diff:.0f} 次，可能漏登记了！")
        elif diff >= 1:
            print(f"ℹ️  今天比平均少 {diff:.0f} 次 (可能还没结束)")
        else:
            print("✅ 次数正常")
        print()

    # ── Gap analysis ──
    gaps = find_gaps(sessions)
    if gaps:
        print("⚠️  以下时间段间隔过大 (>{:.0f}小时):".format(GAP_WARNING_HOURS))
        for t1, t2, hrs in gaps:
            print(f"  {t1.strftime('%H:%M')} → {t2.strftime('%H:%M')}  ({hrs:.1f} 小时)")
        print()

    # ── Time since last pump (only for today) ──
    if target_date == now.strftime("%Y-%m-%d"):
        overdue = check_since_last(sessions, now)
        if overdue:
            last_t, hrs = overdue
            print(f"⚠️  距上次泵奶 ({last_t.strftime('%H:%M')}) 已过 {hrs:.1f} 小时")
        elif not gaps:
            print("✅ 今天泵奶间隔正常，没有发现漏登记")

    conn.close()


if __name__ == "__main__":
    main()
