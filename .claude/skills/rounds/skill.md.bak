---
name: rounds
description: Display per-round feeding table for a given day or date range, showing nursing, pump, expressed, formula, baby intake, and milk removal per round
argument-hint: [date or date-range, e.g. "3/21" or "3/20-3/21"]
allowed-tools: Bash(python3 *), Bash(source *), Read
---

## Task

Generate a per-round feeding table for the requested date(s): **$ARGUMENTS**

If no argument given, default to **yesterday**.

## Setup

```bash
cd "/Users/chenzhu/Claude Code Projects/breast-friend" && source .venv/bin/activate && python3 update.py --skip-sheets --skip-html 2>&1
```

Run the above first to ensure the database is up to date.

## Data Source

SQLite database: `breast_friend.db`

Tables and key columns:
- `nursing_sessions`: datetime, date, nursing_ml, total_min, note
- `pump_sessions`: datetime, date, total_ml, total_min, note
- `expressed_sessions`: datetime, date, amount_ml, note
- `formula_sessions`: datetime, date, amount_ml, note
- `diaper_events`: datetime, date, status, note
- `sleep_sessions`: datetime, date, duration_min, note

## Round Grouping Logic

**A round = baby eats one meal.** The primary criterion is baby intake reaching the per-feed target, NOT time gaps.

### Algorithm

1. Query ALL events (nursing, pump, expressed, formula) for the requested date(s), sorted by datetime
2. Determine the day's per-feed target: `total_daily_intake / number_of_feeds` (typically ~90-100ml recently)
3. Walk through events in order, accumulating baby intake (nursing_ml + expressed + formula):
   - When cumulative baby intake reaches the per-feed target → this completes a round
   - Any pump event(s) immediately following (before the next feeding event) belong to the same round
   - The next feeding event (nursing, expressed, or formula) starts a new round
4. Time gaps are a **secondary signal only** — a gap >3-4 hours with no events almost certainly means a new round, but a gap <90 min does NOT mean same round if baby already ate a full meal

### Important rules

- A round does NOT always start with nursing — it can start with bottle feed (expressed/formula) when mom skips nursing
- A standalone pump between feeding events belongs to whichever round's feeding it is closest to (usually the one just before)
- Do NOT use a fixed time threshold (like 90 min) as the primary round boundary — two full meals can happen 73 minutes apart

4. For each round, calculate:
   - **间隔**: time from the last event of the previous round to the first event of this round
   - **亲喂 (N)**: sum of `nursing_ml` in this round
   - **泵奶 (P)**: sum of `total_ml` from pump_sessions in this round
   - **瓶喂 (E+F)**: sum of expressed `amount_ml` (as E) + formula `amount_ml` (as F)
   - **宝宝摄入**: nursing_ml + expressed + formula
   - **产奶 (milk removal)**: nursing_ml + pump total_ml

## Output Format

One table per day. Each row is one round. Include time range for each round.

```
================================================================================
 YYYY-MM-DD
================================================================================
  轮 |    间隔 |       亲喂 |    瓶喂(E+F) |       泵奶 |   宝宝摄入 |   产奶
--------------------------------------------------------------------------------
  1 |     — |    N 80ml |      E10    |    P 47ml |     90ml |  127ml  08:40-09:40
  ...
--------------------------------------------------------------------------------
    |       |          |             |           |    XXXml |  XXXml  TOTAL
```

Column formatting:
- 亲喂: show `N {ml}ml` or `—` if no nursing
- 瓶喂: show `E{ml}` and/or `F{ml}` joined with `+`, or `—`
- 泵奶: show `P {ml}ml` or `—` if no pump
- Time range: `HH:MM` if single event, `HH:MM-HH:MM` if multiple events in round

## Special Notes

- If a round has nursing but NO pump, append `⚠️ no pump` flag
- The first round of the day has `—` for 间隔
- Midnight-adjusted timestamps: pump/expressed notes may contain real timestamps like "[1am-1:45am]" or "[12:20am]" — mention this if relevant
- Show day totals at the bottom
