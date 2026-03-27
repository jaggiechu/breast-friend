---
name: supply
description: Comprehensive breast milk supply analysis with lactation consultant-level insights — trends, per-round efficiency, circadian patterns, power pumping detection, and red flags
argument-hint: [date-range, e.g. "3/10-3/21" or "last 7 days"]
allowed-tools: Bash(python3 *), Bash(source *), Read
effort: max
---

## Task

Perform a comprehensive breast milk supply analysis for: **$ARGUMENTS**

If no argument given, default to **last 7 complete days**.

## Context

Baby Liz, born 2026-01-29. Triple-feed workflow: nursing → bottle supplement (expressed + formula) → pump. Nursing ml tracking reliable from 2/20/26 onward.

## Setup

```bash
cd "/Users/chenzhu/Claude Code Projects/breast-friend" && source .venv/bin/activate && python3 update.py --skip-sheets --skip-html 2>&1
```

## Data Source

SQLite database: `breast_friend.db`

Key tables:
- `nursing_sessions`: datetime, date, nursing_ml, total_min, note
- `pump_sessions`: datetime, date, total_ml, total_min, left_ml, right_ml, note
- `expressed_sessions`: datetime, date, amount_ml, note
- `formula_sessions`: datetime, date, amount_ml, note
- `daily_summary`: date, bm_supply_ml, nursing_vol_ml, pump_total_ml, bm_pct, nursing_sessions_count, pump_sessions_count, formula_ml, total_intake_ml, etc.
- `growth_records`: date, weight (lbs), note

## Analysis Framework

Perform ALL of the following analyses. Use per-session and per-round data, not just daily totals.

### 1. Daily Supply Trend

- Show `bm_supply_ml` for each day in range (exclude incomplete days for trend, but note them)
- Calculate 7-day rolling average
- Trend direction: increasing / stable / declining (>10% drop over 5+ days = declining)
- Compare to previous period of same length

### 2. Per-Round Milk Removal Analysis

Query all nursing + pump events, group into rounds (90-min gap = new round).

For each day, calculate:
- Number of rounds
- **Per-round yield** (nursing_ml + pump_ml per round)
- **间隔 vs 产量 correlation**: Does longer interval consistently produce more milk per round?
- Identify **大轮** (high-yield rounds, ≥85ml removal) and their conditions (interval length, time of day)
- Identify **低效轮** (low-yield rounds despite adequate interval)

### 3. Circadian Pattern

Group all pump and nursing sessions by time-of-day buckets:
- **Night/early morning** (00:00-06:00): expect highest pump output (prolactin peak)
- **Morning** (06:00-12:00): expect good output
- **Afternoon** (12:00-18:00): moderate
- **Evening** (18:00-00:00): expect lowest output

For each bucket, show:
- Average per-session pump output
- Average per-session nursing transfer
- Number of sessions

Flag if evening output is <30% of morning (abnormal) or if night sessions are being skipped.

### 4. First-Morning Pump Tracking

The first pump of the day (typically 3-5am) is the single best proxy for overall production capacity.

- Extract the first pump session of each day
- Show the trend: is it increasing, stable, or declining?
- A declining first-morning pump for 5+ days is a **red flag**

### 5. Power Pumping Detection

Look for pump clusters: 2-3 pump sessions within a 60-90 minute window where individual sessions are short (5-20 min) with short rest gaps (5-15 min).

If detected:
- Note the dates and times
- Compare daily pump totals in the 3-7 days after vs before
- Assess effectiveness

### 6. Nursing Transfer Efficiency

- Average nursing transfer per session: `nursing_vol_ml / nursing_sessions_count`
- Transfer rate: `nursing_ml / total_min` per session (mL/minute) — below 1 mL/min may indicate poor transfer
- Compare nursing transfer to pump output at similar times of day
- `nursing_pct_of_supply` trend: is baby getting more efficient at the breast?
- Highest vs lowest transfer ratio: >3x spread suggests inconsistent latch

### 7. Pump Equipment Signals

- Check if `pump_vol_highest_ml` has dropped >20% compared to 7-day rolling average — flag for equipment review
- Check for notes mentioning pump brands/issues (Spectra, Medela, Momcozy, etc.)
- Check left_ml vs right_ml asymmetry if data available

### 8. Supply Regulation Assessment

Baby's age in weeks = (analysis_date - 2026-01-29).days / 7

- **Weeks 1-6**: Endocrine/mixed phase. Supply should be rapidly building.
- **Weeks 6-12**: Transition to autocrine. Supply may plateau — this is normal IF it stabilizes near baby's needs. Supply dips now can become permanent.
- **Weeks 12+**: Established supply. Declining trends need active intervention.

Assess where the baby is on this timeline and what that means for the current data.

### 9. Formula Supplementation Trend

- `formula_ml / total_intake_ml` as percentage over time
- Is it increasing (concerning), stable, or decreasing (positive)?
- Absolute formula amount trend

### 10. Red Flags and Positive Signals

**Check for red flags:**
- [ ] First-morning pump declining 5+ consecutive days
- [ ] `bm_pct` trending downward over 7+ days
- [ ] Total milk removals (nursing + pump) consistently <8/day
- [ ] Average interval between removals >4 hours regularly
- [ ] Increasing formula supplementation ratio
- [ ] Nursing duration increasing without transfer volume increasing
- [ ] `pump_vol_lowest_ml` repeatedly near zero

**Check for positive signals:**
- [ ] `bm_supply_ml` rolling average increasing
- [ ] First-morning pump stable or increasing
- [ ] `bm_pct` stable above 70%
- [ ] `nursing_pct_of_supply` increasing (baby more efficient)
- [ ] Per-session nursing transfer increasing
- [ ] Weight gain on track (150-210g/week)

## Output Format

Structure the output as a report with clear sections. Use tables for data, and end with:

### Summary
- 2-3 key findings
- Any red flags (urgent first)
- Actionable recommendations based on the data (e.g., "avoid intervals <90m between removals", "don't skip the 3-5am pump")

Write in a mix of Chinese and English as appropriate (Chinese for explanations, English for technical terms). Be direct and specific — cite actual numbers and dates, not generalities.
