---
name: inventory
description: Track breast milk inventory (pump vs expressed) from a known starting point to verify no missing pump records
argument-hint: [start-time] [start-inventory] [current-inventory], e.g. "3/20 22:00 0ml 37ml"
allowed-tools: Bash(python3 *), Bash(source *), Read
---

## Task

Track breast milk inventory from a known starting point to now, checking for missing pump records.

Arguments: **$ARGUMENTS**

Parse the arguments to extract:
1. **Start datetime** (e.g., "3/20 22:00")
2. **Starting inventory** (e.g., "0ml")
3. **Current actual inventory** (e.g., "37ml")

If no arguments given, ask the user for these values.

## Setup

```bash
cd "/Users/chenzhu/Claude Code Projects/breast-friend" && source .venv/bin/activate && python3 update.py --skip-sheets --skip-html 2>&1
```

## Data Source

SQLite database: `breast_friend.db`

- `pump_sessions`: datetime, total_ml, note — each pump **adds** to inventory
- `expressed_sessions`: datetime, amount_ml, note — each expressed feed **subtracts** from inventory

## Critical: Check Notes for Discrepancies

Some expressed sessions have notes indicating the amount GIVEN differs from the amount CONSUMED (recorded in `amount_ml`). For example:
- `amount_ml = 22, note = "Given 52ml"` means 52ml was taken from inventory, not 22ml
- The wasted/uneaten portion (52 - 22 = 30ml) still left the inventory

**When calculating inventory changes, use the GIVEN amount from notes if present, not the recorded `amount_ml`.**

Look for patterns in notes like:
- "Given Xml" or "gave Xml" → use X as the inventory deduction
- "扔" or "没吃" → indicates waste, may have additional inventory loss

## Calculation

1. Query all pump and expressed events from the start datetime onward, ordered by datetime
2. For each event:
   - Pump: `inventory += total_ml`
   - Expressed: `inventory -= amount_given` (check notes for actual amount given)
3. Group events into rounds (90-minute gap = new round) for readability

## Output Format

Show per-round inventory changes:

```
轮 | 间隔  | 亲喂     | 泵奶    | 用奶E    | 水奶F    | 库存
---------------------------------------------------------------------
1 |   —  |    —    |  +88   |    —    |    —    |   88ml  03/20 23:57
2 | 280m |  N 96   |  +50   |    —    |    —    |  138ml  03/21 06:29-07:15
...
```

Then show the final comparison:

```
计算库存: XXml  实际: XXml  差额: XXml
```

## Interpretation

- **差额 < 10ml**: No missing records, normal measurement error
- **差额 > 10ml (calculated < actual)**: Likely missing pump record(s). Identify which round(s) have no pump but should (e.g., long interval with nursing but no pump, or inventory going deeply negative mid-day)
- **差额 > 10ml (calculated > actual)**: Likely unrecorded waste/spills, or notes with "given X" not accounted for

Point out the most likely location(s) of missing records based on:
1. Rounds with nursing but no pump (⚠️ flag)
2. Rounds where inventory drops sharply
3. Unusually long gaps without any pump
