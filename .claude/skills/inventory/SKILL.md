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

## Calculation — EVENT-BY-EVENT, NOT BY ROUND

**CRITICAL: Track inventory at every individual pump/expressed event in chronological order.**

Do NOT group by rounds and then check. Within a single round, pump often happens AFTER expressed feeds. If you check at the round level, you'll miss the moment when inventory goes negative — which is exactly where missing records are hiding.

1. Query all pump and expressed events from the start datetime onward, **ordered by datetime**
2. Walk through each event one by one:
   - Pump: `inventory += total_ml`
   - Expressed: `inventory -= amount_given` (check notes for actual amount given)
3. **Flag every moment inventory goes negative** — this means at that exact timestamp, there wasn't enough milk in storage to give that bottle. Either:
   - A pump session before that event was not recorded
   - Collector milk (passive collection during nursing, typically 5-15ml) was used but not logged as a pump
   - Frozen/stored milk from a separate stash was used

### Collector (接奶器) milk

During nursing, mom sometimes uses a collector on the other breast. This yields 5-15ml that goes into inventory but may NOT be recorded as a pump session. Pump notes sometimes mention "from collector" or "Xml from collector + Yml pump". If inventory is negative by a small amount (≤15ml), unrecorded collector usage is the most likely explanation.

## Output Format

Show the event-by-event inventory ledger:

```
起始库存（来源说明）:                          XXml

HH:MM  P +XX                                 XXml
HH:MM  E -XX                                 XXml
HH:MM  E -XX                                -XXml ← 负了
HH:MM  P +XX                                 XXml
...

计算库存: XXml  实际: XXml  差额: XXml
```

Mark every line where inventory goes negative with `← 负了`.

Then show the final comparison:

```
计算库存: XXml  实际: XXml  差额: XXml
```

## Interpretation

- **差额 < 10ml**: No missing records, normal measurement error
- **差额 10-15ml (calculated < actual)**: Likely unrecorded collector milk (~5-15ml per nursing session)
- **差额 > 15ml (calculated < actual)**: Likely missing pump record(s). Identify where inventory first goes negative — the missing pump must be BEFORE that timestamp
- **差额 > 10ml (calculated > actual)**: Likely unrecorded waste/spills, or notes with "given X" not accounted for

Point out the most likely location(s) of missing records based on:
1. The exact timestamps where inventory goes negative
2. Whether the deficit is small enough to be explained by collector (~15ml) or requires a full missing pump session
3. Check pump notes for "from collector" patterns on nearby days to estimate typical collector yield
