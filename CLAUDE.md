# Breast Friend — Project Instructions

## Auto-refresh Data
**IMPORTANT:** At the start of every conversation in this project, BEFORE answering any user questions, run:
```bash
cd "/Users/chenzhu/Claude Code Projects/breast-friend" && source .venv/bin/activate && python3 update.py --skip-sheets --skip-html 2>&1
```
This ensures the database has the latest data from iCloud Baby Tracker.

Then read the latest daily_summary.csv to have context for answering questions:
```bash
cat "/Users/chenzhu/Claude Code Projects/breast-friend/daily_summary.csv"
```

## Project Overview
- Baby feeding tracker for Liz (born 2026-01-29)
- Data source: Baby Tracker app → iCloud `.btbk` backups (auto-synced)
- Pipeline: `.btbk` (zip of SQLite) → parse → `breast_friend.db` → daily_summary
- Outputs: CSV, Google Sheet, HTML dashboard (GitHub Pages)

## Key Data Notes
- Nursing ml tracking started 2/18/26, reliable from 2/20/26
- Days before 2/20 have no nursing transfer volume — exclude from nursing-related comparisons
- Incomplete days (last record before 11pm) should not be compared with complete days
- `bm_supply_ml` = nursing_vol_ml + pump_total_ml
- `Pumped` table in EasyLog = expressed breast milk fed to baby (not pump output)

## Key Files
- `config.py` — all paths and constants
- `ingestion/icloud_reader.py` — reads .btbk from iCloud
- `data/loader.py` — orchestrates data loading
- `data/aggregator.py` — builds daily_summary
- `output/html_dashboard.py` — generates interactive HTML
- `output/google_sheets.py` — exports to Google Sheets
- `update.py` — main entry point
