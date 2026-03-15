"""
config.py — Paths, constants, and configuration for Breast Friend dashboard.
"""
import os
from pathlib import Path

BASE_DIR = Path(__file__).parent
RAWDATA_DIR = BASE_DIR / "rawdata"
INBOX_DIR = BASE_DIR / "inbox"
DB_PATH = BASE_DIR / "breast_friend.db"

# Gmail OAuth credential files (user must provide .gmail_credentials.json)
GMAIL_CREDENTIALS_PATH = BASE_DIR / ".gmail_credentials.json"
GMAIL_TOKEN_PATH = BASE_DIR / ".gmail_token.json"

# Nursing weigh-tracking started on 2/18/26 (partial), reliable from 2/20/26
# Days before this date have no nursing ml data — exclude from nursing-ml charts
NURSING_ML_START_DATE = "2026-02-18"
NURSING_ML_RELIABLE_DATE = "2026-02-20"

# Hour threshold for considering a day "complete" (last record >= this hour)
DAY_COMPLETE_HOUR = 23

# Map string keys to raw CSV file paths
CSV_FILES = {
    "nursing":    RAWDATA_DIR / "liz_nursing.csv",
    "pump":       RAWDATA_DIR / "pump.csv",
    "expressed":  RAWDATA_DIR / "liz_expressed.csv",
    "formula":    RAWDATA_DIR / "liz_formula.csv",
    "diaper":     RAWDATA_DIR / "liz_diaper.csv",
    "sleep":      RAWDATA_DIR / "liz_sleep.csv",
    "growth":     RAWDATA_DIR / "liz_growth.csv",
    "milestone":  RAWDATA_DIR / "liz_milestone.csv",
    "other":      RAWDATA_DIR / "liz_other_activity.csv",
}

# Cycle-grouping time windows (in minutes)
CYCLE_DIAPER_LOOKBACK_MIN = 30    # diaper before nursing start
CYCLE_EXPRESSED_WINDOW_MIN = 60   # expressed feed after nursing end
CYCLE_FORMULA_WINDOW_MIN = 60     # formula feed after nursing end
CYCLE_PUMP_WINDOW_MIN = 90        # pump session after nursing end

# Time format used in all CSV files
CSV_TIME_FORMAT = "%m/%d/%y, %I:%M %p"   # e.g. "2/26/26, 11:02 AM"

ICLOUD_BTBK_DIR = Path.home() / "Library/Mobile Documents/iCloud~com~nighp~babytracker/Documents/backups"

GOOGLE_SHEET_ID = "17LFMH81MrENIyyVAF4Fg8jgVF1R1gJ4y0_meTux4JGE"
GOOGLE_OAUTH_CREDENTIALS = BASE_DIR / ".google_credentials.json"
GOOGLE_OAUTH_TOKEN = BASE_DIR / ".google_token.json"

HTML_OUTPUT_DIR = BASE_DIR / "docs"

APP_TITLE = "🍼 Breast Friend — Liz's Tracker"
BABY_NAME = "Liz"
