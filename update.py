#!/usr/bin/env python3
"""
update.py — Main entry point for Breast Friend data pipeline.

Usage:
    python update.py                 # full update: iCloud → DB → CSV + HTML + Google Sheet
    python update.py --skip-sheets   # skip Google Sheets export
    python update.py --skip-html     # skip HTML dashboard generation
"""

import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Breast Friend data pipeline")
    parser.add_argument("--skip-sheets", action="store_true", help="Skip Google Sheets export")
    parser.add_argument("--skip-html", action="store_true", help="Skip HTML dashboard")
    args = parser.parse_args()

    # Step 1: Load data from iCloud
    log.info("Step 1: Loading data from iCloud .btbk backup...")
    from data.loader import reload_from_icloud
    conn = reload_from_icloud()

    # Step 2: Export daily_summary CSV
    log.info("Step 2: Exporting daily_summary.csv...")
    from data.aggregator import get_daily_summary
    df = get_daily_summary(conn)
    df.to_csv("daily_summary.csv", index=False)
    log.info("Saved daily_summary.csv (%d rows)", len(df))
    conn.close()

    # Step 3: Google Sheets
    if not args.skip_sheets:
        log.info("Step 3: Exporting to Google Sheets...")
        try:
            from output.google_sheets import export_to_sheets
            url = export_to_sheets()
            log.info("Google Sheet: %s", url)
        except FileNotFoundError as e:
            log.warning("Skipping Google Sheets: %s", e)
        except Exception as e:
            log.error("Google Sheets export failed: %s", e)
    else:
        log.info("Step 3: Skipping Google Sheets (--skip-sheets)")

    # Step 4: HTML Dashboard
    if not args.skip_html:
        log.info("Step 4: Generating HTML dashboard...")
        from output.html_dashboard import generate_html
        path = generate_html()
        if path:
            log.info("Dashboard: %s", path)
    else:
        log.info("Step 4: Skipping HTML (--skip-html)")

    log.info("Done!")


if __name__ == "__main__":
    main()
