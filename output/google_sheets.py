"""
google_sheets.py — Export daily_summary to Google Sheets via OAuth.

Uses gspread with OAuth (browser login) for authentication.
First run will open a browser window for Google account authorization.
"""

import json
import logging
from pathlib import Path

import gspread
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from config import GOOGLE_SHEET_ID, GOOGLE_OAUTH_CREDENTIALS, GOOGLE_OAUTH_TOKEN
from data.aggregator import get_daily_summary
from data.db import get_connection
from config import DB_PATH

log = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def _get_credentials() -> Credentials:
    """Get or refresh OAuth credentials, prompting browser login if needed."""
    creds = None

    if GOOGLE_OAUTH_TOKEN.exists():
        creds = Credentials.from_authorized_user_file(str(GOOGLE_OAUTH_TOKEN), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not GOOGLE_OAUTH_CREDENTIALS.exists():
                raise FileNotFoundError(
                    f"OAuth credentials not found at {GOOGLE_OAUTH_CREDENTIALS}. "
                    "Download from Google Cloud Console → APIs & Services → Credentials → "
                    "OAuth 2.0 Client IDs → Download JSON, save as .google_credentials.json"
                )
            flow = InstalledAppFlow.from_client_secrets_file(
                str(GOOGLE_OAUTH_CREDENTIALS), SCOPES
            )
            creds = flow.run_local_server(port=0)

        # Save token for next run
        GOOGLE_OAUTH_TOKEN.write_text(creds.to_json())
        log.info("OAuth token saved to %s", GOOGLE_OAUTH_TOKEN)

    return creds


def export_to_sheets() -> str:
    """
    Export daily_summary to Google Sheet.
    Returns the spreadsheet URL.
    """
    conn = get_connection(DB_PATH)
    try:
        df = get_daily_summary(conn)
    finally:
        conn.close()

    if df.empty:
        log.warning("No daily_summary data to export")
        return ""

    creds = _get_credentials()
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    # Use first worksheet
    ws = sh.sheet1

    # Prepare data: headers + rows
    headers = df.columns.tolist()

    # Convert all values to JSON-safe types
    rows = []
    for _, row in df.iterrows():
        r = []
        for v in row:
            if v is None or (isinstance(v, float) and str(v) == "nan"):
                r.append("")
            else:
                r.append(v)
        rows.append(r)

    # Clear and write
    ws.clear()
    ws.update(values=[headers] + rows, range_name="A1")

    url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}"
    log.info("Exported %d rows to Google Sheet: %s", len(df), url)
    return url
