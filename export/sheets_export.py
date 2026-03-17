"""
Phase 5: Google Sheets CRM Export

Pushes enriched leads to a Google Sheet, updating existing rows
and appending new ones. Sorts by lead_score descending.
"""

import logging
from pathlib import Path

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

from config import GOOGLE_SHEET_ID, GOOGLE_SHEETS_CREDENTIALS_FILE

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

INPUT_PATH = Path("data/processed/enriched_leads.csv")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SHEET_COLUMNS = [
    "name", "category", "city", "address", "phone", "email_hint",
    "website_url", "enriched_url", "has_website", "facebook_only",
    "lead_score", "lead_type", "reg_date", "reg_number",
    "nace_code", "board_members", "google_maps_url",
    "status",  # new / contacted / responded / converted
    "notes",
]


def get_sheet_client() -> gspread.Client:
    creds = Credentials.from_service_account_file(
        GOOGLE_SHEETS_CREDENTIALS_FILE, scopes=SCOPES
    )
    return gspread.authorize(creds)


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Align DataFrame columns to SHEET_COLUMNS, filling missing cols with empty string."""
    for col in SHEET_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[SHEET_COLUMNS].copy()
    df.sort_values("lead_score", ascending=False, inplace=True)
    df["status"] = df["status"].fillna("new")
    df["notes"] = df["notes"].fillna("")
    # Convert all to string to avoid gspread serialisation issues
    df = df.fillna("").astype(str)
    return df


def push_to_sheet(df: pd.DataFrame) -> None:
    """Push DataFrame to Google Sheet, avoiding duplicates on re-runs."""
    client = get_sheet_client()
    sh = client.open_by_key(GOOGLE_SHEET_ID)

    try:
        worksheet = sh.worksheet("Leads")
    except gspread.WorksheetNotFound:
        worksheet = sh.add_worksheet(title="Leads", rows="5000", cols=str(len(SHEET_COLUMNS)))
        logger.info("Created new 'Leads' worksheet")

    existing_data = worksheet.get_all_records()
    existing_names = {row.get("name", "") for row in existing_data}

    # Split into new rows vs updates
    new_rows = df[~df["name"].isin(existing_names)]
    logger.info(f"New rows to add: {len(new_rows)} | Already in sheet: {len(df) - len(new_rows)}")

    if len(existing_data) == 0:
        # Write header + all data
        worksheet.update([SHEET_COLUMNS] + new_rows.values.tolist())
    else:
        # Append only new rows
        if not new_rows.empty:
            worksheet.append_rows(new_rows.values.tolist())

    logger.info("Google Sheets export complete")


def run(input_path: Path = INPUT_PATH) -> None:
    df = pd.read_csv(input_path)
    df = prepare_dataframe(df)
    push_to_sheet(df)


if __name__ == "__main__":
    run()
