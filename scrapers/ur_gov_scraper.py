"""
Phase 2: ur.gov.lv Open Data Scraper

Downloads the Latvian Business Register open data CSV,
filters for newly registered companies in target NACE codes,
and saves results to data/raw/ur_gov_leads.csv
"""

import io
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

from config import TARGET_NACE_CODES, UR_GOV_DATA_URL, NEW_REGISTRATION_DAYS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_PATH = Path("data/raw/ur_gov_leads.csv")


def download_registry_csv(url: str = UR_GOV_DATA_URL) -> pd.DataFrame:
    """Download the ur.gov.lv open data CSV and return as DataFrame."""
    logger.info(f"Downloading business register data from {url}")
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    # The file is UTF-8 encoded; try with latin-1 as fallback
    try:
        df = pd.read_csv(io.StringIO(response.text), low_memory=False)
    except Exception:
        df = pd.read_csv(io.BytesIO(response.content), encoding="latin-1", low_memory=False)

    logger.info(f"Downloaded {len(df):,} total records from business register")
    return df


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise column names to a consistent lowercase snake_case format.
    The ur.gov.lv CSV column names may vary slightly — we map what we can.
    """
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Common column aliases from ur.gov.lv exports
    rename_map = {
        "nosaukums": "company_name",
        "name": "company_name",
        "registracijas_numurs": "reg_number",
        "regnum": "reg_number",
        "registration_number": "reg_number",
        "registracijas_datums": "reg_date",
        "reg_date": "reg_date",
        "registration_date": "reg_date",
        "nace_kods": "nace_code",
        "nace": "nace_code",
        "juridiska_adrese": "legal_address",
        "legal_address": "legal_address",
        "address": "legal_address",
        "valde": "board_members",
        "board": "board_members",
    }
    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)
    return df


def filter_new_registrations(df: pd.DataFrame, days: int = NEW_REGISTRATION_DAYS) -> pd.DataFrame:
    """Keep only companies registered within the last `days` days."""
    if "reg_date" not in df.columns:
        logger.warning("'reg_date' column not found — skipping date filter")
        return df

    df["reg_date"] = pd.to_datetime(df["reg_date"], errors="coerce", dayfirst=True)
    cutoff = datetime.now() - timedelta(days=days)
    filtered = df[df["reg_date"] >= cutoff].copy()
    logger.info(f"Companies registered in last {days} days: {len(filtered):,}")
    return filtered


def filter_by_nace(df: pd.DataFrame, nace_codes: list[str] = TARGET_NACE_CODES) -> pd.DataFrame:
    """Keep only companies with a target NACE code."""
    if "nace_code" not in df.columns:
        logger.warning("'nace_code' column not found — skipping NACE filter")
        return df

    # NACE codes may be stored with or without dots (e.g. '9602' vs '96.02')
    normalised_targets = set(c.replace(".", "") for c in nace_codes)
    df["nace_normalised"] = df["nace_code"].astype(str).str.replace(".", "", regex=False).str.strip()
    filtered = df[df["nace_normalised"].isin(normalised_targets)].copy()
    filtered.drop(columns=["nace_normalised"], inplace=True)
    logger.info(f"Companies matching target NACE codes: {len(filtered):,}")
    return filtered


def select_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Select and order the output columns we care about."""
    desired = ["company_name", "reg_number", "reg_date", "nace_code", "legal_address", "board_members"]
    available = [c for c in desired if c in df.columns]
    missing = [c for c in desired if c not in df.columns]
    if missing:
        logger.warning(f"Columns not found in source data: {missing}")
    return df[available].copy()


def run(output_path: Path = OUTPUT_PATH) -> pd.DataFrame:
    """
    Full scraper run:
    1. Download CSV
    2. Normalise columns
    3. Filter by date
    4. Filter by NACE
    5. Save to CSV
    Returns the filtered DataFrame.
    """
    df = download_registry_csv()
    df = normalise_columns(df)
    df = filter_new_registrations(df)
    df = filter_by_nace(df)
    df = select_output_columns(df)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info(f"Saved {len(df):,} leads to {output_path}")
    return df


if __name__ == "__main__":
    run()
