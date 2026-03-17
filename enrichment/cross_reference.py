"""
Phase 3: Cross-reference & Deduplication

Merges Google Maps leads with ur.gov.lv data,
scores each lead, and flags hot/warm leads.

Outputs: data/processed/scored_leads.csv
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz, process

from config import SCORING

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

MAPS_PATH = Path("data/raw/google_maps_leads.csv")
UR_GOV_PATH = Path("data/raw/ur_gov_leads.csv")
OUTPUT_PATH = Path("data/processed/scored_leads.csv")


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    maps_df = pd.read_csv(MAPS_PATH)
    ur_df = pd.read_csv(UR_GOV_PATH, parse_dates=["reg_date"])
    logger.info(f"Loaded {len(maps_df)} Maps leads and {len(ur_df)} registry leads")
    return maps_df, ur_df


def fuzzy_match_names(maps_name: str, ur_names: list[str], threshold: int = 80) -> str | None:
    """Return the best matching ur.gov.lv company name, or None if below threshold."""
    result = process.extractOne(maps_name, ur_names, scorer=fuzz.token_sort_ratio)
    if result and result[1] >= threshold:
        return result[0]
    return None


def cross_reference(maps_df: pd.DataFrame, ur_df: pd.DataFrame) -> pd.DataFrame:
    """Merge the two datasets on fuzzy company name match."""
    ur_names = ur_df["company_name"].dropna().tolist()
    ur_lookup = ur_df.set_index("company_name")

    merged_rows = []
    for _, row in maps_df.iterrows():
        matched_name = fuzzy_match_names(str(row.get("name", "")), ur_names)
        merged = row.to_dict()
        if matched_name and matched_name in ur_lookup.index:
            ur_row = ur_lookup.loc[matched_name]
            # Handle duplicate company names (take first)
            if isinstance(ur_row, pd.DataFrame):
                ur_row = ur_row.iloc[0]
            merged["reg_number"] = ur_row.get("reg_number", "")
            merged["reg_date"] = ur_row.get("reg_date", None)
            merged["nace_code"] = ur_row.get("nace_code", "")
            merged["board_members"] = ur_row.get("board_members", "")
            merged["registry_match"] = True
        else:
            merged["reg_number"] = ""
            merged["reg_date"] = None
            merged["nace_code"] = ""
            merged["board_members"] = ""
            merged["registry_match"] = False
        merged_rows.append(merged)

    result = pd.DataFrame(merged_rows)
    logger.info(f"Registry matches found: {result['registry_match'].sum()}")
    return result


def score_lead(row: pd.Series) -> int:
    """Calculate lead score based on defined criteria."""
    score = 0

    if not row.get("has_website", True):
        score += SCORING["no_website"]
    if row.get("facebook_only", False):
        score += SCORING["facebook_only"]

    reg_date = row.get("reg_date")
    if pd.notna(reg_date):
        days_old = (datetime.now() - pd.Timestamp(reg_date)).days
        if days_old <= 30:
            score += SCORING["registered_30_days"]
        elif days_old <= 90:
            score += SCORING["registered_90_days"]

    if row.get("phone", ""):
        score += SCORING["has_phone"]

    service_keywords = ["beauty", "repair", "construction", "salon", "frizier",
                        "serviss", "buvnieki", "gramatvediba", "zobarstnieciba"]
    name_lower = str(row.get("name", "")).lower()
    category_lower = str(row.get("category", "")).lower()
    if any(kw in name_lower or kw in category_lower for kw in service_keywords):
        score += SCORING["service_business"]

    if "riga" in str(row.get("city", "")).lower() or "riga" in str(row.get("address", "")).lower():
        score += SCORING["riga_location"]

    return score


def classify_lead(row: pd.Series) -> str:
    """Classify as hot, warm, or cold."""
    is_new = pd.notna(row.get("reg_date")) and (
        (datetime.now() - pd.Timestamp(row["reg_date"])).days <= 90
    )
    no_site = not row.get("has_website", True)
    if is_new and no_site:
        return "hot"
    if no_site:
        return "warm"
    return "cold"


def run(output_path: Path = OUTPUT_PATH) -> pd.DataFrame:
    maps_df, ur_df = load_data()
    merged = cross_reference(maps_df, ur_df)
    merged["lead_score"] = merged.apply(score_lead, axis=1)
    merged["lead_type"] = merged.apply(classify_lead, axis=1)
    merged.sort_values("lead_score", ascending=False, inplace=True)
    merged.drop_duplicates(subset=["name", "address"], inplace=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_path, index=False)
    logger.info(f"Saved {len(merged):,} scored leads to {output_path}")
    return merged


if __name__ == "__main__":
    run()
