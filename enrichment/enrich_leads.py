"""
Phase 4: Lead Enrichment

For each lead, attempts to find:
- Email / phone via web search
- Social media presence (Facebook, Instagram)
- Confirms web presence (or absence)

Outputs: data/processed/enriched_leads.csv
"""

import logging
import time
from pathlib import Path

import pandas as pd
import requests

from config import SERP_API_KEY

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

INPUT_PATH = Path("data/processed/scored_leads.csv")
OUTPUT_PATH = Path("data/processed/enriched_leads.csv")


def check_web_presence(company_name: str, city: str) -> dict:
    """
    Use SerpAPI to check if the business has any web presence.
    Returns dict with 'found_url' and 'email_hint'.
    """
    if not SERP_API_KEY:
        logger.warning("SERP_API_KEY not set — skipping web presence check")
        return {"found_url": "", "email_hint": ""}

    query = f'"{company_name}" {city}'
    url = "https://serpapi.com/search"
    params = {
        "q": query,
        "api_key": SERP_API_KEY,
        "num": 5,
        "gl": "lv",
        "hl": "lv",
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        organic = data.get("organic_results", [])
        found_url = organic[0].get("link", "") if organic else ""

        # Naively scan snippet text for email-like strings
        email_hint = ""
        for result in organic:
            snippet = result.get("snippet", "")
            if "@" in snippet:
                for word in snippet.split():
                    if "@" in word:
                        email_hint = word.strip(",.").strip()
                        break
            if email_hint:
                break

        return {"found_url": found_url, "email_hint": email_hint}
    except Exception as e:
        logger.error(f"SerpAPI error for '{company_name}': {e}")
        return {"found_url": "", "email_hint": ""}


def enrich_row(row: pd.Series) -> pd.Series:
    """Enrich a single lead row."""
    company_name = str(row.get("name", ""))
    city = str(row.get("city", ""))

    presence = check_web_presence(company_name, city)
    row["enriched_url"] = presence["found_url"]
    row["email_hint"] = presence["email_hint"]
    time.sleep(0.5)  # rate limit
    return row


def run(input_path: Path = INPUT_PATH, output_path: Path = OUTPUT_PATH) -> pd.DataFrame:
    df = pd.read_csv(input_path)
    logger.info(f"Enriching {len(df)} leads...")

    enriched = df.apply(enrich_row, axis=1)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    enriched.to_csv(output_path, index=False)
    logger.info(f"Saved {len(enriched):,} enriched leads to {output_path}")
    return enriched


if __name__ == "__main__":
    run()
