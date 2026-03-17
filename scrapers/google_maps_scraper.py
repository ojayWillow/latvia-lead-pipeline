"""
Phase 1: Google Maps Scraper

Uses the Outscraper API (primary) or Google Maps Places API (fallback)
to search for businesses by category + city, filtering those with no website.

Outputs: data/raw/google_maps_leads.csv
"""

import logging
import time
from pathlib import Path

import pandas as pd
import requests

from config import (
    CITIES,
    GOOGLE_MAPS_API_KEY,
    OUTSCRAPER_API_KEY,
    SEARCH_CATEGORIES,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_PATH = Path("data/raw/google_maps_leads.csv")


# ---------------------------------------------------------------------------
# Outscraper path
# ---------------------------------------------------------------------------

def scrape_via_outscraper(queries: list[str]) -> list[dict]:
    """
    Call the Outscraper Google Maps API.
    Docs: https://outscraper.com/google-maps-scraper-api/
    """
    if not OUTSCRAPER_API_KEY:
        raise ValueError("OUTSCRAPER_API_KEY is not set in .env")

    url = "https://api.app.outscraper.com/maps/search-v3"
    all_results = []

    for query in queries:
        logger.info(f"Outscraper query: {query}")
        params = {
            "query": query,
            "language": "lv",
            "limit": 100,
            "async": False,
        }
        headers = {"X-API-KEY": OUTSCRAPER_API_KEY}
        resp = requests.get(url, params=params, headers=headers, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("data", [[]])[0]
        all_results.extend(results)
        time.sleep(1)  # polite delay

    return all_results


def normalise_outscraper_result(raw: dict) -> dict:
    """Map Outscraper field names to our schema."""
    website = raw.get("site", "") or ""
    has_website = bool(website and "facebook.com" not in website)
    facebook_only = bool(website and "facebook.com" in website)

    return {
        "name": raw.get("name", ""),
        "category": raw.get("type", ""),
        "city": raw.get("city", ""),
        "address": raw.get("full_address", ""),
        "phone": raw.get("phone", ""),
        "has_website": has_website,
        "website_url": website,
        "facebook_only": facebook_only,
        "rating": raw.get("rating", None),
        "review_count": raw.get("reviews", None),
        "google_maps_url": raw.get("url", ""),
    }


# ---------------------------------------------------------------------------
# Google Maps Places API path (fallback)
# ---------------------------------------------------------------------------

def scrape_via_places_api(query: str, city: str) -> list[dict]:
    """Use Google Maps Places Text Search API."""
    if not GOOGLE_MAPS_API_KEY:
        raise ValueError("GOOGLE_MAPS_API_KEY is not set in .env")

    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    results = []
    next_page_token = None

    while True:
        params = {
            "query": f"{query} {city} Latvia",
            "key": GOOGLE_MAPS_API_KEY,
            "language": "lv",
        }
        if next_page_token:
            params["pagetoken"] = next_page_token
            time.sleep(2)  # Google requires a short delay before using page tokens

        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        for place in data.get("results", []):
            place_id = place.get("place_id", "")
            detail = fetch_place_details(place_id)
            results.append(detail)

        next_page_token = data.get("next_page_token")
        if not next_page_token:
            break

    return results


def fetch_place_details(place_id: str) -> dict:
    """Fetch full details for a place including website."""
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "name,formatted_address,formatted_phone_number,website,rating,user_ratings_total,url,types",
        "key": GOOGLE_MAPS_API_KEY,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    result = resp.json().get("result", {})

    website = result.get("website", "") or ""
    has_website = bool(website and "facebook.com" not in website)
    facebook_only = bool(website and "facebook.com" in website)

    return {
        "name": result.get("name", ""),
        "category": ", ".join(result.get("types", [])),
        "city": "",  # not returned by details endpoint directly
        "address": result.get("formatted_address", ""),
        "phone": result.get("formatted_phone_number", ""),
        "has_website": has_website,
        "website_url": website,
        "facebook_only": facebook_only,
        "rating": result.get("rating", None),
        "review_count": result.get("user_ratings_total", None),
        "google_maps_url": result.get("url", ""),
    }


# ---------------------------------------------------------------------------
# Main run function
# ---------------------------------------------------------------------------

def build_queries() -> list[str]:
    """Build all (category, city) search query strings."""
    return [f"{cat} {city}" for city in CITIES for cat in SEARCH_CATEGORIES]


def run(output_path: Path = OUTPUT_PATH, use_outscraper: bool = True) -> pd.DataFrame:
    """
    Run the Google Maps scraper.
    Prefers Outscraper if API key is available, falls back to Places API.
    Returns DataFrame of leads with no website.
    """
    queries = build_queries()
    logger.info(f"Total queries to run: {len(queries)}")

    if use_outscraper and OUTSCRAPER_API_KEY:
        raw_results = scrape_via_outscraper(queries)
        leads = [normalise_outscraper_result(r) for r in raw_results]
    elif GOOGLE_MAPS_API_KEY:
        leads = []
        for query in queries:
            city = query.split()[-1]
            results = scrape_via_places_api(query, city)
            leads.extend(results)
    else:
        raise RuntimeError("No API key configured. Set OUTSCRAPER_API_KEY or GOOGLE_MAPS_API_KEY in .env")

    df = pd.DataFrame(leads)

    # Filter to no-website leads only
    no_website_df = df[df["has_website"] == False].copy()  # noqa: E712
    logger.info(f"Total results: {len(df):,} | No website: {len(no_website_df):,}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    no_website_df.to_csv(output_path, index=False)
    logger.info(f"Saved {len(no_website_df):,} leads to {output_path}")
    return no_website_df


if __name__ == "__main__":
    run()
