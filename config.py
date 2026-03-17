import os
from dotenv import load_dotenv

load_dotenv()

# API Keys
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")
OUTSCRAPER_API_KEY = os.getenv("OUTSCRAPER_API_KEY", "")
SERP_API_KEY = os.getenv("SERP_API_KEY", "")
GOOGLE_SHEETS_CREDENTIALS_FILE = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE", "credentials.json")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")

# Target cities
CITIES = [
    "Riga", "Daugavpils", "Liepaja", "Jelgava", "Jurmala",
    "Ventspils", "Rezekne", "Valmiera", "Ogre", "Tukums"
]

# Search categories (Latvian + English)
SEARCH_CATEGORIES = [
    "frizieris", "skaistumkopsana",
    "auto serviss", "automazgatavas",
    "buvnieki", "remonts",
    "gramatvediba",
    "juristi",
    "uzkopsana",
    "veterinari",
    "zobarstnieciba",
    "kafejnicas",
    "sporta zales",
]

# Relevant NACE codes for ur.gov.lv filtering
TARGET_NACE_CODES = [
    "96.02",  # Hairdressing and beauty
    "45.20",  # Motor vehicle repair
    "41.20",  # Construction of buildings
    "69.20",  # Accounting and auditing
    "56.10",  # Restaurants
    "86.23",  # Dental practice
    "96.01",  # Laundry and dry cleaning
    "93.13",  # Fitness facilities
]

# Lead scoring weights
SCORING = {
    "no_website": 10,
    "facebook_only": 5,
    "registered_30_days": 8,
    "registered_90_days": 5,
    "has_phone": 3,
    "service_business": 3,
    "riga_location": 2,
}

# ur.gov.lv open data URL
UR_GOV_DATA_URL = "https://dati.ur.gov.lv/opendata/registr.csv"

# Days lookback for new registrations
NEW_REGISTRATION_DAYS = 90
