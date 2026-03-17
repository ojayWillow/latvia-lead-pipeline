# Latvia Lead Pipeline - Build Plan

## Goal
Build an automated system that finds newly registered Latvian businesses that don't have a website, enriches their contact info, and pushes qualified leads into a Google Sheets CRM so we can offer them affordable, high-quality web development services.

---

## Pipeline Architecture

```
[1] Google Maps Scraper
        |
        v
[2] ur.gov.lv Open Data (new registrations)
        |
        v
[3] Cross-reference & Dedup
        |
        v
[4] Lead Enrichment (phone, email, social)
        |
        v
[5] Google Sheets CRM Export
```

---

## Phase 1: Google Maps Scraper
**File:** `scrapers/google_maps_scraper.py`

### What it does
- Searches Google Maps by category + location (e.g. "frizieris Riga", "auto remonts Daugavpils")
- Extracts: business name, address, phone, website (or lack of), rating, reviews count
- Filters businesses that have NO website field or a Facebook-only link

### Tech
- Option A: Google Maps Places API (official, paid per request)
- Option B: Outscraper API (bulk, cheaper for volume)
- Option C: Playwright/Selenium scraper (free but fragile)

### Search Categories to Target
- Beauty salons / frizieris / skaistumkopsana
- Auto repair / auto serviss / automazgatavas
- Construction / buvnieki / remonts
- Accountants / gramatvediba
- Lawyers / juristi
- Cleaning services / uzkopsana
- Pet services / veterinari
- Dentists / zobarstnieciba
- Restaurants / kafejnicas (small ones)
- Fitness / sporta zales

### Cities to Cover
- Riga, Daugavpils, Liepaja, Jelgava, Jurmala, Ventspils, Rezekne, Valmiera, Ogre, Tukums

### Output
- CSV/JSON with columns: name, category, city, address, phone, has_website (bool), website_url, rating, review_count, google_maps_url

---

## Phase 2: ur.gov.lv Open Data Scraper
**File:** `scrapers/ur_gov_scraper.py`

### What it does
- Downloads daily CSV from dati.ur.gov.lv (Latvian business register open data)
- Filters for newly registered companies (last 30-90 days)
- Filters by relevant NACE codes (services, retail, beauty, construction, etc.)

### Key NACE Codes
- 96.02 - Hairdressing and beauty treatment
- 45.20 - Maintenance and repair of motor vehicles
- 41.20 - Construction of buildings
- 69.20 - Accounting and auditing
- 56.10 - Restaurants and mobile food service
- 86.23 - Dental practice
- 96.01 - Laundry and dry cleaning
- 93.13 - Fitness facilities

### Output
- CSV/JSON with columns: company_name, reg_number, reg_date, nace_code, legal_address, board_members

---

## Phase 3: Cross-reference & Deduplication
**File:** `enrichment/cross_reference.py`

### What it does
- Merges Google Maps results with ur.gov.lv data
- Matches by company name fuzzy matching + address
- Deduplicates entries
- Flags "hot leads" = newly registered (ur.gov.lv) + no website (Google Maps)
- Flags "warm leads" = existing business + no website

### Lead Scoring
- +10 pts: No website at all
- +5 pts: Only Facebook page, no real site
- +8 pts: Registered in last 30 days
- +5 pts: Registered in last 90 days
- +3 pts: Has phone number (contactable)
- +3 pts: Service-based business (beauty, repair, etc.)
- +2 pts: Located in Riga (higher budget potential)

---

## Phase 4: Lead Enrichment
**File:** `enrichment/enrich_leads.py`

### What it does
- For each lead without email: search for Facebook page, extract email/phone from there
- Check if they have any web presence (Google search: "company name" + city)
- Find owner/contact name from ur.gov.lv board member data
- Optionally: check Instagram, LinkedIn for the business

### Tech
- Google Custom Search API or SerpAPI for web presence check
- Simple HTTP requests to check if domains exist
- Facebook Graph API or scraping for page info

---

## Phase 5: Google Sheets CRM Export
**File:** `export/sheets_export.py`

### What it does
- Pushes all qualified leads to a Google Sheet
- Auto-creates columns: name, category, city, phone, email, website_status, lead_score, reg_date, contact_person, notes, status (new/contacted/responded/converted)
- Updates existing rows (doesn't create duplicates on re-runs)
- Sorts by lead_score descending

### Tech
- Google Sheets API v4 + gspread library
- Service account authentication

---

## Phase 6: Orchestrator / CLI
**File:** `main.py`

### Commands
```bash
python main.py scrape-maps          # Run Google Maps scraper
python main.py scrape-registry       # Pull ur.gov.lv data
python main.py cross-reference       # Merge + dedupe + score
python main.py enrich                # Enrich leads with contact info
python main.py export                # Push to Google Sheets
python main.py full-pipeline         # Run everything end to end
```

---

## File Structure

```
latvia-lead-pipeline/
|-- scrapers/
|   |-- __init__.py
|   |-- google_maps_scraper.py
|   |-- ur_gov_scraper.py
|-- enrichment/
|   |-- __init__.py
|   |-- cross_reference.py
|   |-- enrich_leads.py
|-- export/
|   |-- __init__.py
|   |-- sheets_export.py
|-- data/                    # Local data storage (gitignored)
|   |-- raw/
|   |-- processed/
|-- config.py                # API keys, search params, NACE codes
|-- main.py                  # CLI entry point
|-- requirements.txt
|-- .env.example             # Template for API keys
|-- PLAN.md                  # This file
|-- README.md
```

---

## Tech Stack
- **Python 3.11+**
- **Outscraper** or Google Maps Places API
- **requests / httpx** for HTTP calls
- **pandas** for data processing
- **fuzzywuzzy / rapidfuzz** for name matching
- **gspread** + Google Sheets API for CRM export
- **python-dotenv** for env variables
- **click** or **typer** for CLI

---

## Config / Environment Variables
```
GOOGLE_MAPS_API_KEY=
OUTSCRAPER_API_KEY=
GOOGLE_SHEETS_CREDENTIALS_FILE=
GOOGLE_SHEET_ID=
SERP_API_KEY=
```

---

## Build Order
1. [ ] Set up project structure + requirements.txt + .env.example
2. [ ] Build ur.gov.lv scraper (free data, start here)
3. [ ] Build Google Maps scraper (choose API approach)
4. [ ] Build cross-reference + lead scoring
5. [ ] Build enrichment module
6. [ ] Build Google Sheets export
7. [ ] Build CLI orchestrator (main.py)
8. [ ] Test full pipeline end to end
9. [ ] Schedule with cron / n8n for weekly automated runs

---

## Target Output
50-100 qualified leads per week of Latvian businesses that:
- Have no website or only a Facebook page
- Are in service-based industries
- Are contactable (phone or email found)
- Scored and sorted by conversion likelihood
