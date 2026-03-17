"""
Latvia Lead Pipeline - CLI Orchestrator

Usage:
    python main.py scrape-maps
    python main.py scrape-registry
    python main.py cross-reference
    python main.py enrich
    python main.py export
    python main.py full-pipeline
"""

import typer

app = typer.Typer(help="Latvia Lead Pipeline CLI")


@app.command("scrape-maps")
def scrape_maps():
    """Run the Google Maps scraper (Phase 1)."""
    from scrapers.google_maps_scraper import run
    typer.echo("Running Google Maps scraper...")
    df = run()
    typer.echo(f"Done. {len(df)} no-website leads saved.")


@app.command("scrape-registry")
def scrape_registry():
    """Pull and filter ur.gov.lv business registry data (Phase 2)."""
    from scrapers.ur_gov_scraper import run
    typer.echo("Downloading ur.gov.lv open data...")
    df = run()
    typer.echo(f"Done. {len(df)} registry leads saved.")


@app.command("cross-reference")
def cross_reference():
    """Merge, deduplicate, and score leads (Phase 3)."""
    from enrichment.cross_reference import run
    typer.echo("Cross-referencing and scoring leads...")
    df = run()
    typer.echo(f"Done. {len(df)} scored leads saved.")


@app.command("enrich")
def enrich():
    """Enrich leads with web/email data (Phase 4)."""
    from enrichment.enrich_leads import run
    typer.echo("Enriching leads...")
    df = run()
    typer.echo(f"Done. {len(df)} enriched leads saved.")


@app.command("export")
def export():
    """Push enriched leads to Google Sheets CRM (Phase 5)."""
    from export.sheets_export import run
    typer.echo("Exporting to Google Sheets...")
    run()
    typer.echo("Done.")


@app.command("full-pipeline")
def full_pipeline():
    """Run the complete pipeline end to end."""
    typer.echo("=== Latvia Lead Pipeline: Full Run ===")
    scrape_registry()
    scrape_maps()
    cross_reference()
    enrich()
    export()
    typer.echo("=== Pipeline complete ===")


if __name__ == "__main__":
    app()
