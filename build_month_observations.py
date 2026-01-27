#!/usr/bin/env python3
"""
Build month_obs SQLite database from eBird Basic Dataset.

Uses DuckDB to efficiently process large TSV files (100+ GB) without loading
them entirely into memory.

Usage:
    python build_month_obs.py <species_file> <output.db>

Example:
    python build_month_obs.py ebd_relMar-2025.txt ebird.db

For very large files (100+ GB), you may want to:
    - Use --temp-dir to specify a fast SSD for intermediate data
    - Use --memory-limit to control DuckDB's memory usage (default: 80% of RAM)
    - Use --threads to control parallelism (default: all cores)
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Thread
from typing import Optional

import duckdb
import requests


def format_duration(seconds: float) -> str:
    """Format duration in human-readable form."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.0f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def load_env_file() -> dict:
    """Load environment variables from .env file."""
    env_vars = {}
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    env_vars[key.strip()] = value.strip().strip('"').strip("'")
    return env_vars


def download_taxonomy(sqlite_con: sqlite3.Connection) -> int:
    """
    Download eBird taxonomy and insert into species table.
    Returns the number of species inserted.
    """
    url = "https://api.ebird.org/v2/ref/taxonomy/ebird?fmt=json&cat=species"

    response = requests.get(url, timeout=60)
    response.raise_for_status()
    taxonomy = response.json()

    # Create species table
    sqlite_con.execute("DROP TABLE IF EXISTS species")
    sqlite_con.execute("""
        CREATE TABLE species (
            id INTEGER PRIMARY KEY,
            sci_name TEXT NOT NULL,
            name TEXT NOT NULL,
            code TEXT NOT NULL UNIQUE,
            taxon_order INTEGER NOT NULL
        )
    """)

    # Insert species
    for i, sp in enumerate(taxonomy, start=1):
        sqlite_con.execute(
            "INSERT INTO species (id, sci_name, name, code, taxon_order) VALUES (?, ?, ?, ?, ?)",
            (i, sp["sciName"], sp["comName"], sp["speciesCode"], sp["taxonOrder"])
        )

    sqlite_con.commit()
    return len(taxonomy)


def download_hotspots(api_key: str, sqlite_con: sqlite3.Connection) -> int:
    """
    Download all eBird hotspots by country and insert into hotspots table.
    Returns the number of hotspots inserted.
    """
    # Get country list
    countries_url = f"https://api.ebird.org/v2/ref/region/list/country/world?fmt=json&key={api_key}"
    response = requests.get(countries_url, timeout=60)
    response.raise_for_status()
    countries = response.json()

    # Create hotspots table
    sqlite_con.execute("DROP TABLE IF EXISTS hotspots")
    sqlite_con.execute("""
        CREATE TABLE hotspots (
            id TEXT PRIMARY KEY,
            name TEXT,
            country_code TEXT,
            subnational1_code TEXT,
            subnational2_code TEXT,
            lat REAL,
            lng REAL,
            latest_obs_date TEXT,
            num_species INTEGER,
            num_checklists INTEGER
        )
    """)
    sqlite_con.commit()

    total_hotspots = 0

    for i, country in enumerate(countries):
        country_code = country["code"]
        country_name = country["name"]

        # Download hotspots for this country
        hotspots_url = f"https://api.ebird.org/v2/ref/hotspot/{country_code}?fmt=json&key={api_key}"
        try:
            response = requests.get(hotspots_url, timeout=60)
            response.raise_for_status()
            hotspots = response.json()

            # Insert hotspots
            for hs in hotspots:
                sqlite_con.execute(
                    """INSERT INTO hotspots
                       (id, name, country_code, subnational1_code, subnational2_code,
                        lat, lng, latest_obs_date, num_species, num_checklists)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        hs.get("locId"),
                        hs.get("locName"),
                        hs.get("countryCode"),
                        hs.get("subnational1Code"),
                        hs.get("subnational2Code"),
                        hs.get("lat"),
                        hs.get("lng"),
                        hs.get("latestObsDt"),
                        hs.get("numSpeciesAllTime"),
                        hs.get("numChecklistsAllTime"),
                    )
                )

            sqlite_con.commit()
            total_hotspots += len(hotspots)
            print(f"    [{i+1}/{len(countries)}] {country_name}: {len(hotspots):,} hotspots")

        except requests.RequestException as e:
            print(f"    [{i+1}/{len(countries)}] {country_name}: Error - {e}")

        # 5 second pause between countries (except after the last one)
        if i < len(countries) - 1:
            time.sleep(5)

    return total_hotspots


def download_hotspots_background(api_key: str, output_db: Path, result_container: dict) -> None:
    """
    Background thread function to download hotspots.
    Stores result in result_container dict.
    """
    try:
        sqlite_con = sqlite3.connect(output_db)
        count = download_hotspots(api_key, sqlite_con)
        sqlite_con.close()
        result_container["count"] = count
        result_container["error"] = None
    except Exception as e:
        result_container["count"] = 0
        result_container["error"] = str(e)


def build_database(
    species_file: Path,
    output_db: Path,
    temp_dir: Optional[Path] = None,
    memory_limit: Optional[str] = None,
    threads: Optional[int] = None,
) -> None:
    """
    Build the month_obs database from eBird species file.
    """
    start_time = time.time()

    # Load API key from .env
    env_vars = load_env_file()
    api_key = env_vars.get("EBIRD_API_KEY") or os.environ.get("EBIRD_API_KEY")

    if not api_key:
        print("Warning: EBIRD_API_KEY not found in .env or environment. Skipping hotspots download.")

    # Configure DuckDB for large file processing
    config = {}
    if temp_dir:
        config["temp_directory"] = str(temp_dir)
    if threads:
        config["threads"] = threads

    con = duckdb.connect(config=config) if config else duckdb.connect()

    # Set memory limit if specified
    if memory_limit:
        con.execute(f"SET memory_limit = '{memory_limit}'")

    # Install and load SQLite extension for direct export
    con.execute("INSTALL sqlite; LOAD sqlite;")

    print(f"Processing species file: {species_file}")
    print(f"Output database: {output_db}")
    if temp_dir:
        print(f"Temp directory: {temp_dir}")
    if memory_limit:
        print(f"Memory limit: {memory_limit}")
    if threads:
        print(f"Threads: {threads}")

    # Step 1: Download taxonomy (quick, no API key needed)
    print("\nStep 1/6: Downloading eBird taxonomy...")
    step_start = time.time()
    sqlite_con = sqlite3.connect(output_db)
    taxonomy_count = download_taxonomy(sqlite_con)
    sqlite_con.close()
    print(f"  Downloaded {taxonomy_count:,} species ({format_duration(time.time() - step_start)})")

    # Step 2: Start hotspots download in background (takes ~17+ minutes due to rate limiting)
    hotspot_result = {"count": 0, "error": None}
    hotspot_thread = None
    if api_key:
        print("\nStep 2/6: Starting hotspots download in background...")
        hotspot_thread = Thread(
            target=download_hotspots_background,
            args=(api_key, output_db, hotspot_result),
            daemon=True
        )
        hotspot_thread.start()
    else:
        print("\nStep 2/6: Skipping hotspots download (no API key)")

    # Attach SQLite database for output
    con.execute(f"ATTACH '{output_db}' AS sqlite_db (TYPE SQLITE)")

    # Create month_obs table in SQLite (drop existing if re-running)
    con.execute("DROP TABLE IF EXISTS sqlite_db.month_obs")

    con.execute("""
        CREATE TABLE sqlite_db.month_obs (
            location_id TEXT NOT NULL,
            month INTEGER NOT NULL,
            species_id INTEGER NOT NULL,
            obs INTEGER NOT NULL,
            samples INTEGER NOT NULL
        )
    """)

    # Step 3: Calculate samples per (location, month)
    print("\nStep 3/6: Calculating samples per location/month...")
    step_start = time.time()
    con.execute(f"""
        CREATE TEMP TABLE samples_agg AS
        SELECT
            "LOCALITY ID" AS location_id,
            EXTRACT(MONTH FROM CAST("OBSERVATION DATE" AS DATE)) AS month,
            COUNT(DISTINCT COALESCE(NULLIF("GROUP IDENTIFIER", ''), "SAMPLING EVENT IDENTIFIER")) AS samples
        FROM read_csv(
            '{species_file}',
            delim='\t',
            header=true,
            quote='',
            ignore_errors=true
        )
        GROUP BY location_id, month
    """)
    print(f"  Done ({format_duration(time.time() - step_start)})")

    # Step 4: Calculate observations and join with samples
    print("\nStep 4/6: Calculating observations...")
    step_start = time.time()
    con.execute(f"""
        CREATE TEMP TABLE observations_agg AS
        SELECT
            o.location_id,
            o.month,
            o.scientific_name,
            o.obs,
            s.samples
        FROM (
            SELECT
                "LOCALITY ID" AS location_id,
                EXTRACT(MONTH FROM CAST("OBSERVATION DATE" AS DATE)) AS month,
                "SCIENTIFIC NAME" AS scientific_name,
                COUNT(DISTINCT COALESCE(NULLIF("GROUP IDENTIFIER", ''), "SAMPLING EVENT IDENTIFIER")) AS obs
            FROM read_csv(
                '{species_file}',
                delim='\t',
                header=true,
                quote='',
                ignore_errors=true
            )
            GROUP BY location_id, month, scientific_name
        ) o
        JOIN samples_agg s
            ON o.location_id = s.location_id
            AND o.month = s.month
    """)
    print(f"  Done ({format_duration(time.time() - step_start)})")

    # Step 5: Insert into SQLite by month for progress tracking
    # Join with species table to get species_id
    print("\nStep 5/6: Inserting into SQLite...")
    step_start = time.time()
    total_rows = 0
    for month in range(1, 13):
        month_start = time.time()
        con.execute(f"""
            INSERT INTO sqlite_db.month_obs (location_id, month, species_id, obs, samples)
            SELECT
                o.location_id,
                o.month,
                sp.id,
                o.obs,
                o.samples
            FROM observations_agg o
            JOIN sqlite_db.species sp ON o.scientific_name = sp.sci_name
            WHERE o.month = {month}
        """)
        month_count = con.execute(f"SELECT COUNT(*) FROM sqlite_db.month_obs WHERE month = {month}").fetchone()[0]
        total_rows += month_count
        if month_count > 0:
            print(f"  Month {month:2d}: {month_count:,} rows ({format_duration(time.time() - month_start)})")
    print(f"  Total: {total_rows:,} rows ({format_duration(time.time() - step_start)})")

    # Print summary statistics from DuckDB before closing
    result = con.execute("SELECT COUNT(*) FROM sqlite_db.month_obs").fetchone()
    obs_count = result[0]

    result = con.execute("SELECT COUNT(DISTINCT location_id) FROM sqlite_db.month_obs").fetchone()
    loc_count = result[0]

    result = con.execute("SELECT COUNT(DISTINCT species_id) FROM sqlite_db.month_obs").fetchone()
    species_count = result[0]

    con.close()

    # Wait for hotspots download to complete
    if hotspot_thread:
        print("\n  Waiting for hotspots download to complete...")
        hotspot_thread.join()
        if hotspot_result["error"]:
            print(f"  Hotspots download error: {hotspot_result['error']}")
        else:
            print(f"  Hotspots download complete: {hotspot_result['count']:,} hotspots")

    # Step 6: Create indexes using sqlite3
    print("\nStep 6/6: Creating indexes...")
    step_start = time.time()
    sqlite_con = sqlite3.connect(output_db)
    sqlite_con.execute("CREATE INDEX IF NOT EXISTS idx_mo_species_loc_month ON month_obs(species_id, location_id, month)")
    sqlite_con.execute("CREATE INDEX IF NOT EXISTS idx_month_obs_composite ON month_obs(location_id, month, species_id)")
    sqlite_con.execute("CREATE INDEX IF NOT EXISTS idx_hotspots_country ON hotspots(country_code)")
    sqlite_con.execute("CREATE INDEX IF NOT EXISTS idx_hotspots_subnational1 ON hotspots(subnational1_code)")
    sqlite_con.execute("CREATE INDEX IF NOT EXISTS idx_hotspots_subnational2 ON hotspots(subnational2_code)")
    sqlite_con.commit()
    sqlite_con.close()
    print(f"  Done ({format_duration(time.time() - step_start)})")

    # Summary
    total_time = time.time() - start_time
    print("\n" + "=" * 50)
    print("Summary:")
    print(f"  Total month_obs rows: {obs_count:,}")
    print(f"  Total locations: {loc_count:,}")
    print(f"  Unique species: {species_count:,}")
    print(f"  Hotspots: {hotspot_result['count']:,}")
    print(f"  Total time: {format_duration(total_time)}")
    print(f"\nDatabase written to: {output_db}")


def main():
    parser = argparse.ArgumentParser(
        description="Build month_obs SQLite database from eBird data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python build_month_obs.py ebd_relDec-2025.txt output.db

  # Large dataset with memory and temp directory settings
  python build_month_obs.py ebd_relDec-2025.txt output.db \\
      --memory-limit 24GB --threads 8
        """,
    )
    parser.add_argument(
        "species_file",
        type=Path,
        help="Path to species observations file (TSV/TXT)",
    )
    parser.add_argument(
        "output_db",
        type=Path,
        help="Path to output SQLite database",
    )
    parser.add_argument(
        "--temp-dir",
        type=Path,
        help="Directory for DuckDB temp files (use fast SSD for large datasets)",
    )
    parser.add_argument(
        "--memory-limit",
        type=str,
        help="Memory limit for DuckDB (e.g., '32GB', '80%%')",
    )
    parser.add_argument(
        "--threads",
        type=int,
        help="Number of threads for DuckDB (default: all cores)",
    )

    args = parser.parse_args()

    if not args.species_file.exists():
        print(f"Error: Species file not found: {args.species_file}", file=sys.stderr)
        sys.exit(1)

    if args.temp_dir and not args.temp_dir.exists():
        print(f"Error: Temp directory not found: {args.temp_dir}", file=sys.stderr)
        sys.exit(1)

    build_database(
        args.species_file,
        args.output_db,
        temp_dir=args.temp_dir,
        memory_limit=args.memory_limit,
        threads=args.threads,
    )


if __name__ == "__main__":
    main()
