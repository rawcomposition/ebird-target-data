#!/usr/bin/env python3
"""
Generate eBird observation statistics database.

Uses DuckDB to efficiently process large TSV files (100+ GB) without loading
them entirely into memory.

Usage:
    python generate_data.py <species_file> <sampling_file> <output.db>

Example:
    python generate_data.py ebd_filtered.tsv sampling_filtered.tsv ebird.db

For very large files (100+ GB), you may want to:
    - Use --temp-dir to specify a fast SSD for intermediate data
    - Use --memory-limit to control DuckDB's memory usage (default: 80% of RAM)
    - Use --threads to control parallelism (default: all cores)
    - Use --skip-hotspots to skip hotspot downloads
"""

import argparse
import os
import sqlite3
import sys
import time
from pathlib import Path
from threading import Thread, Lock
from typing import Optional

import duckdb
import requests

from utils import format_duration, load_env_file


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


def is_valid_subnational1(code: str) -> bool:
    """
    Check if a subnational1 code is valid.
    Some eBird codes are malformed (e.g., "CO-" instead of "CO-DC").
    Valid codes have content after the dash.
    """
    if not code:
        return False
    parts = code.split("-", 1)
    return len(parts) > 1 and bool(parts[1])


def get_region_code(hs: dict) -> str:
    """
    Get the most specific valid region code for a hotspot.
    Returns subnational2_code > subnational1_code > country_code.
    """
    if hs.get("subnational2Code"):
        return hs["subnational2Code"]

    sub1 = hs.get("subnational1Code")
    if is_valid_subnational1(sub1):
        return sub1

    return hs.get("countryCode")


def download_hotspots(api_key: str, sqlite_con: sqlite3.Connection, log_state: dict) -> int:
    """
    Download all eBird hotspots by country and insert into hotspots table.
    Returns the number of hotspots inserted.

    log_state dict contains:
        - buffer: list of messages to buffer
        - live: bool, when True print directly instead of buffering
        - lock: threading.Lock for thread safety
    """
    def log(msg: str):
        with log_state["lock"]:
            if log_state["live"]:
                print(msg)
            else:
                log_state["buffer"].append(msg)

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
            region_code TEXT,
            lat REAL,
            lng REAL,
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
                sub1 = hs.get("subnational1Code")
                sqlite_con.execute(
                    """INSERT INTO hotspots
                       (id, name, country_code, subnational1_code, subnational2_code,
                        region_code, lat, lng, num_species, num_checklists)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        hs.get("locId"),
                        hs.get("locName"),
                        hs.get("countryCode"),
                        sub1 if is_valid_subnational1(sub1) else None,
                        hs.get("subnational2Code"),
                        get_region_code(hs),
                        hs.get("lat"),
                        hs.get("lng"),
                        hs.get("numSpeciesAllTime"),
                        hs.get("numChecklistsAllTime"),
                    )
                )

            sqlite_con.commit()
            total_hotspots += len(hotspots)
            log(f"    [{i+1}/{len(countries)}] {country_name}: {len(hotspots):,} hotspots")

        except requests.RequestException as e:
            log(f"    [{i+1}/{len(countries)}] {country_name}: Error - {e}")

        # 2 second pause between countries (except after the last one)
        if i < len(countries) - 1:
            time.sleep(2)

    return total_hotspots


def download_hotspots_background(api_key: str, output_db: Path, result_container: dict, log_state: dict) -> None:
    """
    Background thread function to download hotspots.
    Stores result in result_container dict.
    """
    try:
        sqlite_con = sqlite3.connect(output_db)
        count = download_hotspots(api_key, sqlite_con, log_state)
        sqlite_con.close()
        result_container["count"] = count
        result_container["error"] = None
    except Exception as e:
        result_container["count"] = 0
        result_container["error"] = str(e)


def build_database(
    species_file: Path,
    sampling_file: Path,
    output_db: Path,
    temp_dir: Optional[Path] = None,
    memory_limit: Optional[str] = None,
    threads: Optional[int] = None,
    skip_hotspots: bool = False,
    wilson_z: float = 1.96,
) -> None:
    """
    Build the month_obs database from eBird species and sampling files.
    """
    start_time = time.time()

    # Wilson score constants derived from z-index
    z_sq = wilson_z * wilson_z
    z_sq_half = z_sq / 2
    z_sq_quarter = z_sq / 4

    # Load API key from .env (only needed for hotspots download)
    api_key = None
    if not skip_hotspots:
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
    print(f"Processing sampling file: {sampling_file}")
    print(f"Output database: {output_db}")
    if temp_dir:
        print(f"Temp directory: {temp_dir}")
    if memory_limit:
        print(f"Memory limit: {memory_limit}")
    if threads:
        print(f"Threads: {threads}")

    # Determine number of steps based on skip_hotspots
    total_steps = 6 if skip_hotspots else 7
    step_num = 0

    # Step 1: Download taxonomy (always required for species table)
    hotspot_result = {"count": 0, "error": None}
    hotspot_log_state = {"buffer": [], "live": False, "lock": Lock()}
    hotspot_thread = None

    step_num += 1
    print(f"\nStep {step_num}/{total_steps}: Downloading eBird taxonomy...")
    step_start = time.time()
    sqlite_con = sqlite3.connect(output_db)
    taxonomy_count = download_taxonomy(sqlite_con)
    sqlite_con.close()
    print(f"  Downloaded {taxonomy_count:,} species ({format_duration(time.time() - step_start)})")

    # Step 2: Start hotspots download in background (takes ~17+ minutes due to rate limiting)
    if not skip_hotspots:
        step_num += 1
        if api_key:
            print(f"\nStep {step_num}/{total_steps}: Downloading hotspots in background...")
            hotspot_thread = Thread(
                target=download_hotspots_background,
                args=(api_key, output_db, hotspot_result, hotspot_log_state),
                daemon=True
            )
            hotspot_thread.start()
        else:
            print(f"\nStep {step_num}/{total_steps}: Skipping hotspots download (no API key)")

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
            samples INTEGER NOT NULL,
            score REAL NOT NULL
        )
    """)

    # Step 3: Calculate samples per (location, month) and (location, year) from sampling file
    step_num += 1
    print(f"\nStep {step_num}/{total_steps}: Calculating samples per location...")
    step_start = time.time()
    con.execute(f"""
        CREATE TEMP TABLE samples_agg AS
        SELECT
            "LOCALITY ID" AS location_id,
            EXTRACT(MONTH FROM CAST("OBSERVATION DATE" AS DATE)) AS month,
            COUNT(DISTINCT COALESCE(NULLIF("GROUP IDENTIFIER", ''), "SAMPLING EVENT IDENTIFIER")) AS samples
        FROM read_csv(
            '{sampling_file}',
            delim='\t',
            header=true,
            quote='',
            ignore_errors=true
        )
        GROUP BY location_id, month
    """)
    # Also create yearly samples aggregation
    con.execute("""
        CREATE TEMP TABLE year_samples_agg AS
        SELECT
            location_id,
            SUM(samples) AS samples
        FROM samples_agg
        GROUP BY location_id
    """)
    print(f"  Done ({format_duration(time.time() - step_start)})")

    # Step 4: Calculate observations and join with samples
    step_num += 1
    print(f"\nStep {step_num}/{total_steps}: Calculating observations...")
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
    step_num += 1
    print(f"\nStep {step_num}/{total_steps}: Inserting month_obs into SQLite...")
    step_start = time.time()
    total_rows = 0
    for month in range(1, 13):
        month_start = time.time()
        con.execute(f"""
            INSERT INTO sqlite_db.month_obs (location_id, month, species_id, obs, samples, score)
            SELECT
                o.location_id,
                o.month,
                sp.id,
                o.obs,
                o.samples,
                -- Wilson score lower bound (z={wilson_z})
                (o.obs + {z_sq_half} - {wilson_z} * sqrt(o.obs * (o.samples - o.obs) / o.samples + {z_sq_quarter}))
                    / (o.samples + {z_sq}) AS score
            FROM observations_agg o
            JOIN sqlite_db.species sp ON o.scientific_name = sp.sci_name
            WHERE o.month = {month} AND o.obs > 1
        """)
        month_count = con.execute(f"SELECT COUNT(*) FROM sqlite_db.month_obs WHERE month = {month}").fetchone()[0]
        total_rows += month_count
        if month_count > 0:
            print(f"  Month {month:2d}: {month_count:,} rows ({format_duration(time.time() - month_start)})")
    print(f"  Total: {total_rows:,} rows ({format_duration(time.time() - step_start)})")

    # Step 6: Create and populate year_obs table
    # Aggregate from observations_agg (not month_obs) to avoid losing data filtered at month level
    step_num += 1
    print(f"\nStep {step_num}/{total_steps}: Creating year_obs table...")
    step_start = time.time()

    con.execute("DROP TABLE IF EXISTS sqlite_db.year_obs")
    con.execute("""
        CREATE TABLE sqlite_db.year_obs (
            location_id TEXT NOT NULL,
            species_id INTEGER NOT NULL,
            obs INTEGER NOT NULL,
            samples INTEGER NOT NULL,
            score REAL NOT NULL
        )
    """)

    con.execute(f"""
        INSERT INTO sqlite_db.year_obs (location_id, species_id, obs, samples, score)
        SELECT
            agg.location_id,
            agg.species_id,
            agg.obs,
            ys.samples,
            -- Wilson score lower bound (z={wilson_z})
            (agg.obs + {z_sq_half} - {wilson_z} * sqrt(agg.obs * (ys.samples - agg.obs) / ys.samples + {z_sq_quarter}))
                / (ys.samples + {z_sq}) AS score
        FROM (
            SELECT
                o.location_id,
                sp.id AS species_id,
                SUM(o.obs) AS obs
            FROM observations_agg o
            JOIN sqlite_db.species sp ON o.scientific_name = sp.sci_name
            GROUP BY o.location_id, sp.id
            HAVING SUM(o.obs) > 1
        ) agg
        JOIN year_samples_agg ys ON agg.location_id = ys.location_id
    """)

    year_obs_count = con.execute("SELECT COUNT(*) FROM sqlite_db.year_obs").fetchone()[0]
    print(f"  Created {year_obs_count:,} rows ({format_duration(time.time() - step_start)})")

    # Get summary statistics from DuckDB before closing
    obs_count = con.execute("SELECT COUNT(*) FROM sqlite_db.month_obs").fetchone()[0]
    loc_count = con.execute("SELECT COUNT(DISTINCT location_id) FROM sqlite_db.month_obs").fetchone()[0]
    species_count = con.execute("SELECT COUNT(DISTINCT species_id) FROM sqlite_db.month_obs").fetchone()[0]

    con.close()

    # Wait for hotspots download to complete
    if hotspot_thread:
        if hotspot_thread.is_alive():
            print("\n  Waiting for hotspots download to complete...")
            with hotspot_log_state["lock"]:
                for msg in hotspot_log_state["buffer"]:
                    print(msg)
                hotspot_log_state["buffer"].clear()
                hotspot_log_state["live"] = True

        hotspot_thread.join()

        if hotspot_result["error"]:
            print(f"  Hotspots download error: {hotspot_result['error']}")
        else:
            print(f"  Hotspots complete: {hotspot_result['count']:,} hotspots")

    # Step 7: Create indexes using sqlite3
    step_num += 1
    print(f"\nStep {step_num}/{total_steps}: Creating indexes...")
    step_start = time.time()

    indexes = [
        # Species-based queries (sorted by score)
        "CREATE INDEX IF NOT EXISTS idx_mo_species_score ON month_obs(species_id, score DESC)",
        "CREATE INDEX IF NOT EXISTS idx_yo_species_score ON year_obs(species_id, score DESC)",
        # Location-based queries (finding species at a hotspot, no score sorting)
        "CREATE INDEX IF NOT EXISTS idx_mo_location ON month_obs(location_id, month, species_id)",
        "CREATE INDEX IF NOT EXISTS idx_yo_location ON year_obs(location_id, species_id)",
    ]

    if not skip_hotspots:
        indexes.extend([
            "CREATE INDEX IF NOT EXISTS idx_hotspots_country ON hotspots(country_code)",
            "CREATE INDEX IF NOT EXISTS idx_hotspots_subnational1 ON hotspots(subnational1_code)",
            "CREATE INDEX IF NOT EXISTS idx_hotspots_subnational2 ON hotspots(subnational2_code)",
            "CREATE INDEX IF NOT EXISTS idx_hotspots_region ON hotspots(region_code)",
        ])

    sqlite_con = sqlite3.connect(output_db)
    for index_sql in indexes:
        sqlite_con.execute(index_sql)
    sqlite_con.commit()
    sqlite_con.close()

    print(f"  Done ({format_duration(time.time() - step_start)})")

    # Summary
    total_time = time.time() - start_time
    print("\n" + "=" * 50)
    print("Summary:")
    print(f"  Total month_obs rows: {obs_count:,}")
    print(f"  Total year_obs rows: {year_obs_count:,}")
    print(f"  Total locations: {loc_count:,}")
    print(f"  Unique species: {species_count:,}")
    print(f"  Hotspots: {hotspot_result['count']:,}")
    print(f"  Total time: {format_duration(total_time)}")
    print(f"\nDatabase written to: {output_db}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate eBird observation statistics database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python generate_data.py ebd_filtered.tsv sampling_filtered.tsv output.db

  # Large dataset with memory and temp directory settings
  python generate_data.py ebd_filtered.tsv sampling_filtered.tsv output.db \\
      --memory-limit 24GB --threads 8

  # Skip hotspots download
  python generate_data.py ebd_filtered.tsv sampling_filtered.tsv output.db --skip-hotspots
        """,
    )
    parser.add_argument(
        "species_file",
        type=Path,
        help="Path to species observations file (TSV/TXT)",
    )
    parser.add_argument(
        "sampling_file",
        type=Path,
        help="Path to sampling/checklists file (TSV/TXT)",
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
    parser.add_argument(
        "--skip-hotspots",
        action="store_true",
        help="Skip downloading hotspots from eBird API",
    )
    parser.add_argument(
        "--wilson-z",
        type=float,
        default=1.96,
        help="Z-index for Wilson score calculation (default: 1.96 for 95%% confidence)",
    )

    args = parser.parse_args()

    if not args.species_file.exists():
        print(f"Error: Species file not found: {args.species_file}", file=sys.stderr)
        sys.exit(1)

    if not args.sampling_file.exists():
        print(f"Error: Sampling file not found: {args.sampling_file}", file=sys.stderr)
        sys.exit(1)

    if args.temp_dir and not args.temp_dir.exists():
        print(f"Error: Temp directory not found: {args.temp_dir}", file=sys.stderr)
        sys.exit(1)

    build_database(
        args.species_file,
        args.sampling_file,
        args.output_db,
        temp_dir=args.temp_dir,
        memory_limit=args.memory_limit,
        threads=args.threads,
        skip_hotspots=args.skip_hotspots,
        wilson_z=args.wilson_z,
    )


if __name__ == "__main__":
    main()
