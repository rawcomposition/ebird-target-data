#!/usr/bin/env python3
"""
Build month_observations SQLite database from eBird Basic Dataset.

Uses DuckDB to efficiently process large TSV files (100+ GB) without loading
them entirely into memory.

Usage:
    python build_month_observations.py <species_file> <output.db>

Example:
    python build_month_observations.py ebd_relMar-2025.txt ebird.db

For very large files (100+ GB), you may want to:
    - Use --temp-dir to specify a fast SSD for intermediate data
    - Use --memory-limit to control DuckDB's memory usage (default: 80% of RAM)
    - Use --threads to control parallelism (default: all cores)
"""

import argparse
import sqlite3
import sys
import time
from pathlib import Path
from typing import Optional

import duckdb


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


def build_database(
    species_file: Path,
    output_db: Path,
    temp_dir: Optional[Path] = None,
    memory_limit: Optional[str] = None,
    threads: Optional[int] = None,
) -> None:
    """
    Build the month_observations database from eBird species file.

    Args:
        species_file: Path to the species observations TSV/TXT file
        output_db: Path to output SQLite database
        temp_dir: Directory for DuckDB temporary files (for large datasets)
        memory_limit: Memory limit for DuckDB (e.g., "32GB", "80%")
        threads: Number of threads for DuckDB to use
    """
    start_time = time.time()

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

    # Attach SQLite database for output
    con.execute(f"ATTACH '{output_db}' AS sqlite_db (TYPE SQLITE)")

    # Create tables in SQLite
    con.execute("""
        CREATE TABLE IF NOT EXISTS sqlite_db.location (
            location_id TEXT PRIMARY KEY,
            name TEXT,
            latitude REAL,
            longitude REAL
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS sqlite_db.month_observations (
            location_id TEXT NOT NULL,
            month INTEGER NOT NULL,
            scientific_name TEXT NOT NULL,
            observations INTEGER NOT NULL,
            samplings INTEGER NOT NULL,
            UNIQUE(location_id, month, scientific_name)
        )
    """)

    # Step 1: Calculate samplings per (location, month)
    # A sampling is a unique checklist, but GROUP_IDENTIFIER links shared checklists
    # that should only count once.
    print("\nStep 1/5: Calculating samplings per location/month...")
    step_start = time.time()
    con.execute(f"""
        CREATE TEMP TABLE samplings_agg AS
        SELECT
            "LOCALITY ID" AS location_id,
            EXTRACT(MONTH FROM CAST("OBSERVATION DATE" AS DATE)) AS month,
            -- Count distinct groups (using checklist ID if no group)
            COUNT(DISTINCT COALESCE(NULLIF("GROUP IDENTIFIER", ''), "SAMPLING EVENT IDENTIFIER")) AS samplings
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

    # Step 2: Calculate observations per (location, month, species)
    # Deduplicate by GROUP_IDENTIFIER
    print("\nStep 2/5: Calculating observations per location/month/species...")
    step_start = time.time()
    con.execute(f"""
        CREATE TEMP TABLE observations_agg AS
        SELECT
            "LOCALITY ID" AS location_id,
            EXTRACT(MONTH FROM CAST("OBSERVATION DATE" AS DATE)) AS month,
            "SCIENTIFIC NAME" AS scientific_name,
            -- Count distinct groups where this species was observed
            COUNT(DISTINCT COALESCE(NULLIF("GROUP IDENTIFIER", ''), "SAMPLING EVENT IDENTIFIER")) AS observations
        FROM read_csv(
            '{species_file}',
            delim='\t',
            header=true,
            quote='',
            ignore_errors=true
        )
        GROUP BY location_id, month, scientific_name
    """)
    print(f"  Done ({format_duration(time.time() - step_start)})")

    # Step 3: Join observations with samplings and insert into SQLite
    print("\nStep 3/5: Joining and inserting into SQLite...")
    step_start = time.time()
    con.execute("""
        INSERT INTO sqlite_db.month_observations (location_id, month, scientific_name, observations, samplings)
        SELECT
            o.location_id,
            o.month,
            o.scientific_name,
            o.observations,
            s.samplings
        FROM observations_agg o
        JOIN samplings_agg s
            ON o.location_id = s.location_id
            AND o.month = s.month
    """)
    print(f"  Done ({format_duration(time.time() - step_start)})")

    # Step 4: Location table is created but not populated
    # (location data will be populated by a separate process)
    print("\nStep 4/5: Skipping location table (populated separately)...")

    # Print summary statistics from DuckDB before closing
    result = con.execute("SELECT COUNT(*) FROM sqlite_db.month_observations").fetchone()
    obs_count = result[0]

    result = con.execute("SELECT COUNT(DISTINCT location_id) FROM sqlite_db.month_observations").fetchone()
    loc_count = result[0]

    result = con.execute("SELECT COUNT(DISTINCT scientific_name) FROM sqlite_db.month_observations").fetchone()
    species_count = result[0]

    con.close()

    # Step 5: Create indexes using sqlite3
    print("\nStep 5/5: Creating indexes...")
    step_start = time.time()
    sqlite_con = sqlite3.connect(output_db)
    sqlite_con.execute("CREATE INDEX IF NOT EXISTS idx_month_obs_location ON month_observations(location_id)")
    sqlite_con.execute("CREATE INDEX IF NOT EXISTS idx_month_obs_species ON month_observations(scientific_name)")
    sqlite_con.execute("CREATE INDEX IF NOT EXISTS idx_month_obs_month ON month_observations(month)")
    sqlite_con.commit()
    sqlite_con.close()
    print(f"  Done ({format_duration(time.time() - step_start)})")

    # Summary
    total_time = time.time() - start_time
    print("\n" + "=" * 50)
    print("Summary:")
    print(f"  Total month_observations rows: {obs_count:,}")
    print(f"  Total locations: {loc_count:,}")
    print(f"  Unique species: {species_count:,}")
    print(f"  Total time: {format_duration(total_time)}")
    print(f"\nDatabase written to: {output_db}")


def main():
    parser = argparse.ArgumentParser(
        description="Build month_observations SQLite database from eBird data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python build_month_observations.py ebd_relDec-2025.txt output.db

  # Large dataset with memory and temp directory settings
  python build_month_observations.py ebd_relDec-2025.txt output.db \\
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

    # Validate input file exists
    if not args.species_file.exists():
        print(f"Error: Species file not found: {args.species_file}", file=sys.stderr)
        sys.exit(1)

    # Validate temp directory if specified
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
