#!/usr/bin/env python3
"""
Extract only the columns needed for build_month_observations.py from a gzipped eBird file.

This reduces the file size significantly by keeping only essential columns,
making subsequent processing faster and requiring less disk space.

Usage:
    python extract_columns.py <input.txt.gz> <output.tsv>

Example:
    python extract_columns.py ebd_relDec-2025.txt.gz ebd_filtered.tsv
"""

import argparse
import csv
import gzip
import sys
import time
from pathlib import Path

# Columns needed by build_month_observations.py
REQUIRED_COLUMNS = [
    "LOCALITY ID",
    "LOCALITY",
    "LOCALITY TYPE",
    "LATITUDE",
    "LONGITUDE",
    "OBSERVATION DATE",
    "SAMPLING EVENT IDENTIFIER",
    "ALL SPECIES REPORTED",
    "GROUP IDENTIFIER",
    "CATEGORY",
    "SCIENTIFIC NAME",
]


def format_size(bytes_count: int) -> str:
    """Format bytes as human-readable size."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_count < 1024:
            return f"{bytes_count:.1f} {unit}"
        bytes_count /= 1024
    return f"{bytes_count:.1f} PB"


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


def extract_columns(input_file: Path, output_file: Path) -> None:
    """
    Stream through gzipped input and write filtered TSV output.

    Args:
        input_file: Path to gzipped eBird species file
        output_file: Path to output TSV file
    """
    start_time = time.time()
    rows_processed = 0
    bytes_read = 0

    print(f"Input: {input_file}")
    print(f"Output: {output_file}")
    print(f"Extracting columns: {', '.join(REQUIRED_COLUMNS)}")
    print()

    with gzip.open(input_file, "rt", encoding="utf-8", errors="replace") as infile:
        with open(output_file, "w", newline="", encoding="utf-8") as outfile:
            reader = csv.DictReader(infile, delimiter="\t", quoting=csv.QUOTE_NONE)

            # Verify all required columns exist
            missing = set(REQUIRED_COLUMNS) - set(reader.fieldnames or [])
            if missing:
                print(f"Error: Missing columns: {missing}", file=sys.stderr)
                sys.exit(1)

            writer = csv.DictWriter(
                outfile,
                fieldnames=REQUIRED_COLUMNS,
                delimiter="\t",
                extrasaction="ignore",
                quoting=csv.QUOTE_NONE,
                quotechar=None,
                escapechar=None,
            )
            writer.writeheader()

            for row in reader:
                # Skip incomplete checklists
                if row["ALL SPECIES REPORTED"] != "1":
                    continue

                writer.writerow({col: row[col] for col in REQUIRED_COLUMNS})
                rows_processed += 1

                # Progress update every 1 million rows
                if rows_processed % 1_000_000 == 0:
                    elapsed = time.time() - start_time
                    rate = rows_processed / elapsed
                    print(
                        f"  Processed {rows_processed:,} rows "
                        f"({format_duration(elapsed)}, {rate:,.0f} rows/sec)"
                    )

    # Final stats
    elapsed = time.time() - start_time
    output_size = output_file.stat().st_size

    print()
    print("=" * 50)
    print(f"Rows processed: {rows_processed:,}")
    print(f"Output size: {format_size(output_size)}")
    print(f"Total time: {format_duration(elapsed)}")
    print(f"Output written to: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Extract required columns from gzipped eBird file."
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Path to gzipped eBird species file (.txt.gz)",
    )
    parser.add_argument(
        "output_file",
        type=Path,
        help="Path to output TSV file",
    )

    args = parser.parse_args()

    if not args.input_file.exists():
        print(f"Error: Input file not found: {args.input_file}", file=sys.stderr)
        sys.exit(1)

    extract_columns(args.input_file, args.output_file)


if __name__ == "__main__":
    main()
