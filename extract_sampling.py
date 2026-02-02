#!/usr/bin/env python3
"""
Extract only the columns needed for sample counting from a gzipped eBird sampling file.

Filters to complete checklists at hotspots only.

Usage:
    python extract_sampling.py <input.txt.gz> <output.tsv>

Example:
    python extract_sampling.py ebd_sampling_relDec-2025.txt.gz sampling_filtered.tsv
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

from utils import format_duration, format_size

# Columns needed for sample calculation and hotspot extraction
REQUIRED_COLUMNS = [
    "LOCALITY ID",
    "LOCALITY",
    "OBSERVATION DATE",
    "SAMPLING EVENT IDENTIFIER",
    "GROUP IDENTIFIER",
    "COUNTRY CODE",
    "STATE CODE",
    "COUNTY CODE",
    "LATITUDE",
    "LONGITUDE",
]


def extract_sampling(input_file: Path, output_file: Path) -> None:
    """
    Stream through gzipped sampling input and write filtered TSV output.

    Uses pigz for parallel decompression and simple string splitting
    for faster parsing.

    Args:
        input_file: Path to gzipped eBird sampling file
        output_file: Path to output TSV file
    """
    start_time = time.time()
    rows_processed = 0
    rows_skipped = 0

    print(f"Input: {input_file}")
    print(f"Output: {output_file}")
    print(f"Extracting columns: {', '.join(REQUIRED_COLUMNS)}")
    print()

    # Use pigz for parallel decompression (much faster than Python's gzip)
    proc = subprocess.Popen(
        ["pigz", "-dc", str(input_file)],
        stdout=subprocess.PIPE,
        bufsize=1024 * 1024,  # 1MB buffer
    )

    with open(output_file, "w", encoding="utf-8") as outfile:
        # Read and parse header line
        header_line = proc.stdout.readline().decode("utf-8", errors="replace")
        header_cols = header_line.rstrip("\n").split("\t")

        # Build index mapping for required columns
        try:
            col_indices = [header_cols.index(col) for col in REQUIRED_COLUMNS]
        except ValueError as e:
            print(f"Error: Missing column in input file: {e}", file=sys.stderr)
            proc.terminate()
            sys.exit(1)

        # Find indices for filter columns
        all_species_idx = header_cols.index("ALL SPECIES REPORTED")
        locality_type_idx = header_cols.index("LOCALITY TYPE")

        # Write header
        outfile.write("\t".join(REQUIRED_COLUMNS) + "\n")

        # Process data rows
        for line_bytes in proc.stdout:
            cols = line_bytes.decode("utf-8", errors="replace").rstrip("\n").split("\t")

            # Filter: complete checklists at hotspots only
            if cols[all_species_idx] != "1" or cols[locality_type_idx] != "H":
                rows_skipped += 1
                continue

            # Extract only required columns
            outfile.write("\t".join(cols[i] for i in col_indices) + "\n")
            rows_processed += 1

            # Progress update every 1 million rows
            if rows_processed % 1_000_000 == 0:
                elapsed = time.time() - start_time
                rate = rows_processed / elapsed
                print(
                    f"  Processed {rows_processed:,} rows "
                    f"({format_duration(elapsed)}, {rate:,.0f} rows/sec)"
                )

    proc.wait()

    # Final stats
    elapsed = time.time() - start_time
    output_size = output_file.stat().st_size
    total_rows = rows_processed + rows_skipped

    print()
    print("=" * 50)
    print(f"Total rows read: {total_rows:,}")
    print(f"Rows written: {rows_processed:,}")
    print(f"Rows skipped (incomplete/non-hotspot): {rows_skipped:,}")
    print(f"Output size: {format_size(output_size)}")
    print(f"Total time: {format_duration(elapsed)}")
    print(f"\nOutput written to: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Extract required columns from gzipped eBird sampling file."
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Path to gzipped eBird sampling file (.txt.gz)",
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

    extract_sampling(args.input_file, args.output_file)


if __name__ == "__main__":
    main()
