#!/usr/bin/env python3
"""
Interactive CLI for eBird Target Species Aggregator.

Downloads and processes eBird Basic Dataset to build a SQLite database
of bird observation statistics.

Usage:
    python cli.py
"""

import subprocess
import sys
from datetime import datetime
from pathlib import Path

try:
    from simple_term_menu import TerminalMenu
except ImportError:
    print("Error: 'simple-term-menu' library required.")
    print("Install with: python3 -m pip install simple-term-menu")
    sys.exit(1)

# Get script directory for relative paths
SCRIPT_DIR = Path(__file__).parent.resolve()
DATASETS_DIR = SCRIPT_DIR / "datasets"
OUTPUTS_DIR = SCRIPT_DIR / "output"


def load_env_file() -> dict:
    """Load environment variables from .env file."""
    env_vars = {}
    env_path = SCRIPT_DIR / ".env"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    env_vars[key.strip()] = value.strip().strip('"').strip("'")
    return env_vars


def get_month_options() -> list[tuple[str, str, str]]:
    """
    Get current and previous month options.
    Returns list of (display_name, month_abbrev, year) tuples.
    E.g., [("Jan 2026", "Jan", "2026"), ("Dec 2025", "Dec", "2025")]
    """
    now = datetime.now()
    current_month = now.strftime("%b")
    current_year = now.strftime("%Y")

    # Calculate previous month
    if now.month == 1:
        prev_month = "Dec"
        prev_year = str(now.year - 1)
    else:
        prev_date = now.replace(month=now.month - 1)
        prev_month = prev_date.strftime("%b")
        prev_year = prev_date.strftime("%Y")

    return [
        (f"{current_month} {current_year}", current_month, current_year),
        (f"{prev_month} {prev_year}", prev_month, prev_year),
    ]


def get_file_paths(month_abbrev: str, year: str) -> dict:
    """
    Get all file paths for a given month/year.
    E.g., month_abbrev="Jan", year="2026" gives:
    - tar: datasets/ebd-jan-2026.tar
    - txt_gz: datasets/ebd-jan-2026.txt.gz
    - filtered: datasets/ebd-jan-2026-filtered.tsv
    - db: outputs/targets-jan-2026.db
    - ebird_release: ebd_relJan-2026 (for download URL and tar extraction)
    """
    month_lower = month_abbrev.lower()
    base_name = f"ebd-{month_lower}-{year}"
    ebird_release = f"ebd_rel{month_abbrev}-{year}"

    return {
        "tar": DATASETS_DIR / f"{base_name}.tar",
        "txt_gz": DATASETS_DIR / f"{base_name}.txt.gz",
        "filtered": DATASETS_DIR / f"{base_name}-filtered.tsv",
        "db": OUTPUTS_DIR / f"targets-{month_lower}-{year}.db",
        "ebird_release": ebird_release,
        "download_url": f"https://download.ebird.org/ebd/prepackaged/{ebird_release}.tar",
    }


def print_header():
    """Print CLI header."""
    print()
    print("=" * 50)
    print("  eBird Target Species Aggregator")
    print("=" * 50)
    print()


def prompt_choice(prompt: str, options: list[str]) -> int:
    """
    Prompt user to select from a list of options using arrow keys.
    Returns the 0-based index of the selected option.
    """
    print(prompt)
    print()

    menu = TerminalMenu(
        options,
        menu_cursor="→ ",
        menu_cursor_style=("fg_cyan", "bold"),
        menu_highlight_style=("fg_cyan", "bold"),
    )

    idx = menu.show()

    if idx is None:
        print("\nExiting.")
        sys.exit(0)

    return idx


def run_download(paths: dict) -> bool:
    """
    Download the eBird Basic Dataset.
    Returns True if successful, False otherwise.
    """
    tar_file = paths["tar"]
    download_url = paths["download_url"]

    print("\n" + "-" * 50)
    print("Step: Download eBird Basic Dataset")
    print("-" * 50)
    sys.stdout.flush()

    if tar_file.exists():
        print(f"\nDataset already exists: {tar_file}")
        print("Skipping download. Delete the file to re-download.")
        return True

    print(f"\nDownloading: {download_url}")
    print(f"To: {tar_file}")
    print()
    sys.stdout.flush()

    # Ensure datasets directory exists
    DATASETS_DIR.mkdir(parents=True, exist_ok=True)

    try:
        # Use aria2c for fast, resumable downloads
        result = subprocess.run(
            [
                "caffeinate", "-dimsu",
                "aria2c",
                "-d", str(DATASETS_DIR),
                "-o", tar_file.name,
                "-c",  # Continue/resume download
                "-x", "2",  # Max connections per server
                "-s", "2",  # Split file into segments
                "-j", "1",  # Max concurrent downloads
                "--retry-wait=30",
                "--max-tries=0",  # Retry indefinitely
                download_url,
            ],
            check=True,
        )
        print("\nDownload complete!")
        return True
    except subprocess.CalledProcessError as e:
        # Clean up partial download file on failure
        if tar_file.exists():
            tar_file.unlink()
        print(f"\nDownload failed. The dataset may not be available yet.")
        print(f"eBird releases datasets around mid-month.")
        return False
    except FileNotFoundError:
        print("\nError: aria2c not found. Please install it with: brew install aria2")
        return False


def run_extract(paths: dict) -> bool:
    """
    Extract the tar archive.
    Returns True if successful, False otherwise.
    """
    tar_file = paths["tar"]
    txt_gz_file = paths["txt_gz"]
    ebird_release = paths["ebird_release"]

    print("\n" + "-" * 50)
    print("Step: Extract Archive")
    print("-" * 50)
    sys.stdout.flush()

    if txt_gz_file.exists():
        print(f"\nExtracted file already exists: {txt_gz_file}")
        print("Skipping extraction. Delete the file to re-extract.")
        return True

    if not tar_file.exists():
        print(f"\nError: Tar file not found: {tar_file}")
        print("Please run the download step first.")
        return False

    print(f"\nExtracting: {tar_file}")
    print(f"Target file: {ebird_release}.txt.gz")
    print()
    sys.stdout.flush()

    try:
        # Extract only the required file from the tar
        result = subprocess.run(
            [
                "caffeinate", "-i",
                "tar", "-xf", str(tar_file),
                "-C", str(DATASETS_DIR),
                f"{ebird_release}.txt.gz",
            ],
            check=True,
        )

        # Rename to our naming convention
        extracted_file = DATASETS_DIR / f"{ebird_release}.txt.gz"
        if extracted_file.exists():
            extracted_file.rename(txt_gz_file)
            print(f"Extracted and renamed to: {txt_gz_file}")
        else:
            print(f"Warning: Expected file not found: {extracted_file}")
            return False

        print("\nExtraction complete!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\nExtraction failed: {e}")
        return False


def run_filter(paths: dict) -> bool:
    """
    Filter the dataset to extract required columns.
    Returns True if successful, False otherwise.
    """
    txt_gz_file = paths["txt_gz"]
    filtered_file = paths["filtered"]

    print("\n" + "-" * 50)
    print("Step: Filter Dataset")
    print("-" * 50)
    sys.stdout.flush()

    if filtered_file.exists():
        print(f"\nFiltered file already exists: {filtered_file}")
        print("Skipping filtering. Delete the file to re-filter.")
        return True

    if not txt_gz_file.exists():
        print(f"\nError: Gzipped file not found: {txt_gz_file}")
        print("Please run the extract step first.")
        return False

    print(f"\nInput: {txt_gz_file}")
    print(f"Output: {filtered_file}")
    print()
    sys.stdout.flush()

    extract_script = SCRIPT_DIR / "extract_columns.py"

    try:
        result = subprocess.run(
            [
                "caffeinate", "-dims",
                "python3", str(extract_script),
                str(txt_gz_file),
                str(filtered_file),
            ],
            check=True,
        )
        print("\nFiltering complete!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\nFiltering failed: {e}")
        return False


def run_build_db(paths: dict, env_vars: dict, skip_hotspots: bool = False) -> bool:
    """
    Build the SQLite database.
    Returns True if successful, False otherwise.
    """
    filtered_file = paths["filtered"]
    db_file = paths["db"]

    print("\n" + "-" * 50)
    if skip_hotspots:
        print("Step: Build SQLite Database (skipping hotspots)")
    else:
        print("Step: Build SQLite Database")
    print("-" * 50)
    sys.stdout.flush()

    if not filtered_file.exists():
        print(f"\nError: Filtered file not found: {filtered_file}")
        print("Please run the filter step first.")
        return False

    # Ensure output directory exists
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\nInput: {filtered_file}")
    print(f"Output: {db_file}")

    # Get config from environment
    memory_limit = env_vars.get("MEMORY_LIMIT", "24")
    threads = env_vars.get("THREADS", "8")

    print(f"Memory limit: {memory_limit}GB")
    print(f"Threads: {threads}")
    if skip_hotspots:
        print("Skipping hotspots download")
    print()
    sys.stdout.flush()

    generate_script = SCRIPT_DIR / "generate_data.py"

    cmd = [
        "caffeinate", "-dims",
        "python3", str(generate_script),
        str(filtered_file),
        str(db_file),
        "--memory-limit", f"{memory_limit}GB",
        "--threads", threads,
    ]
    if skip_hotspots:
        cmd.append("--skip-hotspots")

    try:
        result = subprocess.run(cmd, check=True)
        print("\nDatabase build complete!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\nDatabase build failed: {e}")
        return False


def main():
    print_header()

    # Load environment variables
    env_vars = load_env_file()

    # Step 1: Choose dataset month
    month_options = get_month_options()
    month_display = [opt[0] for opt in month_options]

    month_idx = prompt_choice("Which dataset do you want to use?", month_display)
    selected_month = month_options[month_idx]
    month_abbrev = selected_month[1]
    year = selected_month[2]

    # Get file paths for selected month
    paths = get_file_paths(month_abbrev, year)

    # Step 2: Choose which operation to run
    operations = [
        "Download EBD Dataset",
        "Extract Archive",
        "Filter Dataset",
        "Build SQLite Database",
        "Build SQLite Database (skip hotspots)",
        "All (Run all steps)",
    ]

    print()
    op_idx = prompt_choice("Which step do you want to run?", operations)

    print()
    print("=" * 50)

    success = True

    if op_idx == 0:  # Download only
        success = run_download(paths)
    elif op_idx == 1:  # Extract only
        success = run_extract(paths)
    elif op_idx == 2:  # Filter only
        success = run_filter(paths)
    elif op_idx == 3:  # Build DB only
        success = run_build_db(paths, env_vars)
    elif op_idx == 4:  # Build DB only (skip hotspots)
        success = run_build_db(paths, env_vars, skip_hotspots=True)
    elif op_idx == 5:  # All steps
        print("\nRunning all steps...")

        if not run_download(paths):
            print("\nAborting: Download step failed.")
            sys.exit(1)

        if not run_extract(paths):
            print("\nAborting: Extract step failed.")
            sys.exit(1)

        if not run_filter(paths):
            print("\nAborting: Filter step failed.")
            sys.exit(1)

        if not run_build_db(paths, env_vars):
            print("\nAborting: Build database step failed.")
            sys.exit(1)

        success = True

    print()
    print("=" * 50)
    if success:
        print("Complete!")
    else:
        print("Failed!")
        sys.exit(1)
    print("=" * 50)


if __name__ == "__main__":
    main()
