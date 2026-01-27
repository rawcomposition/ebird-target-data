# eBird Target Species Aggregator

Builds a SQLite database of bird observation statistics from eBird Basic Dataset files. It allows querying for the best hotspots to find a given species, or the most likely species at a given hotspot.

## Requirements

- Python 3.8+
- DuckDB: `python3 -m pip install duckdb`
- Requests: `python3 -m pip install requests`
- aria2c (for downloading): `brew install aria2`
- pigz (for fast decompression): `brew install pigz`

## Setup

Create a `.env` file in the project directory with your eBird API key:

```
EBIRD_API_KEY=your_api_key_here
```

You can get an API key from https://ebird.org/api/keygen

## Quick Start

Run the all-in-one script to download and build the database:

```bash
./get-latest
```

This will:

1. Download the latest eBird Basic Dataset for the current month
2. Extract the archive
3. Extract required columns
4. Build the SQLite database

The script skips steps where output files already exist. Delete intermediate files to re-run those steps.

To specify a custom output database:

```bash
./get-latest my_ebird.db
```

## Manual Steps

If you need to run steps individually:

### Download eBird Data

Download the eBird Basic Dataset using aria2c for fast parallel downloading:

```bash
# Download the dataset (replace with current release)
caffeinate -dimsu aria2c -d ~/Downloads -c -x 2 -s 2 -j 1 --retry-wait=30 --max-tries=0 https://download.ebird.org/ebd/prepackaged/ebd_relDec-2025.tar
```

### Extract the archive

```bash
caffeinate -i tar -xf ~/Downloads/ebd_relDec-2025.tar -C ~/Downloads
```

### Extract required columns

Extract only the columns needed for processing. This streams from the gzipped file and creates a much smaller TSV:

```bash
caffeinate -dims python3 extract_columns.py ~/Downloads/ebd_relDec-2025.txt.gz ebd_filtered.tsv
```

### Build the database

```bash
caffeinate -dims python3 build_month_observations.py ebd_filtered.tsv ebird.db \
    --memory-limit 24GB \
    --threads 8
```

## Output Schema

```sql
-- Species taxonomy from eBird API
CREATE TABLE species (
    id INTEGER PRIMARY KEY,
    sci_name TEXT NOT NULL,      -- Scientific name
    name TEXT NOT NULL,          -- Common name
    code TEXT NOT NULL UNIQUE,   -- eBird species code
    taxon_order INTEGER NOT NULL -- Taxonomic order
);

-- Hotspot locations from eBird API
CREATE TABLE hotspots (
    id TEXT PRIMARY KEY,          -- eBird location ID (e.g., L1234567)
    name TEXT,
    country_code TEXT,
    subnational1_code TEXT,       -- State/province
    subnational2_code TEXT,       -- County
    lat REAL,
    lng REAL,
    latest_obs_date TEXT,
    num_species INTEGER,          -- All-time species count
    num_checklists INTEGER        -- All-time checklist count
);

-- Aggregated monthly observations
CREATE TABLE month_obs (
    location_id TEXT NOT NULL,
    month INTEGER NOT NULL,       -- 1-12
    species_id INTEGER NOT NULL,
    obs INTEGER NOT NULL,         -- Times species was seen
    samples INTEGER NOT NULL      -- Total checklists at location/month
);
```

## Example Queries

### Best locations to find a species (year-round)

```sql
SELECT
    m.location_id,
    SUM(m.obs) AS obs,
    SUM(m.samples) AS samples,
    ROUND(100.0 * (1.0*SUM(m.obs) / SUM(m.samples)), 1) AS chance_pct
FROM month_obs m
WHERE m.species_id = 1961
GROUP BY m.location_id
HAVING SUM(m.samples) >= 5
ORDER BY (1.0*SUM(m.obs) / SUM(m.samples)) DESC
LIMIT 200;
```

### Best locations to find a species in March

```sql
SELECT
    m.location_id,
    m.obs,
    m.samples,
    ROUND(100.0 * m.obs / m.samples, 1) AS chance_pct
FROM month_obs m
WHERE m.species_id = 3781
  AND m.month = 3
  AND m.samples >= 5
ORDER BY chance_pct DESC;
```

### Best locations to find a species in Canada (year-round)

```sql
SELECT
    m.location_id,
    SUM(m.obs) AS obs,
    SUM(m.samples) AS samples,
    ROUND(100.0 * (1.0*SUM(m.obs) / SUM(m.samples)), 1) AS chance_pct
FROM month_obs m
JOIN hotspots h ON h.id = m.location_id
WHERE m.species_id = 1961
    AND h.country_code = 'CA'
GROUP BY m.location_id
HAVING SUM(m.samples) >= 5
ORDER BY (1.0*SUM(m.obs) / SUM(m.samples)) DESC
LIMIT 200;
```

## Notes

- Only includes hotspot locations (`LOCALITY TYPE = H`)
- Only includes complete checklists (`ALL SPECIES REPORTED = 1`)
- Group checklists are deduplicated (multiple observers = 1 sampling)
- Only species-level taxa are included (`CATEGORY` = 'species' or 'issf')
- Hotspots are downloaded via eBird API with 5 second delays between countries
- Taxonomy is downloaded from eBird API (no API key required)
