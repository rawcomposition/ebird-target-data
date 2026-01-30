# eBird Target Species Aggregator

Builds a SQLite database of bird observation statistics from eBird Basic Dataset files. It allows querying for the best hotspots to find a given species, or the most likely species at a given hotspot.

## Requirements

- Python 3.8+
- aria2c (for downloading): `brew install aria2`
- pigz (for fast decompression): `brew install pigz`

## Setup

Create a virtual environment and install Python dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Copy the example environment file and add your eBird API key (get one at https://ebird.org/api/keygen):

```bash
cp .env.example .env
```

Optional: Add an [ntfy.sh](https://ntfy.sh) topic to `NTFY_NOTIFICATION_TOPIC` to receive notifications when CLI operations complete.

## Usage

Activate the virtual environment and run the interactive CLI:

```bash
source venv/bin/activate
python cli.py
```

The CLI will prompt you to:

1. Choose which dataset to use (current or previous month)
2. Choose which step to run:
   - **Download EBD Dataset** - Download the eBird Basic Dataset
   - **Extract Archive** - Extract the gzipped data file from the tar
   - **Filter Dataset** - Extract required columns and filter to hotspots/complete checklists
   - **Build SQLite Database** - Generate the final SQLite database
   - **Build SQLite Database (skip hotspots)** - Generate the database without downloading hotspots from the eBird API
   - **All** - Run all steps in sequence

Each step skips automatically if its output file already exists. Delete the file to re-run that step.

Files are stored in:

- `datasets/` - Downloaded and intermediate data files
- `output/` - Final SQLite databases

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
    region_code TEXT,             -- Most specific valid region code
    lat REAL,
    lng REAL,
    num_species INTEGER,          -- All-time species count
    num_checklists INTEGER        -- All-time checklist count
);

-- Aggregated monthly observations
CREATE TABLE month_obs (
    location_id TEXT NOT NULL,
    month INTEGER NOT NULL,       -- 1-12
    species_id INTEGER NOT NULL,
    obs INTEGER NOT NULL,         -- Times species was seen
    samples INTEGER NOT NULL,     -- Total checklists at location/month
    score REAL NOT NULL           -- Wilson score lower bound for ranking
);

-- Aggregated yearly observations (for faster year-round queries)
CREATE TABLE year_obs (
    location_id TEXT NOT NULL,
    species_id INTEGER NOT NULL,
    obs INTEGER NOT NULL,         -- Times species was seen (all months)
    samples INTEGER NOT NULL,     -- Total checklists at location (all months)
    score REAL NOT NULL           -- Wilson score lower bound for ranking
);
```

## Example Queries

### Best locations to find a species (year-round)

```sql
SELECT
    y.location_id,
    y.obs,
    y.samples,
    ROUND(100.0 * y.obs / y.samples, 1) AS chance_pct
FROM year_obs y
WHERE y.species_id = 1961
  AND y.samples >= 5
ORDER BY chance_pct DESC
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
    y.location_id,
    y.obs,
    y.samples,
    ROUND(100.0 * y.obs / y.samples, 1) AS chance_pct
FROM year_obs y
JOIN hotspots h ON h.id = y.location_id
WHERE y.species_id = 1961
  AND h.country_code = 'CA'
  AND y.samples >= 5
ORDER BY chance_pct DESC
LIMIT 200;
```

## Notes

- Only includes hotspot locations (`LOCALITY TYPE = H`)
- Only includes complete checklists (`ALL SPECIES REPORTED = 1`)
- Group checklists are deduplicated (multiple observers = 1 sampling)
- Only species-level taxa are included (`CATEGORY` = 'species' or 'issf')
- Only location/species combinations with at least 2 observations are included
- Hotspots are downloaded via eBird API with 5 second delays between countries
- Taxonomy is downloaded from eBird API
- The `region_code` column contains the most specific valid region: subnational2 > subnational1 > country (some eBird subnational1 codes are invalid like "CO-", so these fall back to country)
- The `score` column uses the Wilson score lower bound formula:
  ```
  (obs + z²/2 - z * sqrt(obs * (samples - obs) / samples + z²/4)) / (samples + z²)
  ```
  The z-index is configurable via `WILSON_SCORE_Z_INDEX` in `.env` (default: 1.96 for 95% confidence). This balances frequency (obs/samples) with sample size, preventing locations with few checklists from ranking too high.
