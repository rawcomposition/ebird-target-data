# eBird Month Observations Builder

Builds a SQLite database of bird observation statistics from eBird Basic Dataset files.

## Requirements

- Python 3.8+
- DuckDB: `pip install duckdb`
- aria2c (for downloading): `brew install aria2`
- pigz (for decompressing): `brew install pigz`

## Download eBird Data

Download the eBird Basic Dataset using aria2c for fast parallel downloading:

```bash
# Download the dataset (replace with current release)
caffeinate -dimsu aria2c -d ~/Downloads -c -x 2 -s 2 -j 1 --retry-wait=30 --max-tries=0 https://download.ebird.org/ebd/prepackaged/ebd_relDec-2025.tar
```

## Extract the archive

1. run `caffeinate -i tar -xf ~/Downloads/ebd_relDec-2025.tar -C ~/Downloads`
2. Run `pigz -dk ~/Downloads/ebd_relDec-2025.txt.gz`

The `-x 2 -s 2 -j 1` flags enable 2 parallel connections for faster downloads.

After extraction, you only need the main species file:

- `ebd_relDec-2025.txt` - Species observations (~200GB)

## Usage

```bash
python3 build_month_observations.py <species_file> <output.db>
```

For large datasets (100+ GB), use memory and thread options:

```bash
python3 build_month_observations.py ebd_relDec-2025.txt ebird.db \
    --memory-limit 24GB \
    --threads 8
```

## Output Schema

```sql
-- Hotspot locations from the dataset
CREATE TABLE location (
    location_id TEXT PRIMARY KEY,
    name TEXT,
    latitude REAL,
    longitude REAL
);

-- Aggregated monthly observations
CREATE TABLE month_observations (
    location_id TEXT,
    month INTEGER,          -- 1-12
    scientific_name TEXT,
    observations INTEGER,   -- times species was seen
    samplings INTEGER       -- total checklists at location/month
);
```

## Example Query

```sql
-- Best locations to find a Red-headed Woodpecker in March
SELECT
    l.name,
    m.observations,
    m.samplings,
    ROUND(100.0 * m.observations / m.samplings, 1) AS chance_pct
FROM month_observations m
JOIN location l ON m.location_id = l.location_id
WHERE m.scientific_name = 'Melanerpes erythrocephalus'
  AND m.month = 3
  AND m.samplings >= 5
ORDER BY chance_pct DESC;
```

## Notes

- Only includes hotspot locations (`LOCALITY TYPE = H`)
- Only includes complete checklists (`ALL SPECIES REPORTED = 1`)
- Group checklists are deduplicated (multiple observers = 1 sampling)
- Only species-level taxa are included (`CATEGORY` = 'species' or 'issf')
