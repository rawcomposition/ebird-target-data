# eBird Month Observations Builder

Builds a SQLite database of bird observation statistics from eBird Basic Dataset files.

## Requirements

- Python 3.8+
- DuckDB: `pip install duckdb`
- aria2c (for downloading): `brew install aria2`
- pigz (for fast decompression): `brew install pigz`

## Download eBird Data

Download the eBird Basic Dataset using aria2c for fast parallel downloading:

```bash
# Download the dataset (replace with current release)
caffeinate -dimsu aria2c -d ~/Downloads -c -x 2 -s 2 -j 1 --retry-wait=30 --max-tries=0 https://download.ebird.org/ebd/prepackaged/ebd_relDec-2025.tar
```

## Extract the archive

```bash
caffeinate -i tar -xf ~/Downloads/ebd_relDec-2025.tar -C ~/Downloads
```

## Extract required columns

Extract only the columns needed for processing. This streams from the gzipped file and creates a much smaller TSV:

```bash
caffeinate -dims python3 extract_columns.py ~/Downloads/ebd_relDec-2025.txt.gz ebd_filtered.tsv
```

## Build the database

```bash
caffeinate -dims python3 build_month_observations.py ebd_filtered.tsv ebird.db \
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
CREATE TABLE month_obs (
    location_id TEXT,
    month INTEGER,          -- 1-12
    scientific_name TEXT,
    obs INTEGER,            -- times species was seen
    samples INTEGER         -- total checklists at location/month
);
```

## Example Queries

### Best locations to find a Red-headed Woodpecker (year-round)

```sql
SELECT
    m.location_id,
    m.obs,
    m.samples,
    ROUND(100.0 * m.obs / m.samples, 1) AS chance_pct
FROM month_obs m
WHERE m.scientific_name = 'Melanerpes erythrocephalus'
  AND m.month = 3
  AND m.samples >= 5
ORDER BY chance_pct DESC;
```

### Best locations to find a Red-headed Woodpecker in March

```sql
SELECT
  m.location_id,
  m.obs,
  m.samples,
  ROUND(100.0 * m.obs / m.samples, 1) AS chance_pct
FROM month_obs m
WHERE m.scientific_name = 'Melanerpes erythrocephalus'
  AND m.month = 3
  AND m.samples >= 5
ORDER BY chance_pct DESC;
```

## Notes

- Only includes hotspot locations (`LOCALITY TYPE = H`)
- Only includes complete checklists (`ALL SPECIES REPORTED = 1`)
- Group checklists are deduplicated (multiple observers = 1 sampling)
- Only species-level taxa are included (`CATEGORY` = 'species' or 'issf')
