# eBird Month Observations Builder

Builds a SQLite database of bird observation statistics from eBird Basic Dataset files.

## Requirements

- Python 3.8+
- DuckDB: `pip install duckdb`

## Usage

```bash
python3 build_month_observations.py <species_file.tsv> <sampling_file.tsv> <output.db>
```

For large datasets (100+ GB), use memory and temp directory options:

```bash
python3 build_month_observations.py species.tsv sampling.tsv ebird.db \
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
