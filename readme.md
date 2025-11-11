# Clinical Trials Pipeline

Databricks pipeline for ingesting and processing clinical trials data from ClinicalTrials.gov API.

## Quick Start

```bash
# Deploy the pipeline
databricks bundle deploy

# Run the pipeline
databricks bundle run clinical_trials_pipeline
```

## Configuration

Edit `databricks.yml` to configure:
- `catalog`: Target catalog (default: `pavan_naidu`)
- `schema`: Target schema (default: `clinical_trials`)
- `ct_condition`: Search condition (default: `diabetes`)
- `ct_study_type`: Study type (default: `INTERVENTIONAL`)
- `ct_status`: Recruitment status (default: `RECRUITING`)
- `ct_page_size`: Records per API call (default: `100`)
- `ct_max_pages`: Max pages to fetch - 0 = unlimited, >0 = limit (default: `0`)

## Architecture

- **Bronze**: Raw API data stored as VARIANT type
- **Silver**: Validated data with CDC (Change Data Capture)
- **Gold**: Curated analytics tables

## Clean Up

```bash
databricks bundle destroy
```
