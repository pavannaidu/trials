# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository demonstrates ingesting and processing semi-structured data from ClinicalTrials.gov API using Databricks VARIANT data type. The pipeline fetches clinical trial data via the ClinicalTrials.gov API v2 and processes it through a medallion architecture (Bronze → Silver → Gold) for analytics.

**Key Technology**: VARIANT data type with `parse_json` for flexible, schema-less semi-structured data storage with 8-30x faster reads compared to JSON strings.

## Key Commands

### Databricks Asset Bundle Commands

```bash
# Validate bundle configuration
databricks bundle validate

# Deploy the bundle (uploads notebook and creates pipeline)
databricks bundle deploy

# Deploy with custom variables
databricks bundle deploy --var catalog=my_catalog --var schema=my_schema

# Deploy with custom Clinical Trials parameters
databricks bundle deploy --var ct_condition=cancer --var ct_study_type=OBSERVATIONAL --var ct_max_pages=10

# Run the pipeline
databricks bundle run clinical_trials_pipeline

# Destroy bundle (deletes pipeline configuration, not tables)
databricks bundle destroy
```

## Architecture

### Directory Structure

```
clinical_trials/
├── databricks.yml                      # Bundle configuration with variables
├── resources/
│   └── clinical_trials_pipeline.yml    # Pipeline definition
└── src/
    └── ClinicalTrails/
        └── pipeline.ipynb              # API ingestion with VARIANT type
```

### Pipeline Architecture (Medallion Pattern)

**Bronze Layer**: Raw API data stored as VARIANT
- Fetches data from `https://clinicaltrials.gov/api/v2/studies`
- Supports pagination with `nextPageToken`
- Stores raw JSON response as VARIANT type using `parse_json(col("raw_data"))`

**Silver Layer**: Validated data with extracted fields
- Extracts key fields using colon notation (e.g., `variant_data:protocolSection.identificationModule.nctId`)
- Applies data quality checks with `@dp.expect_or_drop()`
- Flattens nested structures for easier querying

**Gold Layer**: Business-ready aggregations
- `study_overview`: Summary statistics and high-level metrics
- `phase_analysis`: Study phase distribution and trends
- `sponsor_analytics`: Sponsor and collaborator analysis
- `timeline_trends`: Temporal analysis of study dates

## Configuration System

The `databricks.yml` file defines configurable variables:

**Core Variables:**
- `catalog`: Target catalog (default: `pavan_naidu`)
- `schema`: Target schema (default: `json`)
- `volume`: Volume for data storage (default: `raw_data`)
- `workspace`: Databricks workspace URL

**Clinical Trials Parameters** (configurable via bundle or webapp):
- `ct_condition`: Medical condition to search (default: `diabetes`)
- `ct_study_type`: Study type - INTERVENTIONAL, OBSERVATIONAL, etc. (default: `INTERVENTIONAL`)
- `ct_status`: Recruitment status - RECRUITING, COMPLETED, etc. (default: `RECRUITING`)
- `ct_page_size`: Records per API call (default: `100`)
- `ct_max_pages`: Maximum pages to fetch (default: `5`)

**Accessing Configuration in Notebooks:**
```python
catalog = spark.conf.get("catalog", "default_catalog")
condition = spark.conf.get("pipeline.condition", "diabetes")
```

## VARIANT Type Usage

VARIANT provides 30x faster reads (with shredding) compared to JSON strings:
- **No schema required**: Store any JSON structure without defining schema upfront
- **Flexible field access**: Use colon notation to access nested fields
  - `col("variant_data:field.subfield")` - nested object access
  - `col("variant_data:array[0]")` - array element access
- **Performance optimization**: Requires Databricks Runtime 17.2+ for shredding
- **Enable shredding**: Set table property `"delta.feature.variantType-preview": "supported"`

### When to Use VARIANT
- Schema changes frequently or is unknown upfront
- Maximum flexibility needed for semi-structured data
- Type mismatches shouldn't cause pipeline failures
- Working with external APIs where schema may evolve

## Important Implementation Details

### Spark Declarative Pipelines (SDP)
- Use `@dp.table()` decorator with table properties for metadata
- Use `@dp.expect_or_drop()` for data quality constraints
- Configuration parameters accessed via `spark.conf.get()` from bundle configuration
- Read streaming sources with `spark.readStream` in bronze layer
- Use `dp.read()` for reading DLT tables in downstream layers

### Clinical Trials API Specifics
- API endpoint: `https://clinicaltrials.gov/api/v2/studies`
- Pagination: Use `nextPageToken` from response for subsequent requests
- Parameters: All search parameters configurable via bundle variables
- Rate limiting: Consider API rate limits when setting `ct_page_size` and `ct_max_pages`
- Response structure: Studies array contains individual trial records

## Required Databricks Resources

Before deploying, ensure these exist:
- **Catalog**: Specified in `databricks.yml` variables (default: `pavan_naidu`)
- **Schema**: Within the catalog (default: `json`)
- **Volume**: For raw data storage if needed (default: `raw_data`)

The pipeline will create tables in the specified `catalog.schema`:
- `clinical_trials_bronze`
- `clinical_trials_silver`
- `study_overview` (gold)
- `phase_analysis` (gold)
- `sponsor_analytics` (gold)
- `timeline_trends` (gold)

## Development Workflow

The project uses **Databricks Connect** for local development with remote cluster execution:
1. Develop notebooks interactively in Databricks workspace
2. Test API parameters and query logic
3. Deploy via Asset Bundles for production pipelines
4. Monitor pipeline execution via Databricks workspace UI

## References

- [Variant Data Type Introduction](https://www.databricks.com/blog/introducing-open-variant-data-type-delta-lake-and-apache-spark)
- [Variant as Open Standard in Apache Parquet](https://www.databricks.com/blog/introducing-variant-new-open-standard-semi-structured-data-apache-parquettm-delta-lake)
- [Variant Shredding Documentation](https://docs.databricks.com/aws/en/delta/variant-shredding)
- [ClinicalTrials.gov API v2 Documentation](https://clinicaltrials.gov/data-api/about-api)
