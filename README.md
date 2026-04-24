# Data Pipeline Validation Framework

Automated test suite for validating ETL data integrity across a 3-stage Snowflake pipeline:
**Extract (RAW_DB)** → **dbt Transform (STAGING_DB)** → **Snowflake Warehouse (ANALYTICS_DB)**

## What it tests

| Test Suite | What it catches |
|---|---|
| Schema Contracts | Column drift, type mismatches, NOT NULL violations |
| Row-Count Parity | Data loss between extract → transform → warehouse |
| Duplicate Keys | Primary key violations post-load |
| Null Rate Thresholds | Unexpected nulls signalling enrichment failures |

## Stack
- **pytest** + **snowflake-connector-python** + **dbt-snowflake**
- **Allure** for HTML data-quality reports
- **Jenkins** nightly cron trigger (2 AM)

## Setup

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in your Snowflake credentials
```

## Run locally

```bash
pytest tests/ -v --alluredir=allure-results
allure serve allure-results
```

## CI
Runs nightly via `Jenkinsfile`. Failures trigger email to the data engineering team.
