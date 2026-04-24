"""
Fixtures for Data Pipeline Validation Framework.
Manages Snowflake connections for source, staging, and warehouse layers.
"""

import pytest
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture(scope="session")
def sf_conn():
    """Single Snowflake connection reused across the full test session."""
    conn = snowflake.connector.connect(
        user=os.getenv("SF_USER"),
        password=os.getenv("SF_PASSWORD"),
        account=os.getenv("SF_ACCOUNT"),
        warehouse=os.getenv("SF_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SF_DATABASE", "ANALYTICS_DB"),
        schema=os.getenv("SF_SCHEMA", "PUBLIC"),
        role=os.getenv("SF_ROLE", "SYSADMIN"),
    )
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def sf_cursor(sf_conn):
    cur = sf_conn.cursor()
    yield cur
    cur.close()


@pytest.fixture(scope="session")
def layers():
    """Three ETL layer definitions for row-count parity checks."""
    return {
        "extract":   {"db": "RAW_DB",       "schema": "LANDING"},
        "transform": {"db": "STAGING_DB",   "schema": "STAGING"},
        "warehouse": {"db": "ANALYTICS_DB", "schema": "PUBLIC"},
    }


@pytest.fixture(scope="session")
def table_registry():
    """Expected schema contracts per warehouse table."""
    return {
        "dim_users": {
            "columns": {
                "user_id":    {"type": "NUMBER",        "nullable": False},
                "email":      {"type": "VARCHAR",       "nullable": False},
                "plan_type":  {"type": "VARCHAR",       "nullable": False},
                "created_at": {"type": "TIMESTAMP_NTZ", "nullable": False},
                "updated_at": {"type": "TIMESTAMP_NTZ", "nullable": True},
            },
            "primary_key": "user_id",
        },
        "fact_subscriptions": {
            "columns": {
                "subscription_id": {"type": "NUMBER",  "nullable": False},
                "user_id":         {"type": "NUMBER",  "nullable": False},
                "plan_id":         {"type": "NUMBER",  "nullable": False},
                "start_date":      {"type": "DATE",    "nullable": False},
                "end_date":        {"type": "DATE",    "nullable": True},
                "amount_usd":      {"type": "FLOAT",   "nullable": False},
                "status":          {"type": "VARCHAR", "nullable": False},
            },
            "primary_key": "subscription_id",
        },
        "fact_ad_impressions": {
            "columns": {
                "impression_id": {"type": "VARCHAR",       "nullable": False},
                "user_id":       {"type": "NUMBER",        "nullable": True},
                "ad_id":         {"type": "VARCHAR",       "nullable": False},
                "device_type":   {"type": "VARCHAR",       "nullable": False},
                "impression_ts": {"type": "TIMESTAMP_NTZ", "nullable": False},
                "click_flag":    {"type": "BOOLEAN",       "nullable": False},
            },
            "primary_key": "impression_id",
        },
    }
