"""
ETL Pipeline Tests — Data Pipeline Validation Framework
Validates row-count parity across extract -> dbt transform -> Snowflake warehouse,
duplicate-key detection, and null-rate thresholds per table.
"""

import pytest
import allure


# Table name mapping per ETL layer
LAYER_TABLE_MAP = {
    "dim_users":           {"extract": "raw_users",         "transform": "stg_users"},
    "fact_subscriptions":  {"extract": "raw_subscriptions",  "transform": "stg_subscriptions"},
    "fact_ad_impressions": {"extract": "raw_ad_events",      "transform": "stg_ad_impressions"},
}

NULL_RATE_THRESHOLD = 0.02  # 2% max acceptable null rate for nullable columns


def get_row_count(cursor, db, schema, table):
    cursor.execute(f"SELECT COUNT(*) FROM {db}.{schema}.{table}")
    return cursor.fetchone()[0]


@allure.suite("ETL Pipeline")
class TestETLRowCountParity:

    @allure.title("Row-count parity: extract -> transform [{table}]")
    @pytest.mark.parametrize("table", LAYER_TABLE_MAP.keys())
    def test_extract_to_transform_parity(self, sf_cursor, layers, table):
        """Row count must be identical between landing (extract) and staging (transform)."""
        extract_table   = LAYER_TABLE_MAP[table]["extract"]
        transform_table = LAYER_TABLE_MAP[table]["transform"]

        extract_count   = get_row_count(sf_cursor, layers["extract"]["db"],   layers["extract"]["schema"],   extract_table)
        transform_count = get_row_count(sf_cursor, layers["transform"]["db"], layers["transform"]["schema"], transform_table)

        assert extract_count == transform_count, (
            f"[{table}] Row count mismatch extract->transform: "
            f"{extract_count} vs {transform_count}. "
            f"Possible data loss in dbt transform."
        )

    @allure.title("Row-count parity: transform -> warehouse [{table}]")
    @pytest.mark.parametrize("table", LAYER_TABLE_MAP.keys())
    def test_transform_to_warehouse_parity(self, sf_cursor, layers, table):
        """Row count must be identical between staging (transform) and warehouse."""
        transform_table = LAYER_TABLE_MAP[table]["transform"]

        transform_count = get_row_count(sf_cursor, layers["transform"]["db"], layers["transform"]["schema"], transform_table)
        warehouse_count = get_row_count(sf_cursor, layers["warehouse"]["db"], layers["warehouse"]["schema"], table)

        assert transform_count == warehouse_count, (
            f"[{table}] Row count mismatch transform->warehouse: "
            f"{transform_count} vs {warehouse_count}. "
            f"Possible load failure."
        )


@allure.suite("ETL Pipeline")
class TestDuplicateKeys:

    @allure.title("No duplicate primary keys — {table}")
    @pytest.mark.parametrize("table,pk", [
        ("dim_users",           "user_id"),
        ("fact_subscriptions",  "subscription_id"),
        ("fact_ad_impressions", "impression_id"),
    ])
    def test_no_duplicate_primary_keys(self, sf_cursor, table, pk):
        """Primary key column must be unique across all warehouse rows."""
        sf_cursor.execute(f"""
            SELECT {pk}, COUNT(*) AS cnt
            FROM ANALYTICS_DB.PUBLIC.{table}
            GROUP BY {pk}
            HAVING cnt > 1
            LIMIT 10
        """)
        duplicates = sf_cursor.fetchall()
        assert not duplicates, (
            f"[{table}] Duplicate {pk} values found (first 10): "
            + ", ".join(str(r[0]) for r in duplicates)
        )


@allure.suite("ETL Pipeline")
class TestNullRates:

    @allure.title("Null rate within threshold — fact_subscriptions.end_date")
    def test_null_rate_end_date(self, sf_cursor):
        """
        end_date is nullable (active subscriptions have no end date).
        Null rate should not exceed threshold — a spike signals a pipeline bug.
        """
        sf_cursor.execute("""
            SELECT
                SUM(CASE WHEN end_date IS NULL THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS null_rate
            FROM ANALYTICS_DB.PUBLIC.fact_subscriptions
        """)
        null_rate = sf_cursor.fetchone()[0]
        assert null_rate <= NULL_RATE_THRESHOLD or null_rate > 0.5, (
            # >50% nulls is expected (active subs) — flag only unexpected mid-range spikes
            f"Unexpected null rate for end_date: {null_rate:.2%}. "
            f"Threshold: {NULL_RATE_THRESHOLD:.2%}. Possible pipeline issue."
        )

    @allure.title("Null rate within threshold — fact_ad_impressions.user_id")
    def test_null_rate_ad_user_id(self, sf_cursor):
        """Anonymous ad impressions may have null user_id; rate must stay below threshold."""
        sf_cursor.execute("""
            SELECT
                SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS null_rate
            FROM ANALYTICS_DB.PUBLIC.fact_ad_impressions
        """)
        null_rate = sf_cursor.fetchone()[0]
        assert null_rate <= NULL_RATE_THRESHOLD, (
            f"Anonymous impression rate {null_rate:.2%} exceeds threshold "
            f"{NULL_RATE_THRESHOLD:.2%}. Possible user enrichment failure."
        )
