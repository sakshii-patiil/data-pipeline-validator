"""
Schema Contract Tests — Data Pipeline Validation Framework
Asserts column names, data types, NOT NULL constraints, and referential
integrity for every table defined in the table_registry fixture.
"""

import pytest
import allure


@allure.suite("Schema Contracts")
class TestSchemaContracts:

    @allure.title("Column presence — {table}")
    @pytest.mark.parametrize("table", ["dim_users", "fact_subscriptions", "fact_ad_impressions"])
    def test_expected_columns_exist(self, sf_cursor, table_registry, table):
        """Every expected column must exist in the warehouse table."""
        expected_cols = set(table_registry[table]["columns"].keys())
        sf_cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM ANALYTICS_DB.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = UPPER('{table}')
        """)
        actual_cols = {row[0].lower() for row in sf_cursor.fetchall()}
        missing = expected_cols - actual_cols
        assert not missing, (
            f"[{table}] Missing columns: {missing}. "
            f"Found: {actual_cols}"
        )

    @allure.title("Data types — {table}")
    @pytest.mark.parametrize("table", ["dim_users", "fact_subscriptions", "fact_ad_impressions"])
    def test_column_data_types(self, sf_cursor, table_registry, table):
        """Each column must match its declared data type."""
        contract = table_registry[table]["columns"]
        sf_cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM ANALYTICS_DB.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = UPPER('{table}')
        """)
        actual_types = {row[0].lower(): row[1].upper() for row in sf_cursor.fetchall()}
        mismatches = []
        for col, spec in contract.items():
            expected_type = spec["type"].upper()
            actual_type   = actual_types.get(col, "MISSING").upper()
            if expected_type not in actual_type:  # covers VARCHAR(255) matching VARCHAR
                mismatches.append(
                    f"{col}: expected {expected_type}, got {actual_type}"
                )
        assert not mismatches, f"[{table}] Type mismatches:\n" + "\n".join(mismatches)

    @allure.title("NOT NULL constraints — {table}")
    @pytest.mark.parametrize("table", ["dim_users", "fact_subscriptions", "fact_ad_impressions"])
    def test_not_null_constraints(self, sf_cursor, table_registry, table):
        """Non-nullable columns must contain zero NULL values."""
        contract = table_registry[table]["columns"]
        non_nullable = [col for col, spec in contract.items() if not spec["nullable"]]
        violations = []
        for col in non_nullable:
            sf_cursor.execute(f"""
                SELECT COUNT(*) FROM ANALYTICS_DB.PUBLIC.{table}
                WHERE {col} IS NULL
            """)
            null_count = sf_cursor.fetchone()[0]
            if null_count > 0:
                violations.append(f"{col}: {null_count} NULL rows")
        assert not violations, (
            f"[{table}] NOT NULL violations:\n" + "\n".join(violations)
        )

    @allure.title("Referential integrity — fact_subscriptions.user_id -> dim_users")
    def test_referential_integrity_subscriptions_users(self, sf_cursor):
        """Every user_id in fact_subscriptions must exist in dim_users."""
        sf_cursor.execute("""
            SELECT COUNT(*) FROM ANALYTICS_DB.PUBLIC.fact_subscriptions s
            LEFT JOIN ANALYTICS_DB.PUBLIC.dim_users u ON s.user_id = u.user_id
            WHERE u.user_id IS NULL
        """)
        orphan_count = sf_cursor.fetchone()[0]
        assert orphan_count == 0, (
            f"Referential integrity violation: {orphan_count} subscriptions "
            f"reference non-existent user_ids."
        )
