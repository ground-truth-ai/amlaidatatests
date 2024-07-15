#!/usr/bin/env python

import pytest

from amlaidatatests.base import AbstractTableTest
from amlaidatatests.schema.utils import resolve_table_config
from amlaidatatests.test_generators import get_generic_table_tests, non_nullable_fields
from amlaidatatests.tests import common

TABLE_CONFIG = resolve_table_config("risk_case_event")


@pytest.mark.parametrize(
    "test", get_generic_table_tests(table_config=TABLE_CONFIG, max_rows_factor=10e6)
)
def test_table(connection, test: AbstractTableTest):
    test(connection=connection)


def test_primary_keys(connection):
    test = common.PrimaryKeyColumnsTest(
        table_config=TABLE_CONFIG, unique_combination_of_columns=["risk_case_event_id"]
    )
    test(connection)


# For each column in the schema, check all columns are all present
@pytest.mark.parametrize("column", TABLE_CONFIG.table.schema().fields.keys())
def test_column_presence(connection, column: str):
    test = common.ColumnPresenceTest(table_config=TABLE_CONFIG, column=column)
    test(connection)


# For each column in the schema, check all columns are the correct type
@pytest.mark.parametrize("column", TABLE_CONFIG.table.schema().fields.keys())
def test_column_type(connection, column):
    test = common.ColumnTypeTest(table_config=TABLE_CONFIG, column=column)
    test(connection)


# Validate all fields marked in the schema as being non-nullable are non-nullable. This is in addition
# to the schema level tests, since it's not possible to enforce an embedded struct is non-nullable.


@pytest.mark.parametrize("column", non_nullable_fields(TABLE_CONFIG.table.schema()))
def test_non_nullable_fields(connection, column):
    test = common.FieldNeverNullTest(table_config=TABLE_CONFIG, column=column)
    test(connection)


@pytest.mark.parametrize(
    "test",
    [
        common.ColumnValuesTest(
            values=[
                "AML_SUSPICIOUS_ACTIVITY_START",
                "AML_SUSPICIOUS_ACTIVITY_END",
                "AML_PROCESS_START",
                "AML_PROCESS_END",
                "AML_ALERT_GOOGLE",
                "AML_ALERT_LEGACY",
                "AML_ALERT_ADHOC",
                "AML_ALERT_EXPLORATORY",
                "AML_SAR",
                "AML_EXTERNAL",
                "AML_EXIT",
            ],
            table_config=TABLE_CONFIG,
            column="type",
        )
    ],
)
def test_column_values(connection, test):
    test(connection)


def test_referential_integrity_party(connection):
    to_table_config = resolve_table_config("party")
    test = common.ReferentialIntegrityTest(
        table_config=TABLE_CONFIG, to_table_config=to_table_config, keys=["party_id"]
    )
    test(connection)


if __name__ == "__main__":
    retcode = pytest.main()
