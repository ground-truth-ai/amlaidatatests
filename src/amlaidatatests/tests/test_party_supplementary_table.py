#!/usr/bin/env python

from amlaidatatests.schema.utils import get_unbound_table
from amlaidatatests.test_generators import (
    get_entity_mutation_tests,
    get_generic_table_tests,
    non_nullable_fields,
)
from amlaidatatests.tests import common
from amlaidatatests.base import AbstractColumnTest, AbstractTableTest, AMLAITestSeverity
import pytest


TABLE = get_unbound_table("party_supplementary_data")


# Don't error or warn on an empty party_supplementary_data table
@pytest.mark.parametrize(
    "test", get_generic_table_tests(table=TABLE, max_rows_factor=1e12)
)
def test_table(connection, test: AbstractTableTest):
    test(connection=connection)


def test_primary_keys(connection):
    test = common.TestPrimaryKeyColumns(
        table=TABLE,
        unique_combination_of_columns=[
            "party_supplementary_data_id",
            "party_id",
            "validity_start_time",
        ],
    )
    test(connection)


@pytest.mark.parametrize(
    "test", get_entity_mutation_tests(table=TABLE, primary_keys=["party_id"])
)
def test_entity_mutation_tests(connection, test: AbstractColumnTest):
    test(connection=connection)


# For each column in the schema, check all columns are all present
@pytest.mark.parametrize("column", TABLE.schema().fields.keys())
def test_column_presence(connection, column: str):
    test = common.TestColumnPresence(table=TABLE, column=column)
    test(connection)


# For each column in the schema, check all columns are the correct type
@pytest.mark.parametrize("column", TABLE.schema().fields.keys())
def test_column_type(connection, column):
    test = common.TestColumnType(table=TABLE, column=column)
    test(connection)


# Validate all fields marked in the schema as being non-nullable are non-nullable. This is in addition
# to the schema level tests, since it's not possible to enforce an embedded struct is non-nullable.


@pytest.mark.parametrize("column", non_nullable_fields(TABLE.schema()))
def test_non_nullable_fields(connection, column):
    test = common.TestFieldNeverNull(table=TABLE, column=column)
    test(connection)


@pytest.mark.parametrize("to_table,keys", [["party", (["party_id"])]])
def test_referential_integrity(connection, to_table: str, keys: list[str]):
    to_table_obj = get_unbound_table(to_table)
    test = common.TestReferentialIntegrity(
        table=TABLE, to_table=to_table_obj, keys=keys
    )
    test(connection)


if __name__ == "__main__":
    retcode = pytest.main()
