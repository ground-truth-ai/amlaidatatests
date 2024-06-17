#!/usr/bin/env python

from amlaidatatests.io import get_valid_region_codes
from amlaidatatests.schema.utils import get_table
from amlaidatatests.test_generators import non_nullable_fields
from amlaidatatests.test_generators import entity_columns, get_entity_mutation_tests, get_entity_tests
from amlaidatatests.tests import common_tests
from amlaidatatests.tests.base import AbstractColumnTest
import pytest

TABLE = get_table("party_supplementary_data")

def test_unique_combination_of_columns(connection):
    test = common_tests.TestUniqueCombinationOfColumns(table=TABLE, unique_combination_of_columns=["party_supplementary_data_id", "party_id", "validity_start_time"])
    test(connection)

@pytest.mark.parametrize("test", get_entity_mutation_tests(table=TABLE, primary_keys=["party_id"]))
def test_entity_mutation_tests(connection, test: AbstractColumnTest):
    test(connection=connection)

# For each column in the schema, check all columns are all present
@pytest.mark.parametrize("column", TABLE.schema().fields.keys())
def test_column_presence(connection, column: str):
    test = common_tests.TestColumnPresence(table=TABLE, column=column)
    test(connection)

# For each column in the schema, check all columns are the correct type
@pytest.mark.parametrize("column", TABLE.schema().fields.keys())
def test_column_type(connection, column):
    test = common_tests.TestColumnType(table=TABLE, column=column)
    test(connection)

# Validate all fields marked in the schema as being non-nullable are non-nullable. This is in addition
# to the schema level tests, since it's not possible to enforce an embedded struct is non-nullable.

@pytest.mark.parametrize("column", non_nullable_fields(TABLE.schema()))
def test_non_nullable_fields(connection, column):
    test = common_tests.TestFieldNeverNull(table=TABLE, column=column)
    test(connection)

@pytest.mark.parametrize("to_table,keys", [["party", (["party_id"])]] )
def test_referential_integrity(connection, to_table: str, keys: list[str]):
    to_table_obj = get_table(to_table)
    test = common_tests.TestReferentialIntegrity(table=TABLE, to_table=to_table_obj, keys=keys)
    test(connection)

if __name__ == "__main__":
    retcode = pytest.main()
