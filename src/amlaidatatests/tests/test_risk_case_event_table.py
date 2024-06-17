#!/usr/bin/env python

from amlaidatatests.io import get_valid_region_codes
from amlaidatatests.schema.utils import get_table, get_table_name
from amlaidatatests.test_generators import non_nullable_fields
from amlaidatatests.schema.v1 import party_schema
from amlaidatatests.test_generators import entity_columns, get_entity_mutation_tests, get_entity_tests
from amlaidatatests.tests import common_tests
from amlaidatatests.tests.base import AbstractColumnTest
import pytest
import ibis

TABLE = get_table("risk_case_event")

def test_unique_combination_of_columns(connection):
    test = common_tests.TestUniqueCombinationOfColumns(table=TABLE, unique_combination_of_columns=["risk_case_event_id"])
    test(connection)

@pytest.mark.parametrize("test", get_entity_mutation_tests(table=TABLE, primary_keys=["party_id"]))
def test_entity_mutation_tests(connection, test: AbstractColumnTest):
    test(connection=connection)

@pytest.mark.parametrize("column", entity_columns(schema=TABLE.schema(), entity_types=["CurrencyValue"]))
@pytest.mark.parametrize("test", get_entity_tests(table=TABLE, entity_name="CurrencyValue"))
def test_currency_value_entity(connection, column, test: AbstractColumnTest):
    test(connection=connection, prefix=column)

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

@pytest.mark.parametrize("column,values", [
    ("type", ['AML_SUSPICIOUS_ACTIVITY_START', 'AML_SUSPICIOUS_ACTIVITY_END','AML_PROCESS_START','AML_PROCESS_END','AML_ALERT_GOOGLE','AML_ALERT_LEGACY','AML_ALERT_ADHOC','AML_ALERT_EXPLORATORY','AML_SAR','AML_EXTERNAL','AML_EXIT'])
])
def test_column_values(connection, column, values):
    test = common_tests.TestColumnValues(values=values, table=TABLE, column=column)
    test(connection)

@pytest.mark.parametrize("to_table,keys", [["party", (["party_id"])]] )
def test_referential_integrity(connection, to_table: str, keys: list[str]):
    to_table_obj = get_table(to_table)
    test = common_tests.TestReferentialIntegrity(table=TABLE, to_table=to_table_obj, keys=keys)
    test(connection)


if __name__ == "__main__":
    retcode = pytest.main()
