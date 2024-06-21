#!/usr/bin/env python

from amlaidatatests.io import get_valid_region_codes
from amlaidatatests.schema.utils import get_table, get_table_name
from amlaidatatests.test_generators import non_nullable_fields
from amlaidatatests.schema.v1 import party_schema
from amlaidatatests.test_generators import entity_columns, get_entity_mutation_tests, get_entity_tests
from amlaidatatests.tests import common
from amlaidatatests.tests.base import AbstractColumnTest
import pytest
import ibis

TABLE = get_table("party")

def test_unique_combination_of_columns(connection):
    test = common.TestUniqueCombinationOfColumns(table=TABLE, unique_combination_of_columns=["party_id", "validity_start_time"])
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

@pytest.mark.parametrize("column,values", [
    ("type", ["COMPANY", "CONSUMER"]),
    ("civil_status_code", ['SINGLE', 'LEGALLY_DIVORCED', 'DIVORCED', 'WIDOW', 'STABLE_UNION', 'SEPARATED', 'UNKNOWN']),
    ("education_level_code", ['LESS_THAN_PRIMARY_EDUCATION', 'PRIMARY_EDUCATION', 'LOWER_SECONDARY_EDUCATION', 'UPPER_SECONDARY_EDUCATION', 'POST_SECONDARY_NON_TERTIARY_EDUCATION', 'SHORT_CYCLE_TERTIARY_EDUCATION', 'BACHELORS_OR_EQUIVALENT', 'MASTERS_OR_EQUIVALENT', 'DOCTORAL_OR_EQUIVALENT', 'NOT_ELSEWHERE_CLASSIFIED', 'UNKNOWN']),
    ("nationalities.region_code", get_valid_region_codes()),
    ("residencies.region_code", get_valid_region_codes())
])
def test_column_values(connection, column, values):
    test = common.TestColumnValues(values=values, table=TABLE, column=column)
    test(connection)

@pytest.mark.parametrize("column,expression", [
    ("birth_date", TABLE.type == "COMPANY"),
    ("gender", TABLE.type == "COMPANY"),
    ("establishment_date", TABLE.type == "CONSUMER"),
    ("occupation", TABLE.type == "CONSUMER"),
])
def test_null_if(connection, column, expression):
    test = common.TestNullIf(expression=expression, table=TABLE, column=column)
    test(connection)

if __name__ == "__main__":
    retcode = pytest.main()
