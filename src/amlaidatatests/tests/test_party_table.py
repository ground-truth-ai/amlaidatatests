#!/usr/bin/env python

from typing import Callable, Tuple
from amlaidatatests.io import get_valid_region_codes
from amlaidatatests.io import get_table
from amlaidatatests.schema.v1.common import EntityTest, entity_columns, get_entity_mutation_tests, get_entity_tests, non_nullable_fields
from amlaidatatests.schema.v1 import party_schema
from amlaidatatests.tests.fixtures import common_tests
import pytest

SUFFIX = "1234"
SCHEMA = party_schema

variables = {
    "suffix": SUFFIX
}

table = get_table("party_1234")

def test_unique_combination_of_columns():
    test = common_tests.TestUniqueCombinationOfColumns(unique_combination_of_columns=["party_id", "validity_start_time"])
    test.test(table=table)

@pytest.mark.parametrize("test", get_entity_mutation_tests(primary_keys=["party_id"]))
def test_entity_mutation_tests(test: EntityTest):
    f = test.test.test
    f(table=table)

@pytest.mark.parametrize("column", entity_columns(schema=SCHEMA, entity_types=["CurrencyValue"]))
@pytest.mark.parametrize("test", get_entity_tests(entity_name="CurrencyValue"))
def test_currency_value_entity(column, test: EntityTest):
    # Todo: tidy this up a bit
    f = test.test.test
    subfield = f"{column}.{test.test_field}"
    f(column=subfield, table=table)

# For each column in the schema, check all columns are all present
@pytest.mark.parametrize("column", SCHEMA.fields.keys())
def test_column_presence(column: str):
    test = common_tests.TestColumnPresence()
    test.test(table=table, column=column)

# For each column in the schema, check all columns are the correct type
@pytest.mark.parametrize("column", SCHEMA.fields.keys())
def test_column_type(column):
    test = common_tests.TestColumnType(SCHEMA)
    test.test(column=column, table=table)

# Validate all fields marked in the schema as being non-nullable are non-nullable. This is in addition
# to the schema level tests, since it's not possible to enforce an embedded struct is non-nullable.

@pytest.mark.parametrize("column", non_nullable_fields(SCHEMA))
def test_non_nullable_fields(column):
    test = common_tests.TestFieldNeverNull()
    test.test(column=column, table=table)

@pytest.mark.parametrize("column,values", [
    ("type", ["COMPANY", "CONSUMER"]),
    ("civil_status_code", ['SINGLE', 'LEGALLY_DIVORCED', 'DIVORCED', 'WIDOW', 'STABLE_UNION', 'SEPARATED', 'UNKNOWN']),
    ("education_level_code", ['LESS_THAN_PRIMARY_EDUCATION', 'PRIMARY_EDUCATION', 'LOWER_SECONDARY_EDUCATION', 'UPPER_SECONDARY_EDUCATION', 'POST_SECONDARY_NON_TERTIARY_EDUCATION', 'SHORT_CYCLE_TERTIARY_EDUCATION', 'BACHELORS_OR_EQUIVALENT', 'MASTERS_OR_EQUIVALENT', 'DOCTORAL_OR_EQUIVALENT', 'NOT_ELSEWHERE_CLASSIFIED', 'UNKNOWN']),
    ("nationalities.region_code", get_valid_region_codes()),
    ("residencies.region_code", get_valid_region_codes())
])
def test_column_values(column, values):
    test = common_tests.TestColumnValues(values=values)
    test.test(column=column, table=table)

@pytest.mark.parametrize("column,expression", [
    ("birth_date", table.type == "COMPANY"),
    ("gender", table.type == "COMPANY"),
    ("establishment_date", table.type == "CONSUMER"),
    ("occupation", table.type == "CONSUMER"),
])
def test_null_if(column, expression):
    test = common_tests.TestNullIf(expression)
    test.test(table=table, column=column)

if __name__ == "__main__":
    retcode = pytest.main()
