#!/usr/bin/env python

from amlaidatatests.io import get_valid_region_codes
from amlaidatatests.schema.utils import get_unbound_table
from amlaidatatests.test_generators import get_generic_table_tests, non_nullable_field_tests
from amlaidatatests.test_generators import entity_columns, get_entity_mutation_tests, get_entity_tests
from amlaidatatests.tests import common
from amlaidatatests.tests.base import AbstractColumnTest, AbstractTableTest, TestSeverity
import pytest

TABLE = get_unbound_table("party")

@pytest.mark.parametrize("test", get_generic_table_tests(table=TABLE, max_rows_factor=50e9, severity=TestSeverity.INFO))
def test_table(connection, test: AbstractTableTest):
    test(connection=connection)

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

@pytest.mark.parametrize("test", non_nullable_field_tests(TABLE))
def test_non_nullable_fields(connection, test: AbstractColumnTest):
    test(connection)

@pytest.mark.parametrize("test", [
    common.TestColumnValues(column="type", values=["COMPANY", "CONSUMER"], table=TABLE),
    common.TestColumnValues(column="civil_status_code", values=['SINGLE', 'LEGALLY_DIVORCED', 'DIVORCED', 'WIDOW', 'STABLE_UNION', 'SEPARATED', 'UNKNOWN'], table=TABLE),
    common.TestColumnValues(column="education_level_code", values=['LESS_THAN_PRIMARY_EDUCATION', 'PRIMARY_EDUCATION', 'LOWER_SECONDARY_EDUCATION', 'UPPER_SECONDARY_EDUCATION', 'POST_SECONDARY_NON_TERTIARY_EDUCATION', 'SHORT_CYCLE_TERTIARY_EDUCATION', 'BACHELORS_OR_EQUIVALENT', 'MASTERS_OR_EQUIVALENT', 'DOCTORAL_OR_EQUIVALENT', 'NOT_ELSEWHERE_CLASSIFIED', 'UNKNOWN'], table=TABLE),
    common.TestColumnValues(column="nationalities.region_code", values=get_valid_region_codes(), table=TABLE),
    common.TestColumnValues(column="residencies.region_code", values=get_valid_region_codes(), table=TABLE)
])
def test_column_values(connection, test):
    test(connection)

@pytest.mark.parametrize("test", [
    common.TestNullIf(column="birth_date", table=TABLE, expression=TABLE.type == "COMPANY"),
    common.TestNullIf(column="gender", table=TABLE, expression=TABLE.type == "COMPANY"),
    common.TestNullIf(column="establishment_date", table=TABLE, expression=TABLE.type == "CONSUMER"),
    common.TestNullIf(column="occupation", table=TABLE, expression=TABLE.type == "CONSUMER"),
])
def test_null_if(connection, test):
    test(connection)

def test_referential_integrity(connection):
    to_table_obj = get_unbound_table("account_party_link")
    test = common.TestReferentialIntegrity(table=TABLE, to_table=to_table_obj, keys=["party_id"], severity=TestSeverity.WARN)
    test(connection)

if __name__ == "__main__":
    retcode = pytest.main()
