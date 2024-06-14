from amlaidatatests.schema.v1 import transaction_schema
from amlaidatatests.schema.v1.common import EntityTest, get_entity_mutation_tests, non_nullable_fields
from amlaidatatests.tests import common_tests
import pytest

SCHEMA = transaction_schema

@pytest.fixture
def table(table_factory):
    return table_factory("transaction")



# For each column in the schema, check all columns are all present
@pytest.mark.parametrize("column", SCHEMA.fields.keys())
def test_column_presence(table, column: str):
    test = common_tests.TestColumnPresence(SCHEMA)
    test(table=table, column=column)

# For each column in the schema, check all columns are the correct type
@pytest.mark.parametrize("column", SCHEMA.fields.keys())
def test_column_type(table, column):
    test = common_tests.TestColumnType(SCHEMA)
    test(column=column, table=table)

@pytest.mark.parametrize("column", non_nullable_fields(SCHEMA))
def test_non_nullable_fields(table, column):
    test = common_tests.TestFieldNeverNull(SCHEMA)
    test(column=column, table=table)

def test_unique_combination_of_columns(table):
    test = common_tests.TestUniqueCombinationOfColumns(schema=SCHEMA, unique_combination_of_columns=["transaction_id", "validity_start_time"])
    test(table=table)

@pytest.mark.parametrize("test", get_entity_mutation_tests(schema=SCHEMA, primary_keys=["transaction_id"]))
def test_entity_mutation_tests(table, test: EntityTest):
    f = test.test
    f(table=table)