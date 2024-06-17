from amlaidatatests.schema.v1 import transaction_schema
from amlaidatatests.test_generators import entity_columns, get_entity_mutation_tests, get_entity_tests, non_nullable_fields
from amlaidatatests.tests import common_tests
from amlaidatatests.schema.utils import get_table, get_table_name
from amlaidatatests.tests.base import AbstractColumnTest
import pytest
import ibis

TABLE = get_table("transaction")

def test_unique_combination_of_columns(connection):
    test = common_tests.TestUniqueCombinationOfColumns(table=TABLE, unique_combination_of_columns=["transaction_id", "validity_start_time"])
    test(connection)

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

@pytest.mark.parametrize("column", non_nullable_fields(TABLE.schema()))
def test_non_nullable_fields(connection, column):
    test = common_tests.TestFieldNeverNull(table=TABLE, column=column)
    test(connection)

@pytest.mark.parametrize("to_table,keys", [["account_party_link", (["account_id"])]] )
def test_referential_integrity(connection, to_table: str, keys: list[str]):
    to_table_obj = get_table(to_table)
    test = common_tests.TestReferentialIntegrity(table=TABLE, to_table=to_table_obj, keys=keys)
    test(connection)

@pytest.mark.parametrize("test", get_entity_mutation_tests(table=TABLE, primary_keys=["transaction_id"]))
def test_entity_mutation_tests(connection, test: AbstractColumnTest):
    test(connection=connection)

@pytest.mark.parametrize("column,values", [
    ("type", ['WIRE', 'CASH', 'CHECK', 'CARD', 'OTHER']),
    ("direction", ['DEBIT', 'CREDIT']),
    
])
def test_column_values(connection, column, values):
    test = common_tests.TestColumnValues(values=values, table=TABLE, column=column)
    test(connection)

@pytest.mark.parametrize("column", entity_columns(schema=TABLE.schema(), entity_types=["CurrencyValue"]))
@pytest.mark.parametrize("test", get_entity_tests(table=TABLE, entity_name="CurrencyValue"))
def test_currency_value_entity(connection, column, test: AbstractColumnTest):
    test(connection=connection, prefix=column)