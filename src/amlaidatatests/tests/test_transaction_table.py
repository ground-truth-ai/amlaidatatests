from amlaidatatests.test_generators import entity_columns, get_entity_mutation_tests, get_entity_tests, get_generic_table_tests, non_nullable_fields
from amlaidatatests.tests import common
from amlaidatatests.schema.utils import get_unbound_table
from amlaidatatests.base import AbstractColumnTest, AbstractTableTest, AMLAITestSeverity
import pytest

TABLE = get_unbound_table("transaction")

@pytest.mark.parametrize("test", get_generic_table_tests(table=TABLE, max_rows_factor=50e9))
def test_table(connection, test: AbstractTableTest):
    test(connection=connection)

def test_primary_keys(connection):
    test = common.TestPrimaryKeyColumns(table=TABLE, unique_combination_of_columns=["transaction_id", "validity_start_time"])
    test(connection)

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

@pytest.mark.parametrize("column", non_nullable_fields(TABLE.schema()))
def test_non_nullable_fields(connection, column):
    test = common.TestFieldNeverNull(table=TABLE, column=column)
    test(connection)

@pytest.mark.parametrize("to_table,keys", [["account_party_link", (["account_id"])]] )
def test_referential_integrity(connection, to_table: str, keys: list[str]):
    to_table_obj = get_unbound_table(to_table)
    test = common.TestReferentialIntegrity(table=TABLE, to_table=to_table_obj, keys=keys)
    test(connection)

@pytest.mark.parametrize("test", get_entity_mutation_tests(table=TABLE, primary_keys=["transaction_id"]))
def test_entity_mutation_tests(connection, test: AbstractColumnTest):
    test(connection=connection)

@pytest.mark.parametrize("test", [
    common.TestColumnValues(column="type", values=['WIRE', 'CASH', 'CHECK', 'CARD', 'OTHER'], table=TABLE),
    common.TestColumnValues(column="direction", values=['DEBIT', 'CREDIT'], table=TABLE)
])
def test_column_values(connection, test):
    test(connection)

@pytest.mark.parametrize("column", entity_columns(schema=TABLE.schema(), entity_types=["CurrencyValue"]))
@pytest.mark.parametrize("test", get_entity_tests(table=TABLE, entity_name="CurrencyValue"))
def test_currency_value_entity(connection, column, test: AbstractColumnTest):
    test(connection=connection, prefix=column)