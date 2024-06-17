

from amlaidatatests.schema.utils import get_table
from amlaidatatests.test_generators import get_entity_mutation_tests
from amlaidatatests.tests import common_tests
from amlaidatatests.tests.base import AbstractColumnTest
import pytest

TABLE = get_table("account_party_link")

def test_unique_combination_of_columns(connection):
    test = common_tests.TestUniqueCombinationOfColumns(table=TABLE, unique_combination_of_columns=["party_id", 
                                                                                                   "account_id", "validity_start_time"])
    test(connection)

# For each column in the schema, check all columns are all present
@pytest.mark.parametrize("column", TABLE.schema().fields.keys())
def test_column_presence(connection, column: str):
    test = common_tests.TestColumnPresence(table=TABLE, column=column)
    test(connection)

@pytest.mark.parametrize("to_table,keys", [["party", (["party_id"])]] )
def test_referential_integrity(connection, to_table: str, keys: list[str]):
    to_table_obj = get_table(to_table)
    test = common_tests.TestReferentialIntegrity(table=TABLE, to_table=to_table_obj, keys=keys)
    test(connection)

@pytest.mark.parametrize("column,values", [
    ("role", ['PRIMARY_HOLDER', 'SECONDARY_HOLDER', 'SUPPLEMENTARY_HOLDER'])
])
def test_column_values(connection, column, values):
    test = common_tests.TestColumnValues(values=values, table=TABLE, column=column)
    test(connection)

@pytest.mark.parametrize("test", get_entity_mutation_tests(table=TABLE, primary_keys=["party_id", "account_id"]))
def test_entity_mutation_tests(connection, test: AbstractColumnTest):
    test(connection=connection)
