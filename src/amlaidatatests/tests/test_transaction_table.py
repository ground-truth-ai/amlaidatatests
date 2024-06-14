from amlaidatatests.schema.v1 import transaction_schema
from amlaidatatests.schema.v1.common import non_nullable_fields
from amlaidatatests.tests import common_tests
from amlaidatatests.io import get_table_name
import pytest
import ibis

SCHEMA = transaction_schema
TABLE = ibis.table(transaction_schema, name=get_table_name("transaction"))

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