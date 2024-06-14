from amlaidatatests.tests.base import FailTest
import ibis
import pytest
from ibis.expr.datatypes import String, Struct
from amlaidatatests.tests import common_tests

@pytest.fixture()
def in_memory_connection():
    return ibis.pandas.connect()


def test_missing_required_column(in_memory_connection):
    table = ibis.table(name="test_table", schema={"a": String(nullable=False), "b": String(nullable=False)})
    in_memory_connection.create_table("test_table", ibis.memtable(data=[{'a': "alpha"}], schema={"a": str}))
    with pytest.raises(FailTest, match=r'Missing Required Column: b'):
        t = common_tests.TestColumnPresence(table=table, column="b")
        t(in_memory_connection)

# Expect this test to always be skipped - there's no other
# way of directly testing this because pytest does not expose the
# Skipped Exception it uses internally so we can't catch it without
# accessing the internal pytest API
def test_missing_optional_column_skips_test(in_memory_connection):
    table = ibis.table(name="test_table", schema={"a": String(nullable=True), "b": String(nullable=False)})
    in_memory_connection.create_table("test_table", ibis.memtable(data=[{'b': "alpha"}], schema={"b": str}))
    t = common_tests.TestColumnPresence(table=table, column='a')
    t(in_memory_connection)

def test_all_columns_present(in_memory_connection):
    schema = ibis.table(name="test_table", schema={"a": String(nullable=True), "b": String(nullable=False)})
    in_memory_connection.create_table("test_table", ibis.memtable(data=[{'a': "alpha", 'b': 'beta'}], schema={"a": str, "b": str}))
    t = common_tests.TestColumnPresence(table=schema, column='a')
    t(in_memory_connection)

def test_ignores_order_of_struct_columns(in_memory_connection) -> None:
    schema = ibis.table(name="test_table", schema={"a": Struct(fields={"1": String(), "2": String()})})
    in_memory_connection.create_table("test_table", ibis.memtable(data=[{'a': {"2": "hello", "1": "goodbye"}}], schema={"a": Struct(fields={"2": String(), "1": String()})}))
    t = common_tests.TestColumnType(table=schema, column='a')
    t(in_memory_connection)