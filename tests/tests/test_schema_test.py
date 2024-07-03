from amlaidatatests.tests.base import FailTest
import ibis
import pytest
from ibis.expr.datatypes import String, Struct, Int64
from amlaidatatests.tests import common




def test_missing_required_column(test_connection, create_test_table):
    tbl = create_test_table(ibis.memtable(data=[{'a': "alpha"}], schema={"a": str}))
    table = ibis.table(name=tbl, schema={"a": String(nullable=False), "b": String(nullable=False)})

    with pytest.raises(FailTest, match=r'Missing Required Column: b'):
        t = common.TestColumnPresence(table=table, column="b")
        t(test_connection)

# Expect this test to always be skipped - there's no other
# way of directly testing this because pytest does not expose the
# Skipped Exception it uses internally so we can't catch it without
# accessing the internal pytest API
def test_missing_optional_column_skips_test(test_connection, create_test_table):
    tbl = create_test_table(ibis.memtable(data=[{'b': "alpha"}], schema={"b": str}))
    table = ibis.table(name=tbl, schema={"a": String(nullable=True), "b": String(nullable=False)})

    t = common.TestColumnPresence(table=table, column='a')
    t(test_connection)
    raise Exception("This test should always be skipped")

def test_all_columns_present(test_connection, create_test_table):
    tbl = create_test_table(ibis.memtable(data=[{'a': "alpha", 'b': 'beta'}], schema={"a": str, "b": str}))

    table = ibis.table(name=tbl, schema={"a": String(nullable=True), "b": String(nullable=False)})
    t = common.TestColumnPresence(table=table, column='a')
    t(test_connection)

def test_ignores_order_of_struct_columns(test_connection, create_test_table) -> None:
    tbl = create_test_table(ibis.memtable(data=[{'a': {"2": "hello", "1": "goodbye"}}], schema={"a": Struct(fields={"2": String(), "1": String()})}))

    table = ibis.table(name=tbl, schema={"a": Struct(fields={"1": String(), "2": String()})})
    t = common.TestColumnType(table=table, column='a')
    t(test_connection)

def test_one_excess_column(test_connection, create_test_table):
    tbl = create_test_table(ibis.memtable(data=[{'a': "hello", "b": "goodbye"}], schema={"a": String(), "b": String()}))
    table = ibis.table(name=tbl, schema={"a": String()})

    t = common.TestTableSchema(table=table)
    with pytest.warns(common.WarnTest, match="1 unexpected columns found in table"):
        t(test_connection)

def test_no_warn_on_missing_column_only(test_connection, create_test_table):
    tbl = create_test_table(ibis.memtable(data=[{"b": "goodbye"}], schema={"b": String()}))
    table = ibis.table(name=tbl, schema={"a": String(), "b": String()})
    
    t = common.TestTableSchema(table=table)
    t(test_connection)

def test_two_excess_columns(test_connection, create_test_table):
    tbl = create_test_table(ibis.memtable(data=[{'a': "hello", "b": "goodbye", "c": "apple"}], schema={"a": String(), "b": String(), "c": String()}))
    schema = ibis.table(name=tbl, schema={"a": String()})

    t = common.TestTableSchema(table=schema)
    with pytest.warns(common.WarnTest, match="2 unexpected columns found in table"):
        t(test_connection)

def test_column_wrong_type(test_connection, create_test_table):
    tbl = create_test_table(ibis.memtable(data=[{'a': "hello"}], schema={"a": String()}))
    schema = ibis.table(name=tbl, schema={"a": Int64()})

    t = common.TestColumnType(table=schema, column='a')
    with pytest.raises(common.FailTest, match="Expected column a to be int64, found string"):
        t(test_connection)

def test_column_non_nullable_type(test_connection, create_test_table):
    tbl = create_test_table(ibis.memtable(data=[{'a': "hello"}], schema={"a": String(nullable=True)}))
    schema = ibis.table(name=tbl, schema={"a": String(nullable=False)})

    t = common.TestColumnType(table=schema, column='a')
    with pytest.raises(common.FailTest, match="Expected column a to be !string, found string"):
        t(test_connection)

def test_column_too_strict(test_connection, create_test_table):
    tbl = create_test_table(ibis.memtable(data=[{'a': "hello"}], schema={"a": String(nullable=False)}))
    schema = ibis.table(name=tbl, schema={"a": String(nullable=True)})

    t = common.TestColumnType(table=schema, column='a')
    with pytest.warns(common.WarnTest, match="Schema is stricter than required: expected column a to be string, found !string"):
        t(test_connection)