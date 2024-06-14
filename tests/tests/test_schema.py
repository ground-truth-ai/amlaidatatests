from amlaidatatests.tests.base import FailTest
import ibis
import pytest
from ibis.expr.datatypes import String, Struct
from amlaidatatests.tests import common_tests


def test_missing_required_column():
    schema = ibis.Schema({"a": String(nullable=False), "b": String(nullable=False)})
    table = ibis.memtable(data=[{'a': "alpha"}], schema={"a": str})
    with pytest.raises(FailTest, match=r'Missing Required Column: b'):
        t = common_tests.TestColumnPresence(schema)
        t(table, "b")

# Expect this test to always be skipped - there's no other
# way of directly testing this because pytest does not expose the
# Skipped Exception it uses internally so we can't catch it without
# accessing the internal pytest API
def test_missing_optional_column_skips_test():
    schema = ibis.Schema({"a": String(nullable=True), "b": String(nullable=False)})
    table = ibis.memtable(data=[{'b': "alpha"}], schema={"b": str})
    t = common_tests.TestColumnPresence(schema)
    t(table, "a")

def test_all_columns_present():
    schema = ibis.Schema({"a": String(nullable=True), "b": String(nullable=False)})
    table = ibis.memtable(data=[{'a': "alpha", 'b': 'beta'}], schema={"a": str, "b": str})
    t = common_tests.TestColumnPresence(schema)
    t(table, "a")

def test_ignores_order_of_struct_columns() -> None:
    schema = ibis.Schema({"a": Struct(fields={"1": String(), "2": String()})})
    table = ibis.memtable(data=[{'a': {"2": "hello", "1": "goodbye"}}], schema={"a": Struct(fields={"2": String(), "1": String()})})
    t = common_tests.TestColumnType(schema)
    t(table, "a")