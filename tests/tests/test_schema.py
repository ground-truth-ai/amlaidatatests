from amlaidatatests.tests.base import FailTest
import ibis
import pytest

def test_missing_column(test_column_presence):
    schema = ibis.Schema({"a": str, "b": str})
    table = ibis.memtable(data=[], schema={"a": str})
    with pytest.raises(FailTest, match=r'Missing Column: b'):
        test_column_presence(table=table, schema=schema)

def test_multiple_missing_column(test_column_presence):
    schema = ibis.Schema({"a": str, "b": str, "c": str})
    table = ibis.memtable(data=[], schema={"a": str})
    with pytest.raises(FailTest, match=r'Missing Column: b\nMissing Column: c'):
        test_column_presence(table=table, schema=schema)

def test_has_all_columns(test_column_presence):
    schema = ibis.Schema({"a": str, "b": str})
    table = ibis.memtable(data=[], schema={"a": str, "b": str})
    test_column_presence(table=table, schema=schema)

def test_wrong_type(test_column_types):
    schema = ibis.Schema({"a": str})
    table = ibis.memtable(data=[], schema={"a": int})
    with pytest.raises(FailTest, match=r'Column a is the wrong type. Was int64, expected string'):
        test_column_types(table=table, schema=schema)

# def test_multiple_missing_column(test_column_presence):
#     schema = ibis.Schema({"a": str, "b": str, "c": str})
#     table = ibis.memtable(data=[], schema={"a": str})
#     with pytest.raises(FailTest, match=r'Missing Column: b\nMissing Column: c'):
#         test_column_presence(table=table, schema=schema)

# def test_has_all_columns(test_column_presence):
#     schema = ibis.Schema({"a": str, "b": str})
#     table = ibis.memtable(data=[], schema={"a": str, "b": str})
#     test_column_presence(table=table, schema=schema)