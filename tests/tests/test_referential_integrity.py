from amlaidatatests.tests.base import FailTest
import ibis
import pytest
from ibis.expr.datatypes import String, Struct
from amlaidatatests.tests import common_tests

@pytest.fixture()
def in_memory_connection():
    return ibis.pandas.connect()


def test_missing_key_local_table(in_memory_connection):
    # Test behaviour if the other table has more than one key which isn't on
    # the table for testing. This shouldn't be a problem - there may well be
    # keys not on this table which are present on the base table
    local_table = ibis.table(name="local_table", schema={"id": String(nullable=False)})
    other_table = ibis.table(name="other_table", schema={"id": String(nullable=False)})
    in_memory_connection.create_table("local_table", ibis.memtable(data=[{'id': "1"}], schema={"a": str}))
    in_memory_connection.create_table("other_table", ibis.memtable(data=[{'id': "1"}, {'id': "2"}], schema={"a": str}))
    t = common_tests.TestReferentialIntegrity(table=local_table, to_table=other_table, keys=["id"])
    # Should pass - RI works one way only
    t(in_memory_connection)

def test_missing_key_other_table(in_memory_connection):
    local_table = ibis.table(name="local_table", schema={"id": String(nullable=False)})
    other_table = ibis.table(name="other_table", schema={"id": String(nullable=False)})
    in_memory_connection.create_table("other_table", ibis.memtable(data=[{'id': "1"}], schema={"a": str}))
    in_memory_connection.create_table("local_table", ibis.memtable(data=[{'id': "1"}, {'id': "2"}], schema={"a": str}))
    # Should fail - key doesn't exist
    with pytest.raises(FailTest, match=r'1 keys found in table'):
        t = common_tests.TestReferentialIntegrity(table=local_table, to_table=other_table, keys=["id"])
        t(in_memory_connection)

def test_missing_multiple_keys_other_table(in_memory_connection):
    local_table = ibis.table(name="local_table", schema={"id1": String(nullable=False), "id2": String(nullable=False)})
    other_table = ibis.table(name="other_table", schema={"id1": String(nullable=False), "id2": String(nullable=False)})
    in_memory_connection.create_table("other_table", ibis.memtable(data=[{'id1': "1", 'id2': "2"}], schema={"a": str}))
    in_memory_connection.create_table("local_table", ibis.memtable(data=[{'id1': "2", 'id2': "3"}], schema={"a": str}))
    # Should fail - key doesn't exist
    with pytest.raises(FailTest, match=r'1 keys found in table'):
        t = common_tests.TestReferentialIntegrity(table=local_table, to_table=other_table, keys=["id1", 'id2'])
        t(in_memory_connection)