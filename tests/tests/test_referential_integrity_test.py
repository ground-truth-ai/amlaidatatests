from amlaidatatests.base import FailTest
import ibis
import pytest
from ibis.expr.datatypes import String
from amlaidatatests.tests import common




def test_missing_key_local_table(test_connection, create_test_table):
    # Test behaviour if the other table has more than one key which isn't on
    # the table for testing. This shouldn't be a problem - there may well be
    # keys not on this table which are present on the base table
    lcl_tbl = create_test_table(ibis.memtable(data=[{'id': "1"}], schema={"id": str}))
    otr_tbl = create_test_table(ibis.memtable(data=[{'id': "1"}, {'id': "2"}], schema={"id": str}))

    local_table = ibis.table(name=lcl_tbl, schema={"id": String(nullable=False)})
    other_table = ibis.table(name=otr_tbl, schema={"id": String(nullable=False)})

    t = common.TestReferentialIntegrity(table=local_table, to_table=other_table, keys=["id"])
    # Should pass - RI works one way only
    t(test_connection)

def test_missing_key_other_table(test_connection, create_test_table):

    schema = {"id": String(nullable=False)}

    lcl_tbl = create_test_table(ibis.memtable(data=[{'id': "1"}, {'id': "2"}], schema=schema))
    otr_tbl = create_test_table(ibis.memtable(data=[{'id': "1"}], schema=schema))

    local_table = ibis.table(name=lcl_tbl, schema=schema)
    other_table = ibis.table(name=otr_tbl, schema=schema)
    
    # Should fail - key doesn't exist
    with pytest.raises(FailTest, match=r'1 keys found in table'):
        t = common.TestReferentialIntegrity(table=local_table, to_table=other_table, keys=["id"])
        t(test_connection)

def test_passes_multiple_keys(test_connection, create_test_table):
    schema = {"id1": String(nullable=False), "id2": String(nullable=False)}
    
    lcl_tbl = create_test_table(ibis.memtable(data=[{'id1': "1", 'id2': "2"}, {'id1': "2", 'id2': "3"}], schema=schema))
    otr_tbl = create_test_table(ibis.memtable(data=[{'id1': "1", 'id2': "2"}, {'id1': "2", 'id2': "3"}], schema=schema))

    local_table = ibis.table(name=lcl_tbl, schema=schema)
    other_table = ibis.table(name=otr_tbl, schema=schema)
    t = common.TestReferentialIntegrity(table=local_table, to_table=other_table, keys=["id1", 'id2'])
    t(test_connection)


def test_passes_duplicated_keys(test_connection, create_test_table):
    schema={"id1": String(nullable=False), "id2": String(nullable=False)}

    lcl_tbl = create_test_table(ibis.memtable(data=[{'id1': "1", 'id2': "2"}, {'id1': "1", 'id2': "2"}], schema=schema))
    otr_tbl = create_test_table(ibis.memtable(data=[{'id1': "1", 'id2': "2"}, {'id1': "1", 'id2': "2"}], schema=schema))
    
    local_table = ibis.table(name=lcl_tbl, schema=schema)
    other_table = ibis.table(name=otr_tbl, schema=schema)

    t = common.TestReferentialIntegrity(table=local_table, to_table=other_table, keys=["id1", 'id2'])
    t(test_connection)

def test_missing_multiple_keys_other_table(test_connection, create_test_table):
    schema = {"id1": String(nullable=False), "id2": String(nullable=False)}

    lcl_tbl = create_test_table(ibis.memtable(data=[{'id1': "1", 'id2': "2"}], schema=schema))
    otr_tbl = create_test_table(ibis.memtable(data=[{'id1': "2", 'id2': "3"}], schema=schema))

    local_table = ibis.table(name=lcl_tbl, schema=schema)
    other_table = ibis.table(name=otr_tbl, schema=schema)
    # Should fail - key doesn't exist
    with pytest.raises(FailTest, match=r'1 keys found in table'):
        t = common.TestReferentialIntegrity(table=local_table, to_table=other_table, keys=["id1", 'id2'])
        t(test_connection)