from amlaidatatests.tests.base import FailTest
import ibis
import pytest
from ibis.expr.datatypes import String, Struct
from amlaidatatests.tests import common

def test_null_if_succeeds(test_connection, create_test_table):
    tbl = create_test_table(ibis.memtable(data=[{'type': "card", "b": None}, 
                                                {'type': "cash", "b": "yes"}], 
                                          schema={"type": String(), "b": String()}))
    table = ibis.table(name=tbl, schema={"type": String(), "b": String()})

    t = common.TestNullIf(table=table, column="b", expression=table.type == "card")
    t(test_connection)

def test_null_if_fails(test_connection, create_test_table):
    tbl = create_test_table(ibis.memtable(data=[{'type': "card", "b": "bad"}, 
                                                {'type': "cash", "b": "yes"}], 
                                          schema={"type": String(), "b": String()}))
    table = ibis.table(name=tbl, schema={"type": String(), "b": String()})

    t = common.TestNullIf(table=table, column="b", expression=table.type == "card")
    with pytest.raises(FailTest, match=r'1 rows not fulfilling criteria'):
        t(test_connection)