

from amlaidatatests.tests.base import FailTest
import ibis
import pytest
from ibis.expr.datatypes import String
from amlaidatatests.tests import common


@pytest.fixture()
def in_memory_connection():
    return ibis.pandas.connect()


def test_column_is_always_null(in_memory_connection):
    # Test behaviour if the other table has more than one key which isn't on
    # the table for testing. This shouldn't be a problem - there may well be
    # keys not on this table which are present on the base table
    table = ibis.table(name="table", schema={"id": String(nullable=False)})
    in_memory_connection.create_table("table", ibis.memtable(data=[{'id': None}, {'id': None}], schema={"id": str}))

    t = common.TestFieldNeverNull(table=table, column="id")

    with pytest.raises(FailTest, match=r'2 rows found with null values of id'):
        t(in_memory_connection)

def test_column_is_sometimes_null(in_memory_connection):
    # Test behaviour if the other table has more than one key which isn't on
    # the table for testing. This shouldn't be a problem - there may well be
    # keys not on this table which are present on the base table
    table = ibis.table(name="table", schema={"id": String(nullable=False)})
    in_memory_connection.create_table("table", ibis.memtable(data=[{'id': None}, {'id': '12'}], schema={"id": str}))

    t = common.TestFieldNeverNull(table=table, column="id")
    
    with pytest.raises(FailTest, match=r'2 rows found with null values of id'):
        t(in_memory_connection)
