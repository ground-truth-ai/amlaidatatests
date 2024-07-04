import datetime
from amlaidatatests.base import FailTest
import ibis
import pytest
from amlaidatatests.tests.common import TestPrimaryKeyColumns
from ibis.expr.datatypes import String, Timestamp



@pytest.fixture
def test_base_table() -> TestPrimaryKeyColumns:
    def _test_base_table(table: str):
        table = ibis.table(name=table, schema={"alpha": String(nullable=False),
                            "beta": String(nullable=False)})
        return TestPrimaryKeyColumns(unique_combination_of_columns=["alpha", "beta"], table=table)
    return _test_base_table

def test_no_duplicate_rows(test_connection, test_base_table, create_test_table):
    data = ibis.memtable([{"alpha": "a", "beta": "1"}, {"alpha": "b", "beta": "2"}])
    tbl = create_test_table(data)
    t = test_base_table(tbl)
    
    t(test_connection)

def test_one_duplicate_rows(test_connection, test_base_table, create_test_table):
    data = ibis.memtable([{"alpha": "a", "beta": "1"}, {"alpha": "a", "beta": "1"}, {"alpha": "b", "beta": "2"}])
    tbl = create_test_table(data)
    t = test_base_table(tbl)

    with pytest.raises(FailTest, match=r'Found 1 duplicate values$'):
        t(test_connection)

def test_two_duplicate_rows(test_connection, test_base_table, create_test_table):
    data = ibis.memtable([{"alpha": "a", "beta": "1"}, {"alpha": "a", "beta": "1"}, {"alpha": "b", "beta": "2"}, {"alpha": "b", "beta": "2"}])
    tbl = create_test_table(data)
    t = test_base_table(tbl)

    with pytest.raises(FailTest, match=r'Found 2 duplicate values$'):
        t(test_connection)

def test_mixed_types(test_connection, create_test_table):
    schema = {"id": String(nullable=False),
              "id2": Timestamp(nullable=False, timezone="UTC")}
    data = create_test_table(ibis.memtable([ {"id": "a", "id2": datetime.datetime(1970,1,1, tzinfo=datetime.UTC)}], 
                                    schema=schema))
    tbl = ibis.table(name=data, 
                       schema=schema)
    t = TestPrimaryKeyColumns(unique_combination_of_columns=["id", "id2"], table=tbl)
        
    t(test_connection)