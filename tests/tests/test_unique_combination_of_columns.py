from amlaidatatests.tests.base import FailTest
import ibis
import pytest
from amlaidatatests.tests.common_tests import TestUniqueCombinationOfColumns
from ibis.expr.datatypes import String

table = ibis.table(name="test_table", schema={"alpha": String(nullable=False),
                      "beta": String(nullable=False)})

@pytest.fixture()
def in_memory_connection():
    return ibis.pandas.connect()


test_unique_combination_of_columns = TestUniqueCombinationOfColumns(unique_combination_of_columns=["alpha", "beta"], table=table)

def test_no_duplicate_rows(in_memory_connection):
    data = ibis.memtable([{"alpha": "a", "beta": "1"}, {"alpha": "b", "beta": "2"}])
    in_memory_connection.create_table("test_table", data)
    
    test_unique_combination_of_columns(in_memory_connection)

def test_one_duplicate_rows(in_memory_connection):
    data = ibis.memtable([{"alpha": "a", "beta": "1"}, {"alpha": "a", "beta": "1"}, {"alpha": "b", "beta": "2"}])
    in_memory_connection.create_table("test_table", data)

    with pytest.raises(FailTest, match=r'Found 1 duplicate values$'):
        test_unique_combination_of_columns(in_memory_connection)

def test_two_duplicate_rows(in_memory_connection):
    data = ibis.memtable([{"alpha": "a", "beta": "1"}, {"alpha": "a", "beta": "1"}, {"alpha": "b", "beta": "2"}, {"alpha": "b", "beta": "2"}])
    in_memory_connection.create_table("test_table", data)

    with pytest.raises(FailTest, match=r'Found 2 duplicate values$'):
        test_unique_combination_of_columns(in_memory_connection)