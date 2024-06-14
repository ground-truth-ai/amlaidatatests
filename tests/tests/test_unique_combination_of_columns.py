from amlaidatatests.tests.base import FailTest
import ibis
import pytest
from amlaidatatests.tests.common_tests import TestUniqueCombinationOfColumns
from ibis.expr.datatypes import String

schema = ibis.schema({"alpha": String(nullable=False),
                      "beta": String(nullable=False)})

test_unique_combination_of_columns = TestUniqueCombinationOfColumns(unique_combination_of_columns=["alpha", "beta"], schema=schema)

def test_no_duplicate_rows():
    data = ibis.memtable([{"alpha": "a", "beta": "1"}, {"alpha": "b", "beta": "2"}])
    test_unique_combination_of_columns.test(table=data)

def test_one_duplicate_rows():
    data = ibis.memtable([{"alpha": "a", "beta": "1"}, {"alpha": "a", "beta": "1"}, {"alpha": "b", "beta": "2"}])
    with pytest.raises(FailTest, match=r'Found 1 duplicate values$'):
        test_unique_combination_of_columns.test(table=data)

def test_two_duplicate_rows():
    data = ibis.memtable([{"alpha": "a", "beta": "1"}, {"alpha": "a", "beta": "1"}, {"alpha": "b", "beta": "2"}, {"alpha": "b", "beta": "2"}])
    with pytest.raises(FailTest, match=r'Found 2 duplicate values$'):
        test_unique_combination_of_columns.test(table=data)