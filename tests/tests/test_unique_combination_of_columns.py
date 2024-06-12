from amlaidatatests.tests.base import FailTest
import ibis
import pytest

def test_no_duplicate_rows(unique_combination_of_columns):
    data = ibis.memtable([{"alpha": "a", "beta": "1"}, {"alpha": "b", "beta": "2"}])
    unique_combination_of_columns(table=data, unique_combination_of_columns=["alpha", "beta"])

def test_one_duplicate_rows(unique_combination_of_columns):
    data = ibis.memtable([{"alpha": "a", "beta": "1"}, {"alpha": "a", "beta": "1"}, {"alpha": "b", "beta": "2"}])
    with pytest.raises(FailTest, match=r'Found 1 duplicate values$'):
        unique_combination_of_columns(table=data, unique_combination_of_columns=["alpha", "beta"])

def test_two_duplicate_rows(unique_combination_of_columns):
    data = ibis.memtable([{"alpha": "a", "beta": "1"}, {"alpha": "a", "beta": "1"}, {"alpha": "b", "beta": "2"}, {"alpha": "b", "beta": "2"}])
    with pytest.raises(FailTest, match=r'Found 2 duplicate values$'):
        unique_combination_of_columns(table=data, unique_combination_of_columns=["alpha", "beta"])