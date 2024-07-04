from amlaidatatests.base import FailTest
import ibis
import pytest
from ibis.expr.datatypes import Int64, Float64
from amlaidatatests.tests import common


def test_equals_low_threshold(test_connection, create_test_table):
    schema = {"column": Int64(nullable=False)}

    tbl = create_test_table(ibis.memtable(data=[{'column': 1}, 
                                                {'column': 10}], schema=schema))
    table = ibis.table(name=tbl, schema=schema)

    t = common.TestAcceptedRange(table=table, min=1, column='column')
    t(test_connection) # should succeed

def test_equals_high_threshold(test_connection, create_test_table):
    schema = {"column": Int64(nullable=False)}

    tbl = create_test_table(ibis.memtable(data=[{'column': 1}, 
                                                {'column': 10}], schema=schema))
    table = ibis.table(name=tbl, schema=schema)

    t = common.TestAcceptedRange(table=table, max=10, column='column')
    t(test_connection) # should succeed

def test_float_in_range(test_connection, create_test_table):
    schema = {"column": Float64(nullable=False)}

    tbl = create_test_table(ibis.memtable(data=[{'column': 1.0}, 
                                                {'column': 10.0}], schema=schema))
    table = ibis.table(name=tbl, schema=schema)

    t = common.TestAcceptedRange(table=table, min=0, max=20, column='column')
    t(test_connection)

def test_int_in_range(test_connection, create_test_table):
    schema = {"column": Int64(nullable=False)}

    tbl = create_test_table(ibis.memtable(data=[{'column': 1}, 
                                                {'column': 10}], schema=schema))
    table = ibis.table(name=tbl, schema=schema)

    t = common.TestAcceptedRange(table=table, min=0, max=20, column='column')
    t(test_connection)

def test_int_out_of_range_max(test_connection, create_test_table):
    schema = {"column": Int64(nullable=False)}

    tbl = create_test_table(ibis.memtable(data=[{'column': 1}, 
                                                {'column': 25}], schema=schema))
    table = ibis.table(name=tbl, schema=schema)

    t = common.TestAcceptedRange(table=table, min=0, max=20, column='column')

    with pytest.raises(expected_exception=FailTest, match=r'1 rows in column'):
        t(test_connection)

def test_int_out_of_range_min(test_connection, create_test_table):
    schema = {"column": Int64(nullable=False)}

    tbl = create_test_table(ibis.memtable(data=[{'column': -10}, 
                                                {'column': 25}], schema=schema))
    table = ibis.table(name=tbl, schema=schema)

    t = common.TestAcceptedRange(table=table, min=0, column='column')

    with pytest.raises(expected_exception=FailTest, match=r'1 rows in column'):
        t(test_connection)