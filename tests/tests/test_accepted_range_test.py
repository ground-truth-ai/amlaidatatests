import ibis
import pytest
from ibis.expr.datatypes import Float64, Int64

from amlaidatatests.exceptions import DataTestFailure
from amlaidatatests.schema.base import ResolvedTableConfig
from amlaidatatests.tests import common


def test_equals_low_threshold(test_connection, create_test_table, request):
    schema = {"column": Int64(nullable=False)}

    tbl = create_test_table(
        ibis.memtable(data=[{"column": 1}, {"column": 10}], schema=schema)
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema)
    )

    t = common.AcceptedRangeTest(
        table_config=table_config, min_value=1, column="column"
    )
    t(test_connection, request)  # should succeed


def test_equals_high_threshold(test_connection, create_test_table, request):
    schema = {"column": Int64(nullable=False)}

    tbl = create_test_table(
        ibis.memtable(data=[{"column": 1}, {"column": 10}], schema=schema)
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema)
    )

    t = common.AcceptedRangeTest(
        table_config=table_config, max_value=10, column="column"
    )
    t(test_connection, request)  # should succeed


def test_float_in_range(test_connection, create_test_table, request):
    schema = {"column": Float64(nullable=False)}

    tbl = create_test_table(
        ibis.memtable(data=[{"column": 1.0}, {"column": 10.0}], schema=schema)
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema)
    )

    t = common.AcceptedRangeTest(
        table_config=table_config, min_value=0, max_value=20, column="column"
    )
    t(test_connection, request)


def test_int_in_range(test_connection, create_test_table, request):
    schema = {"column": Int64(nullable=False)}

    tbl = create_test_table(
        ibis.memtable(data=[{"column": 1}, {"column": 10}], schema=schema)
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema)
    )

    t = common.AcceptedRangeTest(
        table_config=table_config, min_value=0, max_value=20, column="column"
    )
    t(test_connection, request)


def test_int_out_of_range_max(test_connection, create_test_table, request):
    schema = {"column": Int64(nullable=False)}

    tbl = create_test_table(
        ibis.memtable(data=[{"column": 1}, {"column": 25}], schema=schema)
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema)
    )

    t = common.AcceptedRangeTest(
        table_config=table_config, min_value=0, max_value=20, column="column"
    )

    with pytest.raises(expected_exception=DataTestFailure, match=r"1 rows"):
        t(test_connection, request)


def test_int_out_of_range_min(test_connection, create_test_table, request):
    schema = {"column": Int64(nullable=False)}

    tbl = create_test_table(
        ibis.memtable(data=[{"column": -10}, {"column": 25}], schema=schema)
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema)
    )

    t = common.AcceptedRangeTest(
        table_config=table_config, min_value=0, column="column"
    )

    with pytest.raises(expected_exception=DataTestFailure, match=r"1 rows"):
        t(test_connection, request)
