import ibis
import pytest
from ibis.expr.datatypes import Array, String, Struct

from amlaidatatests.exceptions import DataTestFailure
from amlaidatatests.schema.base import ResolvedTableConfig
from amlaidatatests.tests import common


def test_column_only_has_allowed_values(test_connection, create_test_table, request):
    schema = {"column": String(nullable=False)}

    tbl = create_test_table(
        ibis.memtable(
            data=[{"column": "alpha"}, {"column": "beta"}, {"column": "alpha"}],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema)
    )

    t = common.ColumnValuesTest(
        table_config=table_config, column="column", allowed_values=["alpha", "beta"]
    )

    t(test_connection, request)


def test_column_has_invalid_values(test_connection, create_test_table, request):
    schema = {"column": String(nullable=False)}

    tbl = create_test_table(
        ibis.memtable(
            data=[{"column": "alpha"}, {"column": "beta"}, {"column": "gamma"}],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema)
    )

    t = common.ColumnValuesTest(
        table_config=table_config, column="column", allowed_values=["alpha", "beta"]
    )
    with pytest.raises(
        expected_exception=DataTestFailure,
        match="1 rows",
    ):
        t(test_connection, request)


def test_column_only_has_allowed_values_embedded_struct(
    test_connection, create_test_table, request
):
    schema = {"column": Array(value_type=Struct(fields={"v": String(nullable=False)}))}

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {"column": [{"v": "alpha"}]},
                {"column": [{"v": "alpha"}]},
                {"column": [{"v": "alpha"}]},
            ],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema)
    )

    t = common.ColumnValuesTest(
        table_config=table_config, column="column.v", allowed_values=["alpha", "beta"]
    )

    t(test_connection, request)
