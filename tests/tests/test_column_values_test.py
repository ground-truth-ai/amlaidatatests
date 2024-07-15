import ibis
import pytest
from ibis.expr.datatypes import String

from amlaidatatests.base import FailTest
from amlaidatatests.schema.base import ResolvedTableConfig
from amlaidatatests.tests import common


def test_column_only_has_allowed_values(test_connection, create_test_table):
    schema = {"column": String(nullable=False)}

    tbl = create_test_table(
        ibis.memtable(
            data=[{"column": "alpha"}, {"column": "beta"}, {"column": "alpha"}],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(table=ibis.table(name=tbl, schema=schema))

    t = common.ColumnValuesTest(
        table_config=table_config, column="column", values=["alpha", "beta"]
    )

    t(test_connection)


def test_column_has_invalid_values(test_connection, create_test_table):
    schema = {"column": String(nullable=False)}

    tbl = create_test_table(
        ibis.memtable(
            data=[{"column": "alpha"}, {"column": "beta"}, {"column": "gamma"}],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(table=ibis.table(name=tbl, schema=schema))

    t = common.ColumnValuesTest(
        table_config=table_config, column="column", values=["alpha", "beta"]
    )
    with pytest.raises(
        expected_exception=FailTest,
        match=r"1 rows found with invalid values. Valid values are",
    ):
        t(test_connection)
