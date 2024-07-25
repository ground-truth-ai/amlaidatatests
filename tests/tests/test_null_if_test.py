import ibis
import pytest
from ibis.expr.datatypes import String

from amlaidatatests.exceptions import DataTestFailure
from amlaidatatests.schema.base import ResolvedTableConfig
from amlaidatatests.tests import common


def test_null_if_succeeds(test_connection, create_test_table, request):
    schema = {"type": String(), "b": String()}
    tbl = create_test_table(
        ibis.memtable(
            data=[{"type": "card", "b": None}, {"type": "cash", "b": "yes"}],
            schema=schema,
        )
    )

    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema)
    )

    t = common.NullIfTest(
        table_config=table_config,
        column="b",
        expression=lambda t: t.type == "card",
    )
    t(test_connection, request)


def test_null_if_fails(test_connection, create_test_table, request):
    schema = {"type": String(), "b": String()}

    tbl = create_test_table(
        ibis.memtable(
            data=[{"type": "card", "b": "bad"}, {"type": "cash", "b": "yes"}],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema)
    )

    t = common.NullIfTest(
        table_config=table_config,
        column="b",
        expression=lambda t: t.type == "card",
    )
    with pytest.raises(DataTestFailure, match=r"1 rows"):
        t(test_connection, request)
