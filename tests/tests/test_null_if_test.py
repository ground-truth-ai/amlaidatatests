import ibis
import pytest
from ibis.expr.datatypes import String

from amlaidatatests.base import FailTest
from amlaidatatests.schema.base import ResolvedTableConfig
from amlaidatatests.tests import common


def test_null_if_succeeds(test_connection, create_test_table):
    schema = {"type": String(), "b": String()}
    tbl = create_test_table(
        ibis.memtable(
            data=[{"type": "card", "b": None}, {"type": "cash", "b": "yes"}],
            schema=schema,
        )
    )

    table_config = ResolvedTableConfig(table=ibis.table(name=tbl, schema=schema))

    t = common.NullIfTest(
        table_config=table_config,
        column="b",
        expression=table_config.table.type == "card",
    )
    t(test_connection)


def test_null_if_fails(test_connection, create_test_table):
    schema = {"type": String(), "b": String()}

    tbl = create_test_table(
        ibis.memtable(
            data=[{"type": "card", "b": "bad"}, {"type": "cash", "b": "yes"}],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(table=ibis.table(name=tbl, schema=schema))

    t = common.NullIfTest(
        table_config=table_config,
        column="b",
        expression=table_config.table.type == "card",
    )
    with pytest.raises(FailTest, match=r"1 rows not fulfilling criteria"):
        t(test_connection)
