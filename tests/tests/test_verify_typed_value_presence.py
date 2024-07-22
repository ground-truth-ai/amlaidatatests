import ibis
import pytest
from ibis.expr.datatypes import String

from amlaidatatests.exceptions import FailTest
from amlaidatatests.schema.base import ResolvedTableConfig, TableType
from amlaidatatests.tests import common


def test_min_group_by(test_connection, create_test_table):
    schema = {"account_id": String(nullable=False), "column": String(nullable=False)}

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {"account_id": "1", "column": "born"},
                {"account_id": "1", "column": "lived"},
                {"account_id": "2", "column": "born"},
                {"account_id": "3", "column": "born"},
            ],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(
        table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    t = common.VerifyTypedValuePresence(
        table_config=table_config,
        group_by=["account_id"],
        min_number=1,
        column="column",
        value="born",
    )
    t(test_connection)  # should succeed


def test_min_group_by_fails(test_connection, create_test_table):
    schema = {"account_id": String(nullable=False), "column": String(nullable=False)}

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {"account_id": "1", "column": "lived"},
                {"account_id": "2", "column": "born"},
                {"account_id": "3", "column": "born"},
            ],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(
        table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    t = common.VerifyTypedValuePresence(
        table_config=table_config,
        group_by=["account_id"],
        min_number=1,
        column="column",
        value="born",
    )
    with pytest.raises(expected_exception=FailTest):
        t(test_connection)  # should succeed
