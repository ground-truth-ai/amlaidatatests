import ibis
import pytest
from ibis.expr.datatypes import String

from amlaidatatests.exceptions import DataTestFailure
from amlaidatatests.schema.base import ResolvedTableConfig, TableType
from amlaidatatests.tests import common


def test_verify_subset_succeeds(test_connection, create_test_table, request):
    schema = {"type": String(), "id_0": String(), "id_1": String()}
    tbl = create_test_table(
        ibis.memtable(
            data=[
                {"type": "exit", "id_0": "A", "id_1": "B"},
                {"type": "start", "id_0": "A", "id_1": "B"},
            ],
            schema=schema,
        )
    )

    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    t = common.VerifyEntitySubset(
        table_config=table_config,
        column="type",
        concat=["id_0", "id_1"],
        subset_value="exit",
        superset_value="start",
    )
    t(test_connection, request)


def test_verify_subset_fails(test_connection, create_test_table, request):
    schema = {"type": String(), "id_0": String(), "id_1": String()}

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {"type": "exit", "id_0": "A", "id_1": "B"},
            ],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    t = common.VerifyEntitySubset(
        table_config=table_config,
        column="type",
        concat=["id_0", "id_1"],
        subset_value="exit",
        superset_value="start",
    )
    with pytest.raises(DataTestFailure):
        t(test_connection, request)
