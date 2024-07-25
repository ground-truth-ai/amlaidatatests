import ibis
import pytest
from ibis.expr.datatypes import String

from amlaidatatests.exceptions import DataTestFailure
from amlaidatatests.schema.base import ResolvedTableConfig, TableType
from amlaidatatests.tests import common


def test_consistent_ids_per_column(test_connection, create_test_table, request):
    schema = {
        "party_id": String(nullable=False),
        "party_supplementary_data_id": String(nullable=False),
    }

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {"party_id": "1", "party_supplementary_data_id": "1"},
                {"party_id": "1", "party_supplementary_data_id": "2"},
                {"party_id": "1", "party_supplementary_data_id": "3"},
                {"party_id": "2", "party_supplementary_data_id": "1"},
                {"party_id": "2", "party_supplementary_data_id": "2"},
                {"party_id": "2", "party_supplementary_data_id": "3"},
            ],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    t = common.ConsistentIDsPerColumn(
        table_config=table_config,
        column="party_id",
        id_to_verify="party_supplementary_data_id",
    )
    t(test_connection, request)  # should succeed


def test_inconsistent_ids_per_column(test_connection, create_test_table, request):
    schema = {
        "party_id": String(nullable=False),
        "party_supplementary_data_id": String(nullable=False),
    }

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {"party_id": "1", "party_supplementary_data_id": "1"},
                {"party_id": "1", "party_supplementary_data_id": "2"},
                {"party_id": "1", "party_supplementary_data_id": "3"},
                {"party_id": "2", "party_supplementary_data_id": "1"},
                {"party_id": "2", "party_supplementary_data_id": "2"},
            ],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    t = common.ConsistentIDsPerColumn(
        table_config=table_config,
        column="party_id",
        id_to_verify="party_supplementary_data_id",
    )
    with pytest.raises(DataTestFailure):
        t(test_connection, request)
