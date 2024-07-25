import ibis
import pytest
from ibis.expr.datatypes import String

from amlaidatatests.exceptions import DataTestFailure
from amlaidatatests.schema.base import ResolvedTableConfig, TableType
from amlaidatatests.tests import common


def test_column_cardinality_grouped_max(test_connection, create_test_table, request):
    schema = {
        "id": String(nullable=False),
        "value": String(nullable=False),
    }

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "ent1",
                    "value": "value1",
                },
                {
                    "id": "ent2",
                    "value": "value2",
                },
            ],
            schema=schema,
        )
    )
    # Event type doesn't attempt to deduplicate the table to the latest version
    # of a particular entity
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    t = common.ColumnCardinalityTest(
        column="value", table_config=table_config, group_by=["id"], max_number=1
    )

    t(test_connection, request)


def test_column_cardinality_test_grouped_max_fails(
    test_connection, create_test_table, request
):
    schema = {
        "id": String(nullable=False),
        "value": String(nullable=False),
    }

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "ent1",
                    "value": "value1",
                },
                {
                    "id": "ent1",
                    "value": "value2",
                },
                {
                    "id": "ent1",
                    "value": "value2",
                },
            ],
            schema=schema,
        )
    )
    # Event type doesn't attempt to deduplicate the table
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    t = common.ColumnCardinalityTest(
        column="value", table_config=table_config, group_by=["id"], max_number=1
    )
    with pytest.raises(expected_exception=DataTestFailure):
        t(test_connection, request)


def test_column_cardinality_test_grouped_min(
    test_connection, create_test_table, request
):
    schema = {
        "id": String(nullable=False),
        "value": String(nullable=False),
    }

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "ent1",
                    "value": "value1",
                },
                {
                    "id": "ent1",
                    "value": "value2",
                },
                {
                    "id": "ent2",
                    "value": "value1",
                },
                {
                    "id": "ent2",
                    "value": "value2",
                },
            ],
            schema=schema,
        )
    )
    # Event type doesn't attempt to deduplicate the table
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    t = common.ColumnCardinalityTest(
        column="value", table_config=table_config, group_by=["id"], min_number=2
    )
    t(test_connection, request)


def test_column_cardinality_test_grouped_min_fails(
    test_connection, create_test_table, request
):
    schema = {
        "id": String(nullable=False),
        "value": String(nullable=False),
    }

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "ent1",
                    "value": "value1",
                },
                {
                    "id": "ent1",
                    "value": "value2",
                },
                {
                    "id": "ent2",
                    "value": "value2",
                },
            ],
            schema=schema,
        )
    )
    # Event type doesn't attempt to deduplicate the table
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    t = common.ColumnCardinalityTest(
        column="value", table_config=table_config, group_by=["id"], min_number=2
    )
    with pytest.raises(expected_exception=DataTestFailure):
        t(test_connection, request)


def test_column_cardinality_test_global_min(
    test_connection, create_test_table, request
):
    schema = {
        "id": String(nullable=False),
        "value": String(nullable=False),
    }

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "ent1",
                    "value": "value1",
                },
                {
                    "id": "ent2",
                    "value": "value2",
                },
            ],
            schema=schema,
        )
    )
    # Event type doesn't attempt to deduplicate the table
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    t = common.ColumnCardinalityTest(
        column="value", table_config=table_config, min_number=2
    )
    t(test_connection, request)


# No group
def test_column_cardinality_test_global_min_fails(
    test_connection, create_test_table, request
):
    schema = {
        "id": String(nullable=False),
        "value": String(nullable=False),
    }

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "ent1",
                    "value": "value1",
                },
                {
                    "id": "ent2",
                    "value": "value1",
                },
            ],
            schema=schema,
        )
    )
    # Event type doesn't attempt to deduplicate the table
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    t = common.ColumnCardinalityTest(
        column="value", table_config=table_config, min_number=2
    )
    with pytest.raises(expected_exception=DataTestFailure):
        t(test_connection, request)


def test_column_cardinality_test_global_min_max_fails(
    test_connection, create_test_table, request
):
    schema = {
        "id": String(nullable=False),
        "value": String(nullable=False),
    }

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "ent1",
                    "value": "value1",
                },
                {
                    "id": "ent1",
                    "value": "value2",
                },
                {
                    "id": "ent1",
                    "value": "value3",
                },
                {
                    "id": "ent1",
                    "value": "value4",
                },
                {
                    "id": "ent1",
                    "value": "value5",
                },
                {
                    "id": "ent2",
                    "value": "value1",
                },
            ],
            schema=schema,
        )
    )
    # Event type doesn't attempt to deduplicate the table
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    t = common.ColumnCardinalityTest(
        column="value", table_config=table_config, min_number=2, max_number=4
    )
    with pytest.raises(expected_exception=DataTestFailure):
        t(test_connection, request)
