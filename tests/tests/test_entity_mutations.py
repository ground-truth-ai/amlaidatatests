import datetime
from amlaidatatests.base import AMLAITestSeverity, WarnTest
from amlaidatatests.schema.base import ResolvedTableConfig
import ibis
import pytest
from ibis.expr.datatypes import String, Boolean, Timestamp
from amlaidatatests.tests import common


def test_orphaned_deleted_entity(test_connection, create_test_table):
    schema = {
        "id": String(nullable=False),
        "is_entity_deleted": Boolean(nullable=False),
        "validity_start_time": Timestamp(timezone="UTC"),
    }

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "ent1",
                    "is_entity_deleted": True,
                    "validity_start_time": datetime.datetime(
                        2020, 1, 2, tzinfo=datetime.UTC
                    ),
                }
            ],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(table=ibis.table(name=tbl, schema=schema))

    t = common.TestOrphanDeletions(table_config=table_config, entity_ids=["id"])

    with pytest.warns(WarnTest, match="1 rows found with orphaned entity deletions"):
        t(test_connection)


def test_not_orphaned_deleted_entity(test_connection, create_test_table):
    schema = {
        "id": String(nullable=False),
        "is_entity_deleted": Boolean(nullable=False),
        "validity_start_time": Timestamp(timezone="UTC"),
    }

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "ent1",
                    "is_entity_deleted": False,
                    "validity_start_time": datetime.datetime(
                        2020, 1, 2, tzinfo=datetime.UTC
                    ),
                },
                {
                    "id": "ent1",
                    "is_entity_deleted": True,
                    "validity_start_time": datetime.datetime(
                        2020, 1, 2, tzinfo=datetime.UTC
                    ),
                },
            ],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(table=ibis.table(name=tbl, schema=schema))
    t = common.TestOrphanDeletions(
        table_config=table_config, entity_ids=["id"], severity=AMLAITestSeverity.ERROR
    )
    t(test_connection)
