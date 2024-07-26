import datetime

import ibis
import pytest
from ibis.expr.datatypes import Int64, String, Timestamp

from amlaidatatests.exceptions import DataTestFailure
from amlaidatatests.schema.base import ResolvedTableConfig
from amlaidatatests.tests import common

SCHEMA = {
    "id": Int64(nullable=False),
    "event": String(nullable=False),
    "time": Timestamp(nullable=False, timezone="UTC"),
}


def test_incorrect_event_order(test_connection, create_test_table, request):

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": 1,
                    "event": "BORN",
                    "time": datetime.datetime(
                        2020, 1, 2, hour=1, tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "id": 1,
                    "event": "GRADUATED",
                    "time": datetime.datetime(
                        2020, 1, 2, hour=2, tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "id": 1,
                    "event": "NURSERY",
                    "time": datetime.datetime(
                        2020, 1, 2, hour=3, tzinfo=datetime.timezone.utc
                    ),
                },
            ],
            schema=SCHEMA,
        )
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=SCHEMA)
    )

    t = common.EventOrder(
        table_config=table_config,
        column="event",
        time_column="time",
        events=["BORN", "NURSERY", "GRADUATED"],
        group_by=["id"],
    )

    with pytest.raises(
        expected_exception=DataTestFailure,
    ):
        t(test_connection, request)


def test_correct_event_order(test_connection, create_test_table, request):

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": 1,
                    "event": "BORN",
                    "time": datetime.datetime(
                        2020, 1, 2, hour=1, tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "id": 1,
                    "event": "NURSERY",
                    "time": datetime.datetime(
                        2020, 1, 2, hour=2, tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "id": 1,
                    "event": "GRADUATED",
                    "time": datetime.datetime(
                        2020, 1, 2, hour=3, tzinfo=datetime.timezone.utc
                    ),
                },
            ],
            schema=SCHEMA,
        )
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=SCHEMA)
    )

    t = common.EventOrder(
        table_config=table_config,
        column="event",
        time_column="time",
        events=["BORN", "NURSERY", "GRADUATED"],
        group_by=["id"],
    )

    t(test_connection, request)


def test_correct_event_order_missing_middle_event(
    test_connection, create_test_table, request
):

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": 1,
                    "event": "BORN",
                    "time": datetime.datetime(
                        2020, 1, 2, hour=1, tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "id": 1,
                    "event": "NURSERY",
                    "time": datetime.datetime(
                        2020, 1, 2, hour=3, tzinfo=datetime.timezone.utc
                    ),
                },
            ],
            schema=SCHEMA,
        )
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=SCHEMA)
    )

    t = common.EventOrder(
        table_config=table_config,
        column="event",
        time_column="time",
        events=["BORN", "NURSERY", "GRADUATED"],
        group_by=["id"],
    )

    t(test_connection, request)


def test_correct_event_order_missing_last_event(
    test_connection, create_test_table, request
):

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": 1,
                    "event": "BORN",
                    "time": datetime.datetime(
                        2020, 1, 2, hour=1, tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "id": 1,
                    "event": "NURSERY",
                    "time": datetime.datetime(
                        2020, 1, 2, hour=3, tzinfo=datetime.timezone.utc
                    ),
                },
            ],
            schema=SCHEMA,
        )
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=SCHEMA)
    )

    t = common.EventOrder(
        table_config=table_config,
        column="event",
        time_column="time",
        events=["BORN", "NURSERY", "GRADUATED"],
        group_by=["id"],
    )

    t(test_connection, request)


def test_correct_event_order_missing_events_at_the_same(
    test_connection, create_test_table, request
):

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": 1,
                    "event": "BORN",
                    "time": datetime.datetime(
                        2020, 1, 2, hour=1, tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "id": 1,
                    "event": "NURSERY",
                    "time": datetime.datetime(
                        2020, 1, 2, hour=3, tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "id": 1,
                    "event": "GRADUATED",
                    "time": datetime.datetime(
                        2020, 1, 2, hour=3, tzinfo=datetime.timezone.utc
                    ),
                },
            ],
            schema=SCHEMA,
        )
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=SCHEMA)
    )

    t = common.EventOrder(
        table_config=table_config,
        column="event",
        time_column="time",
        events=["BORN", "GRADUATED"],
        group_by=["id"],
    )

    t(test_connection, request)
