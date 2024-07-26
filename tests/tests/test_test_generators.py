import datetime

import ibis
from ibis.expr.datatypes import String, Struct, Timestamp

from amlaidatatests.schema.base import ResolvedTableConfig
from amlaidatatests.test_generators import (
    find_consistent_timestamp_offset,
    get_non_nullable_fields,
    get_timestamp_fields,
)


def test_nonnullable_fields():

    test_table_config = ResolvedTableConfig(
        name="tbl",
        table=ibis.table(
            name="tbl",
            schema=ibis.schema(
                {
                    "a": Timestamp(nullable=False),
                    "b": Struct(nullable=True, fields={"a": String(nullable=False)}),
                }
            ),
        ),
    )

    ttt = get_non_nullable_fields(test_table_config)
    assert ttt == ["a", "b.a"]


def test_timestamp_fields():
    test_table_config = ResolvedTableConfig(
        name="tbl",
        table=ibis.table(
            name="tbl", schema=ibis.schema({"a": Timestamp(nullable=False)})
        ),
    )

    ttt = get_timestamp_fields(test_table_config)
    assert ttt == ["a"]


def test_find_consistent_timestamp_offset(test_connection, create_test_table, request):

    schema = ibis.schema({"a": Timestamp(nullable=False)})
    tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "a": datetime.datetime(
                        2020, 1, 1, hour=1, tzinfo=datetime.timezone.utc
                    )
                },
                {
                    "a": datetime.datetime(
                        2020, 1, 1, hour=0, tzinfo=datetime.timezone.utc
                    )
                },
            ],
            schema=schema,
        )
    )
    table = ibis.table(schema, tbl)
    expr = find_consistent_timestamp_offset(field="a", table=table)
    result = test_connection.execute(table.count(where=expr))
    assert result == 1
