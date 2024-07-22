from amlaidatatests.test_generators import (
    find_consistent_timestamp_offset,
    timestamp_fields,
)
from ibis.expr.datatypes import Timestamp
import ibis
import datetime


def test_timestamp_fields(test_connection, create_test_table):
    test_schema = ibis.schema({"a": Timestamp(nullable=False)})
    ttt = timestamp_fields(test_schema)
    assert ttt == ["a"]

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
    expr = find_consistent_timestamp_offset(field="a", t=table)
    result = test_connection.execute(table.count(where=expr))
    assert result == 1
