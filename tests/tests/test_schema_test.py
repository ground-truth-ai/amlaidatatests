import ibis
import pytest
from ibis.expr.datatypes import Array, Int64, String, Struct

import amlaidatatests.base
import amlaidatatests.exceptions
from amlaidatatests.exceptions import DataTestFailure, SkipTest
from amlaidatatests.schema.base import ResolvedTableConfig
from amlaidatatests.tests import common


def test_missing_required_column(test_connection, create_test_table, request):
    tbl = create_test_table(ibis.memtable(data=[{"a": "alpha"}], schema={"a": str}))
    table = ibis.table(
        name=tbl, schema={"a": String(nullable=False), "b": String(nullable=False)}
    )

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.ColumnPresenceTest(table_config=table_config, column="b")

    with pytest.raises(DataTestFailure, match="Missing Required Column"):
        t(test_connection, request)


# Expect this test to always be skipped - there's no other
# way of directly testing this because pytest does not expose the
# Skipped Exception it uses internally so we can't catch it without
# accessing the internal pytest API
def test_missing_optional_column_skips_test(
    test_connection, create_test_table, test_raise_on_skip, request
):
    tbl = create_test_table(ibis.memtable(data=[{"b": "alpha"}], schema={"b": str}))
    table = ibis.table(
        name=tbl, schema={"a": String(nullable=True), "b": String(nullable=False)}
    )

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.ColumnPresenceTest(table_config=table_config, column="a")
    with pytest.raises(SkipTest, match=r"Skipping running test on non-existent"):
        t(test_connection, request)


def test_all_columns_present(test_connection, create_test_table, request):
    tbl = create_test_table(
        ibis.memtable(data=[{"a": "alpha", "b": "beta"}], schema={"a": str, "b": str})
    )

    table = ibis.table(
        name=tbl, schema={"a": String(nullable=True), "b": String(nullable=False)}
    )

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.ColumnPresenceTest(table_config=table_config, column="a")
    t(test_connection, request)


def test_ignores_order_of_struct_columns(
    test_connection, create_test_table, request
) -> None:
    tbl = create_test_table(
        ibis.memtable(
            data=[{"a": {"2": "hello", "1": "goodbye"}}],
            schema={"a": Struct(fields={"2": String(), "1": String()})},
        )
    )

    table = ibis.table(
        name=tbl, schema={"a": Struct(fields={"1": String(), "2": String()})}
    )

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.ColumnTypeTest(table_config=table_config, column="a")
    t(test_connection, request)


def test_excess_field_in_struct_warns(
    test_connection, create_test_table, request
) -> None:
    tbl = create_test_table(
        ibis.memtable(
            data=[{"a": {"2": "hello", "1": "goodbye"}}],
            schema={"a": Struct(fields={"2": String(), "1": String()})},
        )
    )

    table = ibis.table(name=tbl, schema={"a": Struct(fields={"1": String()})})

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.ColumnTypeTest(table_config=table_config, column="a")
    with pytest.warns(
        amlaidatatests.base.DataTestWarning,
        match="Additional fields found in struct",
    ):
        t(test_connection, request)


def test_missing_field_in_struct(test_connection, create_test_table, request) -> None:
    tbl = create_test_table(
        ibis.memtable(
            data=[{"a": {"2": "hello", "1": "goodbye"}}],
            schema={"a": Struct(fields={"2": String(), "1": String()})},
        )
    )

    table = ibis.table(
        name=tbl, schema={"a": Struct(fields={"1": String(), "3": String()})}
    )

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.ColumnTypeTest(table_config=table_config, column="a")
    with pytest.raises(
        amlaidatatests.exceptions.DataTestFailure,
        match=("Column type mismatch"),
    ):
        t(test_connection, request)


def test_excess_field_in_embedded_struct(
    test_connection, create_test_table, request
) -> None:
    tbl = create_test_table(
        ibis.memtable(
            data=[{"a": [{"2": "hello", "1": "goodbye"}]}],
            schema={
                "a": Array(value_type=Struct(fields={"2": String(), "1": String()}))
            },
        )
    )

    table = ibis.table(
        name=tbl, schema={"a": Array(value_type=Struct(fields={"1": String()}))}
    )

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.ColumnTypeTest(table_config=table_config, column="a")
    with pytest.warns(
        amlaidatatests.base.DataTestWarning,
        match="Additional fields found in struct",
    ):
        t(test_connection, request)


def test_path_from_excess_field(test_connection, create_test_table, request) -> None:
    t = common.ColumnTypeTest._find_extra_struct_fields(
        expected_type=Struct(fields={"1": String()}),
        actual_type=Struct(fields={"1": String(), "2": String()}),
        path="col",
    )
    assert t == ["col.2"]


def test_path_from_embedded_excess_struct_field(
    test_connection, create_test_table, request
) -> None:
    t = common.ColumnTypeTest._find_extra_struct_fields(
        expected_type=Struct(fields={"1": String()}),
        actual_type=Struct(fields={"1": String(), "2": Struct(fields={"3": String()})}),
        path="col",
    )
    assert t == ["col.2"]


def test_path_from_excess_field_in_embedded_struct(
    test_connection, create_test_table, request
) -> None:
    t = common.ColumnTypeTest._find_extra_struct_fields(
        expected_type=Struct(
            fields={"1": String(), "2": Struct(fields={"3": String()})}
        ),
        actual_type=Struct(
            fields={"1": String(), "2": Struct(fields={"3": String(), "4": String()})}
        ),
        path="col",
    )
    assert t == ["col.2.4"]


def test_one_excess_column(test_connection, create_test_table, request):
    tbl = create_test_table(
        ibis.memtable(
            data=[{"a": "hello", "b": "goodbye"}], schema={"a": String(), "b": String()}
        )
    )
    table = ibis.table(name=tbl, schema={"a": String()})

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.TableExcessColumnsTest(table_config=table_config)
    with pytest.warns(
        amlaidatatests.base.DataTestWarning, match="1 unexpected columns found in table"
    ):
        t(test_connection, request)


def test_no_warn_on_missing_column_only(test_connection, create_test_table, request):
    tbl = create_test_table(
        ibis.memtable(data=[{"b": "goodbye"}], schema={"b": String()})
    )
    table = ibis.table(name=tbl, schema={"a": String(), "b": String()})
    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.TableExcessColumnsTest(table_config=table_config)
    t(test_connection, request)


def test_two_excess_columns(test_connection, create_test_table, request):
    tbl = create_test_table(
        ibis.memtable(
            data=[{"a": "hello", "b": "goodbye", "c": "apple"}],
            schema={"a": String(), "b": String(), "c": String()},
        )
    )
    table = ibis.table(name=tbl, schema={"a": String()})

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.TableExcessColumnsTest(table_config=table_config)
    with pytest.warns(
        amlaidatatests.base.DataTestWarning, match="2 unexpected columns found in table"
    ):
        t(test_connection, request)


def test_column_wrong_type(test_connection, create_test_table, request):
    tbl = create_test_table(
        ibis.memtable(data=[{"a": "hello"}], schema={"a": String()})
    )
    table = ibis.table(name=tbl, schema={"a": Int64()})

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.ColumnTypeTest(table_config=table_config, column="a")
    with pytest.raises(
        amlaidatatests.exceptions.DataTestFailure,
        match="Column type mismatch:",
    ):
        t(test_connection, request)


def test_column_non_nullable_type(test_connection, create_test_table, request):
    tbl = create_test_table(
        ibis.memtable(data=[{"a": "hello"}], schema={"a": String(nullable=True)})
    )
    table = ibis.table(name=tbl, schema={"a": String(nullable=False)})

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.ColumnTypeTest(table_config=table_config, column="a")
    with pytest.raises(
        amlaidatatests.exceptions.DataTestFailure,
        match="Column type mismatch",
    ):
        t(test_connection, request)


def test_column_too_strict(test_connection, create_test_table, request):
    tbl = create_test_table(
        ibis.memtable(data=[{"a": "hello"}], schema={"a": String(nullable=False)})
    )
    table = ibis.table(name=tbl, schema={"a": String(nullable=True)})

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.ColumnTypeTest(table_config=table_config, column="a")
    with pytest.warns(
        amlaidatatests.base.DataTestWarning,
        match=("Schema is stricter than required: expected string found !string"),
    ):
        t(test_connection, request)


# def test_column_array(test_connection, create_test_table, request):
#     # Check that the underlying nullability of a column isn't checked for
#     # verifying the column in a container. This is because this information
#     # isn't surfaced to ibis
#     tbl = create_test_table(
#         ibis.memtable(
#             data=[{"a": ["hello"]}],
#             schema={"a": Array(nullable=False, value_type=String(nullable=False))},
#         )
#     )
#     table = ibis.table(
#        name=tbl, schema={"a": Array(nullable=False, value_type=String(nullable=True))}
#     )

#     table_config = ResolvedTableConfig(name=table.get_name(), table=table)

#     t = common.ColumnTypeTest(table_config=table_config, column="a")
#     t(test_connection, request)
