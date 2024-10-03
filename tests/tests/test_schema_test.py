import ibis
import pytest
from ibis.expr.datatypes import Array, Int64, String, Struct

import amlaidatatests.base
import amlaidatatests.exceptions
from amlaidatatests.exceptions import DataTestFailure, SkipTest
from amlaidatatests.schema.base import ResolvedTableConfig
from amlaidatatests.tests import common


def test_missing_required_column(
    test_connection, create_test_table, request, test_raise_on_skip
):
    tbl = create_test_table(ibis.memtable(data=[{"a": "alpha"}], schema={"a": str}))
    table = ibis.table(
        name=tbl, schema={"a": String(nullable=False), "b": String(nullable=False)}
    )

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.ColumnPresenceTest(table_config=table_config, column="b")

    with pytest.raises(DataTestFailure, match="Required column b does not exist"):
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


def test_missing_required_nested_column(
    test_connection, create_test_table, request, test_raise_on_skip
):
    tbl = create_test_table(
        ibis.memtable(
            data=[{"a": {"a": "hello"}}],
            schema={"a": Struct(fields={"a": String()})},
        )
    )
    table = ibis.table(
        name=tbl,
        schema={
            "a": Struct(
                fields={"a": String(nullable=False), "b": String(nullable=False)}
            )
        },
    )

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.ColumnPresenceTest(table_config=table_config, column="a.b")

    with pytest.raises(DataTestFailure, match=r"Required column a.b does not exist"):
        t(test_connection, request)


def test_missing_optional_nested_column(
    test_connection, create_test_table, request, test_raise_on_skip
):
    tbl = create_test_table(
        ibis.memtable(
            data=[{"a": {"a": "hello"}}],
            schema={"a": Struct(fields={"a": String()})},
        )
    )
    table = ibis.table(
        name=tbl,
        schema={
            "a": Struct(
                fields={"a": String(nullable=False), "b": String(nullable=True)}
            )
        },
    )

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.ColumnPresenceTest(table_config=table_config, column="a.b")

    with pytest.raises(SkipTest, match=r"Skipping running test on non-existent"):
        t(test_connection, request)


def test_missing_optional_struct(
    test_connection, create_test_table, request, test_raise_on_skip
):
    tbl = create_test_table(
        ibis.memtable(
            data=[{"a": "hello"}],
            schema={"a": String(nullable=False)},
        )
    )
    table = ibis.table(
        name=tbl,
        schema={
            "a": Struct(
                fields={
                    "a": String(nullable=False),
                    "b": Struct(fields={"a": String()}),
                }
            )
        },
    )

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.ColumnPresenceTest(table_config=table_config, column="a.b")

    with pytest.raises(SkipTest, match=r"Skipping running test on non-existent"):
        t(test_connection, request)


def test_deeply_nested_struct_optional_1(
    test_connection, create_test_table, request, test_raise_on_skip
):
    if test_connection.dialect == "bigquery":
        pytest.xfail("Bigquery has a problem with creating nested structs")
    tbl = create_test_table(
        ibis.memtable(
            data=[{"c": "hello", "a": {"b": {"nanos": 1}}}],
            schema={
                "a": Struct(
                    fields={
                        "b": Struct(
                            nullable=True,
                            fields={
                                "nanos": Int64(),
                            },
                        )
                    }
                ),
                "c": String(nullable=False),
            },
        )
    )

    table = ibis.table(
        name=tbl,
        schema={
            "a": Struct(
                fields={
                    "b": Struct(
                        nullable=True,
                        fields={
                            "units": Int64(),
                            "nanos": Int64(),
                            "currency_code": String(),
                        },
                    )
                }
            ),
            "c": String(nullable=False),
        },
    )

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.ColumnPresenceTest(table_config=table_config, column="a.b.units")

    with pytest.raises(SkipTest, match=r"Skipping running test on non-existent"):
        t(test_connection, request)


def test_deeply_nested_struct_optional_2(
    test_connection, create_test_table, request, test_raise_on_skip
):
    if test_connection.dialect == "bigquery":
        pytest.xfail("Bigquery has a problem with creating nested structs")
    tbl = create_test_table(
        ibis.memtable(
            data=[{"c": "hello", "a": {"b": {"nanos": 1}}}],
            schema={
                "a": Struct(fields={"other": Struct(fields={"hello": String()})}),
                "c": String(nullable=False),
            },
        )
    )

    table = ibis.table(
        name=tbl,
        schema={
            "a": Struct(
                fields={
                    "b": Struct(
                        nullable=True,
                        fields={
                            "units": Int64(),
                            "nanos": Int64(),
                            "currency_code": String(),
                        },
                    )
                }
            ),
            "c": String(nullable=False),
        },
    )

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.ColumnPresenceTest(table_config=table_config, column="a.b.units")

    with pytest.raises(SkipTest, match=r"Skipping running test on non-existent"):
        t(test_connection, request)


def test_deeply_nested_struct_optional_3(
    test_connection, create_test_table, request, test_raise_on_skip
):
    if test_connection.dialect == "bigquery":
        pytest.xfail("Bigquery has a problem with creating nested structs")
    tbl = create_test_table(
        ibis.memtable(
            data=[{"c": "hello"}],
            schema={"c": String(nullable=False)},
        )
    )

    table = ibis.table(
        name=tbl,
        schema={
            "a": Struct(
                fields={
                    "b": Struct(
                        nullable=True,
                        fields={
                            "units": Int64(),
                            "nanos": Int64(),
                            "currency_code": String(),
                        },
                    )
                }
            ),
            "c": String(nullable=False),
        },
    )

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.ColumnPresenceTest(table_config=table_config, column="a.b.units")

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
        name=tbl,
        schema={
            "a": Struct(
                fields={"1": String(nullable=False), "3": String(nullable=False)}
            )
        },
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
        name=tbl,
        schema={"a": Array(value_type=Struct(fields={"1": String()}))},
    )

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.ColumnTypeTest(table_config=table_config, column="a")
    with pytest.warns(
        amlaidatatests.base.DataTestWarning,
        match="Additional fields found in struct",
    ):
        t(test_connection, request)


def test_path_from_excess_field(test_connection, create_test_table, request) -> None:
    t = common.ColumnTypeTest._check_field_types(
        expected_type=Struct(fields={"1": String()}),
        actual_type=Struct(fields={"1": String(), "2": String()}),
        path="col",
    )
    assert t == ["col.2"]


def test_path_from_embedded_excess_struct_field(
    test_connection, create_test_table, request
) -> None:
    t = common.ColumnTypeTest._check_field_types(
        expected_type=Struct(fields={"1": String()}),
        actual_type=Struct(fields={"1": String(), "2": Struct(fields={"3": String()})}),
        path="col",
    )
    assert t == ["col.2"]


def test_path_from_excess_field_in_embedded_struct(
    test_connection, create_test_table, request
) -> None:
    t = common.ColumnTypeTest._check_field_types(
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
    # Schema
    table = ibis.table(name=tbl, schema={"a": String(nullable=True)})

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.ColumnTypeTest(table_config=table_config, column="a")
    with pytest.warns(
        amlaidatatests.base.DataTestWarning,
        match=("Schema is stricter than required: expected string found !string"),
    ):
        t(test_connection, request)


def test_embedded_column_wrong_type(test_connection, create_test_table, request):
    tbl = create_test_table(
        ibis.memtable(
            data=[{"a": {"1": "hello"}}], schema={"a": Struct(fields={"1": String()})}
        )
    )
    table = ibis.table(name=tbl, schema={"a": Struct(fields={"1": Int64()})})

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.ColumnTypeTest(table_config=table_config, column="a")
    with pytest.raises(
        amlaidatatests.exceptions.DataTestFailure,
        match="Column type mismatch:",
    ):
        t(test_connection, request)


def test_missing_optional_column_in_struct(test_connection, create_test_table, request):
    """Test what happens if a not-required field is missing in the table schema"""
    # ibis doesn't create tables with embedded non-nullable fields, though
    # a PR was merged for version 10.x for bigquery
    tbl = create_test_table(
        ibis.memtable(
            data=[{"a": {"required": "here"}}],
            schema={
                "a": Struct(
                    fields={
                        "required": String(nullable=True),
                    }
                )
            },
        )
    )

    table = ibis.table(
        name=tbl,
        schema={
            "a": Struct(
                fields={
                    "required": String(nullable=True),
                    "not_required": String(nullable=True),
                }
            )
        },
    )

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.ColumnTypeTest(table_config=table_config, column="a")
    t(test_connection, request)


def test_all_optional_columns_in_struct(test_connection, create_test_table, request):
    """Test what happens if a not-required field is missing in the table schema"""
    tbl = create_test_table(
        ibis.memtable(
            data=[{"a": {"required": "here"}}],
            schema={
                "a": Struct(
                    fields={
                        "1": String(nullable=True),
                    }
                )
            },
        )
    )

    table = ibis.table(
        name=tbl,
        schema={
            "a": Struct(
                fields={
                    "1": String(nullable=True),
                    "2": String(nullable=True),
                }
            )
        },
    )

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.ColumnTypeTest(table_config=table_config, column="a")
    t(test_connection, request)
