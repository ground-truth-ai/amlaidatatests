import ibis
import pytest
from ibis.expr.datatypes import String, Struct

from amlaidatatests.exceptions import DataTestFailure
from amlaidatatests.schema.base import ResolvedTableConfig
from amlaidatatests.tests import common


def test_column_is_always_null(test_connection, create_test_table, request):
    schema = {"id": String(nullable=False)}

    tbl = create_test_table(
        ibis.memtable(data=[{"id": None}, {"id": None}], schema={"id": String()})
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema)
    )

    t = common.FieldNeverNullTest(table_config=table_config, column="id")

    with pytest.raises(
        expected_exception=DataTestFailure,
        match="2 rows",
    ):
        t(test_connection, request)


def test_column_optional_parent_always_present(
    test_connection, create_test_table, request
):
    # Optional subfield should only be checked if the underlying field is missing
    schema = {
        "parent_id": Struct(
            nullable=True, fields={"id": String(nullable=False), "other": String()}
        )
    }

    tbl = create_test_table(
        ibis.memtable(
            data=[{"parent_id": None}, {"parent_id": {"id": "hello"}}], schema=schema
        )
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema)
    )

    t = common.FieldNeverNullTest(table_config=table_config, column="parent_id.id")

    t(test_connection, request)


def test_column_optional_parent_field_missing(
    test_connection, create_test_table, request
):
    schema = {"parent_id": Struct(nullable=True, fields={"id": String(nullable=True)})}

    tbl = create_test_table(
        ibis.memtable(
            data=[{"parent_id": None}, {"parent_id": {"id": None, "other": "hello"}}],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema)
    )

    t = common.FieldNeverNullTest(table_config=table_config, column="parent_id.id")

    with pytest.raises(
        expected_exception=DataTestFailure,
        match="1 rows",
    ):
        t(test_connection, request)


def test_column_is_sometimes_null(test_connection, create_test_table, request):
    schema = {"id": String(nullable=False)}

    tbl = create_test_table(
        ibis.memtable(data=[{"id": None}, {"id": "12"}], schema={"id": String()})
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema)
    )

    t = common.FieldNeverNullTest(table_config=table_config, column="id")

    with pytest.raises(
        expected_exception=DataTestFailure,
        match="1 rows",
    ):
        t(test_connection, request)


def test_column_never_null(
    test_connection: ibis.BaseBackend, create_test_table, request
):
    schema = {"id": String(nullable=False)}

    tbl = create_test_table(
        ibis.memtable(data=[{"id": "22"}, {"id": "12"}], schema=schema)
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema)
    )

    t = common.FieldNeverNullTest(table_config=table_config, column="id")

    t(test_connection, request)


def test_string_column_blanks(
    test_connection: ibis.BaseBackend, create_test_table, request
):
    schema = {"id": String(nullable=False)}

    tbl = create_test_table(
        ibis.memtable(data=[{"id": ""}, {"id": "   "}], schema={"id": str})
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema)
    )

    t = common.FieldNeverWhitespaceOnlyTest(table_config=table_config, column="id")

    with pytest.raises(
        expected_exception=DataTestFailure,
        match="2 rows ",
    ):
        t(test_connection, request)


def test_string_column_no_blanks(
    test_connection: ibis.BaseBackend, create_test_table, request
):
    schema = {"id": String(nullable=False)}

    tbl = create_test_table(
        ibis.memtable(data=[{"id": "   a   "}, {"id": "  1234  "}], schema={"id": str})
    )

    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema)
    )

    t = common.FieldNeverWhitespaceOnlyTest(table_config=table_config, column="id")

    t(test_connection, request)
