import ibis
import pytest
from ibis.expr.datatypes import Int64, String

from amlaidatatests.exceptions import DataTestFailure
from amlaidatatests.schema.base import ResolvedTableConfig, TableType
from amlaidatatests.tests import common


def test_count_frequency_values(test_connection, create_test_table, request):
    schema = {
        "party_id": String(nullable=False),
        "who": String(nullable=False),
    }

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {"party_id": "1", "who": "foo"},
                {"party_id": "2", "who": "foo"},
                {"party_id": "3", "who": "foo"},
                {"party_id": "4", "who": "baz"},
                {"party_id": "5", "who": "baz"},
                {"party_id": "6", "who": "baz"},
                {"party_id": "7", "who": "other"},
            ],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    t = common.CountFrequencyValues(
        column="who",
        table_config=table_config,
        max_proportion=0.50,
    )

    t(test_connection, request)  # should succeed


def test_count_frequency_values_fails(test_connection, create_test_table, request):
    schema = {
        "party_id": String(nullable=False),
        "who": String(nullable=False),
    }

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {"party_id": "1", "who": "foo"},
                {"party_id": "2", "who": "foo"},
                {"party_id": "3", "who": "foo"},
                {"party_id": "4", "who": "baz"},
                {"party_id": "5", "who": "baz"},
                {"party_id": "6", "who": "baz"},
            ],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    t = common.CountFrequencyValues(
        column="who",
        table_config=table_config,
        max_proportion=0.50,
    )

    with pytest.raises(
        expected_exception=DataTestFailure,
        match="2 column values",
    ):
        t(test_connection, request)  # should succeed


def test_count_frequency_values_ignores_nulls(
    test_connection, create_test_table, request
):
    schema = {
        "party_id": String(nullable=False),
        "who": String(nullable=True),
    }

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {"party_id": "1", "who": "foo"},
                {"party_id": "2", "who": "baz"},
                {"party_id": "3", "who": "other"},
                {"party_id": "6", "who": None},
                {"party_id": "7", "who": None},
                {"party_id": "8", "who": None},
                {"party_id": "9", "who": None},
                {"party_id": "10", "who": None},
                {"party_id": "11", "who": None},
                {"party_id": "12", "who": None},
                {"party_id": "13", "who": None},
            ],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    t = common.CountFrequencyValues(
        column="who",
        table_config=table_config,
        max_proportion=0.50,
    )
    t(test_connection, request)  # should succeed


def test_count_frequency_values_consider_nulls(
    test_connection, create_test_table, request
):
    schema = {
        "party_id": String(nullable=False),
        "who": String(nullable=True),
    }

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {"party_id": "1", "who": "foo"},
                {"party_id": "2", "who": "baz"},
                {"party_id": "3", "who": "other"},
                {"party_id": "6", "who": None},
                {"party_id": "7", "who": None},
                {"party_id": "8", "who": None},
                {"party_id": "9", "who": None},
                {"party_id": "10", "who": None},
                {"party_id": "11", "who": None},
                {"party_id": "12", "who": None},
                {"party_id": "13", "who": None},
            ],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    t = common.CountFrequencyValues(
        column="who", table_config=table_config, max_proportion=0.50, keep_nulls=True
    )
    with pytest.raises(
        expected_exception=DataTestFailure,
        match="1 column values",
    ):
        t(test_connection, request)  # should succeed


def test_count_frequency_values_group_by(test_connection, create_test_table, request):
    schema = {
        "party_id": String(nullable=False),
        "country": String(nullable=False),
        "who": String(nullable=True),
    }

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {"party_id": "1", "country": "UK", "who": "foo"},
                {"party_id": "2", "country": "UK", "who": "foo"},
                {"party_id": "3", "country": "UK", "who": "foo"},
                {"party_id": "6", "country": "UK", "who": "baz"},
                {"party_id": "7", "country": "UK", "who": "baz"},
                {"party_id": "8", "country": "US", "who": "baz"},
                {"party_id": "9", "country": "US", "who": "other"},
                {"party_id": "10", "country": "US", "who": "foo"},
                {"party_id": "11", "country": "US", "who": "foo"},
                {"party_id": "12", "country": "US", "who": "baz"},
                {"party_id": "13", "country": "US", "who": "baz"},
            ],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    t = common.CountFrequencyValues(
        column="who",
        table_config=table_config,
        max_proportion=0.50,
    )
    # Sanity check: should pass if country not specified
    t(test_connection, request)

    t = common.CountFrequencyValues(
        column="who",
        table_config=table_config,
        max_proportion=0.50,
        group_by=["country"],
    )
    with pytest.raises(
        expected_exception=DataTestFailure,
        match="2 column values",
    ):
        t(test_connection, request)  # should succeed


def test_count_frequency_values_group_by_expression_failure(
    test_connection, create_test_table, request
):
    schema = {
        "party_id": String(nullable=False),
        "units": Int64(nullable=False),
        "nanos": Int64(nullable=False),
        "who": String(nullable=False),
    }

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {"party_id": "1", "units": 1, "nanos": 1, "who": "foo"},
                {"party_id": "2", "units": 1, "nanos": 1, "who": "foo"},
                {"party_id": "3", "units": 1, "nanos": 1, "who": "foo"},
                {"party_id": "6", "units": 1, "nanos": 1, "who": "baz"},
                {"party_id": "7", "units": 1, "nanos": 1, "who": "baz"},
                {"party_id": "8", "units": 1, "nanos": 1, "who": "baz"},
                {"party_id": "9", "units": 1, "nanos": 1, "who": "other"},
                {"party_id": "10", "units": 1, "nanos": 1, "who": "foo"},
                {"party_id": "11", "units": 1, "nanos": 1, "who": "foo"},
                {"party_id": "12", "units": 1, "nanos": 1, "who": "baz"},
                {"party_id": "13", "units": 1, "nanos": 1, "who": "baz"},
            ],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    t = common.CountFrequencyValues(
        column="who",
        table_config=table_config,
        max_proportion=0.50,
    )
    # Sanity check: should pass if country not specified
    t(test_connection, request)

    t = common.CountFrequencyValues(
        column=lambda t: t.units + t.nanos,
        table_config=table_config,
        max_proportion=0.50,
    )
    with pytest.raises(
        expected_exception=DataTestFailure,
        match="1 column values",
    ):
        t(test_connection, request)  # should succeed
