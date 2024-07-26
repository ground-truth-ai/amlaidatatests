import ibis
import pytest
from ibis.expr.datatypes import String

from amlaidatatests.exceptions import AMLAITestSeverity, DataTestFailure
from amlaidatatests.schema.base import ResolvedTableConfig, TableType
from amlaidatatests.tests import common


def test_max_proportion_group_by(test_connection, create_test_table, request):
    schema = {"account_id": String(nullable=False), "column": String(nullable=False)}

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {"account_id": "1", "column": "born"},
                {"account_id": "2", "column": "married"},
                {"account_id": "3", "column": "married"},
            ],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    # Check not all account_id have a value of married
    t = common.VerifyTypedValuePresence(
        table_config=table_config,
        group_by=["account_id"],
        max_proportion=1,
        column="column",
        value="married",
    )
    t(test_connection, request)  # should succeed


def test_max_proportion_group_by_fails(test_connection, create_test_table, request):
    schema = {"account_id": String(nullable=False), "column": String(nullable=False)}

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {"account_id": "1", "column": "married"},
                {"account_id": "1", "column": "born"},
                {"account_id": "2", "column": "married"},
                {"account_id": "3", "column": "married"},
            ],
            schema=schema,
        )
    )
    table_config = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    # Checks not all account_id have a value of married
    t = common.VerifyTypedValuePresence(
        table_config=table_config,
        group_by=["account_id"],
        max_proportion=1,
        column="column",
        value="married",
        severity=AMLAITestSeverity.ERROR,
    )
    with pytest.raises(DataTestFailure):
        t(test_connection, request)


def test_collar_proportion_group_by_fails_too_high(
    test_connection, create_test_table, request
):
    schema = {
        "transaction_id": String(nullable=False),
        "direction": String(nullable=False),
    }

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {"transaction_id": "1", "direction": "DEBIT"},
                {"transaction_id": "2", "direction": "DEBIT"},
                {"transaction_id": "3", "direction": "DEBIT"},
                {"transaction_id": "4", "direction": "DEBIT"},
                {"transaction_id": "5", "direction": "DEBIT"},
                {"transaction_id": "6", "direction": "DEBIT"},
                {"transaction_id": "7", "direction": "DEBIT"},
                {"transaction_id": "8", "direction": "CREDIT"},
                {"transaction_id": "9", "direction": "CREDIT"},
                {"transaction_id": "10", "direction": "CREDIT"},
            ],
            schema=schema,
        )
    )
    TABLE_CONFIG = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    # Checks not all account_id have a value of married
    t = common.VerifyTypedValuePresence(
        column="direction",
        table_config=TABLE_CONFIG,
        min_proportion=0.4,
        max_proportion=0.6,
        group_by=["transaction_id"],
        severity=AMLAITestSeverity.ERROR,
        value="DEBIT",
    )
    with pytest.raises(DataTestFailure):
        t(test_connection, request)


def test_collar_proportion_group_by_fails_too_low(
    test_connection, create_test_table, request
):
    schema = {
        "transaction_id": String(nullable=False),
        "direction": String(nullable=False),
    }

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {"transaction_id": "1", "direction": "DEBIT"},
                {"transaction_id": "2", "direction": "DEBIT"},
                {"transaction_id": "3", "direction": "DEBIT"},
                {"transaction_id": "4", "direction": "DEBIT"},
                {"transaction_id": "5", "direction": "DEBIT"},
                {"transaction_id": "6", "direction": "DEBIT"},
                {"transaction_id": "7", "direction": "DEBIT"},
                {"transaction_id": "8", "direction": "CREDIT"},
                {"transaction_id": "9", "direction": "CREDIT"},
                {"transaction_id": "10", "direction": "CREDIT"},
            ],
            schema=schema,
        )
    )
    TABLE_CONFIG = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    # Checks not all account_id have a value of married
    t = common.VerifyTypedValuePresence(
        column="direction",
        table_config=TABLE_CONFIG,
        min_proportion=0.4,
        max_proportion=0.6,
        group_by=["transaction_id"],
        severity=AMLAITestSeverity.ERROR,
        value="CREDIT",
    )
    with pytest.raises(DataTestFailure):
        t(test_connection, request)


def test_collar_proportion_group_by(test_connection, create_test_table, request):
    schema = {
        "transaction_id": String(nullable=False),
        "direction": String(nullable=False),
    }

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {"transaction_id": "internal_account_1", "direction": "DEBIT"},
                {"transaction_id": "internal_account_1", "direction": "DEBIT"},
                {"transaction_id": "internal_account_1", "direction": "DEBIT"},
                {"transaction_id": "internal_account_1", "direction": "DEBIT"},
                {"transaction_id": "5", "direction": "DEBIT"},
                {"transaction_id": "6", "direction": "DEBIT"},
                {"transaction_id": "7", "direction": "DEBIT"},
                {"transaction_id": "8", "direction": "CREDIT"},
                {"transaction_id": "9", "direction": "CREDIT"},
                {"transaction_id": "10", "direction": "CREDIT"},
            ],
            schema=schema,
        )
    )
    TABLE_CONFIG = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    # Checks not all account_id have a value of married
    t = common.VerifyTypedValuePresence(
        column="direction",
        table_config=TABLE_CONFIG,
        min_proportion=0.4,
        max_proportion=0.6,
        group_by=["transaction_id"],
        severity=AMLAITestSeverity.ERROR,
        value="CREDIT",
    )
    t(test_connection, request)


def test_collar_proportion_group_by_where(test_connection, create_test_table, request):
    schema = {
        "transaction_id": String(nullable=False),
        "direction": String(nullable=False),
    }

    tbl = create_test_table(
        ibis.memtable(
            data=[
                {"transaction_id": "internal_account_1", "direction": "DEBIT"},
                {"transaction_id": "internal_account_2", "direction": "DEBIT"},
                {"transaction_id": "internal_account_3", "direction": "DEBIT"},
                {"transaction_id": "internal_account_4", "direction": "DEBIT"},
                {"transaction_id": "5", "direction": "DEBIT"},
                {"transaction_id": "6", "direction": "DEBIT"},
                {"transaction_id": "7", "direction": "DEBIT"},
                {"transaction_id": "8", "direction": "CREDIT"},
                {"transaction_id": "9", "direction": "CREDIT"},
                {"transaction_id": "10", "direction": "CREDIT"},
            ],
            schema=schema,
        )
    )
    TABLE_CONFIG = ResolvedTableConfig(
        name=tbl, table=ibis.table(name=tbl, schema=schema), table_type=TableType.EVENT
    )

    # Checks not all account_id have a value of married
    t = common.VerifyTypedValuePresence(
        column="direction",
        table_config=TABLE_CONFIG,
        min_proportion=0.4,
        max_proportion=0.6,
        group_by=["transaction_id"],
        compare_group_by_where=lambda t: t["transaction_id"]
        .like("%internal_account%")
        .negate(),
        severity=AMLAITestSeverity.ERROR,
        value="CREDIT",
    )
    t(test_connection, request)
