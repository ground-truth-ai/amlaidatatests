import datetime

import ibis
import pytest
from ibis.expr.datatypes import Boolean, String, Timestamp

from amlaidatatests.exceptions import DataTestFailure
from amlaidatatests.schema.base import ResolvedTableConfig
from amlaidatatests.tests import common

RISK_SCHEMA = {
    "risk_case_id": String(nullable=False),
    "party_id": String(nullable=False),
    "type": String(nullable=False),
    "event_time": Timestamp(nullable=False, timezone="UTC"),
}

TRANSACTION_SCHEMA = ibis.Schema(
    fields={
        "transaction_id": String(nullable=False),
        "account_id": String(nullable=False),
        # "validity_start_time": Timestamp(),
        # "is_entity_deleted": Boolean(),
        "book_time": Timestamp(timezone="UTC"),
    }
)

LINK_SCHEMA = ibis.Schema(
    fields={
        "party_id": String(nullable=False),
        "account_id": String(nullable=False),
        "validity_start_time": Timestamp(nullable=False, timezone="UTC"),
        "is_entity_deleted": Boolean(),
    }
)


def test_has_transactions(test_connection, create_test_table, request):
    risk_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "risk_case_id": "1",
                    "party_id": "1",
                    "type": "AML_PROCESS_START",
                    "event_time": datetime.datetime(
                        2020, 1, 1, hour=0, tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "risk_case_id": "1",
                    "party_id": "1",
                    "type": "AML_SAR",
                    "event_time": datetime.datetime(
                        2020, 1, 2, hour=1, tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "risk_case_id": "1",
                    "party_id": "1",
                    "type": "AML_EXIT",
                    "event_time": datetime.datetime(
                        2020, 1, 2, hour=2, tzinfo=datetime.timezone.utc
                    ),
                },
            ],
            schema=RISK_SCHEMA,
        )
    )

    risk_table_config = ResolvedTableConfig(
        name=risk_tbl, table=ibis.table(name=risk_tbl, schema=RISK_SCHEMA)
    )

    link_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "party_id": "1",
                    "account_id": "20",
                    "validity_start_time": datetime.datetime(
                        2018, 1, 1, tzinfo=datetime.timezone.utc
                    ),
                    "is_entity_deleted": False,
                },
            ],
            schema=LINK_SCHEMA,
        )
    )

    link_table_config = ResolvedTableConfig(
        name=link_tbl,
        table=ibis.table(name=link_tbl, schema=LINK_SCHEMA),
        entity_keys=["account_id", "party_id"],
    )

    txn_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "transaction_id": "1",
                    "account_id": "20",
                    "book_time": datetime.datetime(
                        2019, 6, 2, hour=1, tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "transaction_id": "2",
                    "account_id": "20",
                    "book_time": datetime.datetime(
                        2019, 6, 2, hour=2, tzinfo=datetime.timezone.utc
                    ),
                },
            ],
            schema=TRANSACTION_SCHEMA,
        )
    )

    transaction_table_config = ResolvedTableConfig(
        name=link_tbl, table=ibis.table(name=txn_tbl, schema=TRANSACTION_SCHEMA)
    )

    t = common.NoTransactionsWithinSuspiciousPeriod(
        table_config=risk_table_config,
        account_party_link_table_config=link_table_config,
        transaction_table_config=transaction_table_config,
    )

    t(test_connection, request)


def test_has_no_transactions(test_connection, create_test_table, request):
    risk_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "risk_case_id": "1",
                    "party_id": "1",
                    "type": "AML_PROCESS_START",
                    "event_time": datetime.datetime(
                        2020, 1, 1, hour=0, tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "risk_case_id": "1",
                    "party_id": "1",
                    "type": "AML_SAR",
                    "event_time": datetime.datetime(
                        2020, 1, 2, hour=1, tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "risk_case_id": "1",
                    "party_id": "1",
                    "type": "AML_EXIT",
                    "event_time": datetime.datetime(
                        2020, 1, 2, hour=2, tzinfo=datetime.timezone.utc
                    ),
                },
            ],
            schema=RISK_SCHEMA,
        )
    )

    risk_table_config = ResolvedTableConfig(
        name=risk_tbl, table=ibis.table(name=risk_tbl, schema=RISK_SCHEMA)
    )

    link_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "party_id": "1",
                    "account_id": "20",
                    "validity_start_time": datetime.datetime(
                        2018, 1, 1, tzinfo=datetime.timezone.utc
                    ),
                    "is_entity_deleted": False,
                },
            ],
            schema=LINK_SCHEMA,
        )
    )

    link_table_config = ResolvedTableConfig(
        name=link_tbl,
        table=ibis.table(name=link_tbl, schema=LINK_SCHEMA),
        entity_keys=["account_id", "party_id"],
    )

    txn_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "transaction_id": "1",
                    "account_id": "20",
                    "book_time": datetime.datetime(
                        2024, 1, 2, hour=1, tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "transaction_id": "2",
                    "account_id": "20",
                    "book_time": datetime.datetime(
                        2024, 1, 2, hour=2, tzinfo=datetime.timezone.utc
                    ),
                },
            ],
            schema=TRANSACTION_SCHEMA,
        )
    )

    transaction_table_config = ResolvedTableConfig(
        name=link_tbl, table=ibis.table(name=txn_tbl, schema=TRANSACTION_SCHEMA)
    )

    t = common.NoTransactionsWithinSuspiciousPeriod(
        table_config=risk_table_config,
        account_party_link_table_config=link_table_config,
        transaction_table_config=transaction_table_config,
    )

    with pytest.raises(DataTestFailure):
        t(test_connection, request)


def test_has_transactions_in_one_account_not_linked(
    test_connection, create_test_table, request
):
    # Check transactions prior to the customer being attached to an account are ignored

    risk_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "risk_case_id": "1",
                    "party_id": "1",
                    "type": "AML_PROCESS_START",
                    "event_time": datetime.datetime(
                        2020, 1, 1, hour=0, tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "risk_case_id": "2",
                    "party_id": "1",
                    "type": "AML_PROCESS_START",
                    "event_time": datetime.datetime(
                        2020, 1, 1, hour=0, tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "risk_case_id": "1",
                    "party_id": "1",
                    "type": "AML_SAR",
                    "event_time": datetime.datetime(
                        2020, 1, 2, hour=1, tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "risk_case_id": "2",
                    "party_id": "1",
                    "type": "AML_EXIT",
                    "event_time": datetime.datetime(
                        2020, 1, 2, hour=2, tzinfo=datetime.timezone.utc
                    ),
                },
            ],
            schema=RISK_SCHEMA,
        )
    )

    risk_table_config = ResolvedTableConfig(
        name=risk_tbl, table=ibis.table(name=risk_tbl, schema=RISK_SCHEMA)
    )

    link_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "party_id": "1",
                    "account_id": "20",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, tzinfo=datetime.timezone.utc
                    ),
                    "is_entity_deleted": False,
                },
                {
                    "party_id": "1",
                    "account_id": "30",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, tzinfo=datetime.timezone.utc
                    ),
                    "is_entity_deleted": False,
                },
            ],
            schema=LINK_SCHEMA,
        )
    )

    link_table_config = ResolvedTableConfig(
        name=link_tbl,
        table=ibis.table(name=link_tbl, schema=LINK_SCHEMA),
        entity_keys=["account_id", "party_id"],
    )

    txn_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "transaction_id": "1",
                    "account_id": "20",
                    "book_time": datetime.datetime(
                        2019, 6, 2, hour=1, tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "transaction_id": "2",
                    "account_id": "20",
                    "book_time": datetime.datetime(
                        2019, 6, 2, hour=2, tzinfo=datetime.timezone.utc
                    ),
                },
            ],
            schema=TRANSACTION_SCHEMA,
        )
    )

    transaction_table_config = ResolvedTableConfig(
        name=link_tbl, table=ibis.table(name=txn_tbl, schema=TRANSACTION_SCHEMA)
    )

    t = common.NoTransactionsWithinSuspiciousPeriod(
        table_config=risk_table_config,
        account_party_link_table_config=link_table_config,
        transaction_table_config=transaction_table_config,
    )
    # Transactions in accounts not yet associated with the customer
    # should pass
    with pytest.raises(DataTestFailure):
        t(test_connection, request)


def test_has_transactions_in_one_account_linked(
    test_connection, create_test_table, request
):
    # Check transactions prior to the customer being attached to an account are ignored

    risk_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "risk_case_id": "1",
                    "party_id": "1",
                    "type": "AML_PROCESS_START",
                    "event_time": datetime.datetime(
                        2020, 1, 1, hour=0, tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "risk_case_id": "2",
                    "party_id": "1",
                    "type": "AML_PROCESS_START",
                    "event_time": datetime.datetime(
                        2020, 1, 1, hour=0, tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "risk_case_id": "1",
                    "party_id": "1",
                    "type": "AML_SAR",
                    "event_time": datetime.datetime(
                        2020, 1, 2, hour=1, tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "risk_case_id": "2",
                    "party_id": "1",
                    "type": "AML_EXIT",
                    "event_time": datetime.datetime(
                        2020, 1, 2, hour=2, tzinfo=datetime.timezone.utc
                    ),
                },
            ],
            schema=RISK_SCHEMA,
        )
    )

    risk_table_config = ResolvedTableConfig(
        name=risk_tbl, table=ibis.table(name=risk_tbl, schema=RISK_SCHEMA)
    )

    link_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "party_id": "1",
                    "account_id": "20",
                    "validity_start_time": datetime.datetime(
                        2019, 1, 1, tzinfo=datetime.timezone.utc
                    ),
                    "is_entity_deleted": False,
                },
                {
                    "party_id": "1",
                    "account_id": "30",
                    "validity_start_time": datetime.datetime(
                        2019, 1, 1, tzinfo=datetime.timezone.utc
                    ),
                    "is_entity_deleted": False,
                },
            ],
            schema=LINK_SCHEMA,
        )
    )

    link_table_config = ResolvedTableConfig(
        name=link_tbl,
        table=ibis.table(name=link_tbl, schema=LINK_SCHEMA),
        entity_keys=["account_id", "party_id"],
    )

    txn_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "transaction_id": "1",
                    "account_id": "20",
                    "book_time": datetime.datetime(
                        2019, 6, 2, hour=1, tzinfo=datetime.timezone.utc
                    ),
                },
                {
                    "transaction_id": "2",
                    "account_id": "20",
                    "book_time": datetime.datetime(
                        2019, 6, 2, hour=2, tzinfo=datetime.timezone.utc
                    ),
                },
            ],
            schema=TRANSACTION_SCHEMA,
        )
    )

    transaction_table_config = ResolvedTableConfig(
        name=link_tbl, table=ibis.table(name=txn_tbl, schema=TRANSACTION_SCHEMA)
    )

    t = common.NoTransactionsWithinSuspiciousPeriod(
        table_config=risk_table_config,
        account_party_link_table_config=link_table_config,
        transaction_table_config=transaction_table_config,
    )
    t(test_connection, request)
