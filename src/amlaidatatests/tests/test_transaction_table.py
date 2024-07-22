"""Tests for the transaction table"""

import pytest

from amlaidatatests.base import AbstractColumnTest, AbstractTableTest
from amlaidatatests.config import cfg
from amlaidatatests.exceptions import AMLAITestSeverity
from amlaidatatests.schema.utils import resolve_table_config
from amlaidatatests.test_generators import (
    get_entities,
    get_entity_mutation_tests,
    get_entity_tests,
    get_generic_table_tests,
    get_non_nullable_fields,
    timestamp_field_tests,
)
from amlaidatatests.tests import common
from amlaidatatests.utils import get_columns

TABLE_CONFIG = resolve_table_config("transaction")
TABLE = TABLE_CONFIG.table

TXN_TYPES = ["WIRE", "CASH", "CHECK", "CARD"]


@pytest.mark.parametrize(
    "test", get_generic_table_tests(table_config=TABLE_CONFIG, expected_max_rows=50e9)
)
def test_table(connection, test: AbstractTableTest):
    test(connection=connection)


def test_PK003_primary_keys(connection):
    test = common.PrimaryKeyColumnsTest(
        table_config=TABLE_CONFIG,
        unique_combination_of_columns=["transaction_id", "validity_start_time"],
    )
    test(connection)


# For each column in the schema, check all columns are all present
@pytest.mark.parametrize("column", get_columns(TABLE_CONFIG))
def test_column_presence(connection, column: str):
    test = common.ColumnPresenceTest(table_config=TABLE_CONFIG, column=column)
    test(connection)


# For each column in the schema, check all columns are the correct type
@pytest.mark.parametrize("column", get_columns(TABLE_CONFIG))
def test_column_type(connection, column):
    test = common.ColumnTypeTest(table_config=TABLE_CONFIG, column=column)
    test(connection)


@pytest.mark.parametrize("column", get_non_nullable_fields(TABLE_CONFIG.table.schema()))
def test_non_nullable_fields(connection, column):
    test = common.FieldNeverNullTest(table_config=TABLE_CONFIG, column=column)
    test(connection)


@pytest.mark.parametrize("test", timestamp_field_tests(TABLE_CONFIG))
def test_timestamp_fields(connection, test: AbstractColumnTest):
    test(connection)


def test_RI011_temporal_referential_integrity_account_party_link(connection):
    to_table_config = resolve_table_config("account_party_link")
    test = common.TemporalReferentialIntegrityTest(
        table_config=TABLE_CONFIG, to_table_config=to_table_config, key="account_id"
    )
    test(connection)


def test_RI004_referential_integrity_account_party_link(connection):
    to_table_config = resolve_table_config("account_party_link")
    test = common.ReferentialIntegrityTest(
        table_config=TABLE_CONFIG, to_table_config=to_table_config, keys=["account_id"]
    )
    test(connection)


@pytest.mark.parametrize(
    "test",
    get_entity_mutation_tests(table_config=TABLE_CONFIG),
)
def test_entity_mutation_tests(connection, test: AbstractColumnTest):
    test(connection=connection)


@pytest.mark.parametrize(
    "test",
    [
        common.ColumnValuesTest(
            column="type",
            values=["WIRE", "CASH", "CHECK", "CARD", "OTHER"],
            table_config=TABLE_CONFIG,
            test_id="E005",
        ),
        common.ColumnValuesTest(
            column="direction",
            values=["DEBIT", "CREDIT"],
            table_config=TABLE_CONFIG,
            test_id="E006",
        ),
    ],
)
def test_column_values(connection, test):
    test(connection)


@pytest.mark.parametrize(
    "column",
    get_entities(item=TABLE_CONFIG.table.schema(), entity_types=["CurrencyValue"]),
)
@pytest.mark.parametrize(
    "test", get_entity_tests(table_config=TABLE_CONFIG, entity_name="CurrencyValue")
)
def test_currency_value_entity(connection, column, test: AbstractColumnTest):
    test(connection=connection, prefix=column)


@pytest.mark.parametrize(
    "test",
    [
        common.CountMatchingRows(
            column="book_time",
            table_config=TABLE_CONFIG,
            max_rows=0,
            expression=lambda t: t.book_time >= cfg().interval_end_date,
            severity=AMLAITestSeverity.WARN,
            test_id="DT008",
        )
    ],
)
def test_date_consistency(connection, test):
    test(connection)


@pytest.mark.parametrize(
    "test",
    [
        common.CountFrequencyValues(
            column="book_time",
            table_config=TABLE_CONFIG,
            max_number=1e6,
            severity=AMLAITestSeverity.WARN,
            test_id="P034",
        ),
        common.ColumnCardinalityTest(
            column="source_system",
            table_config=TABLE_CONFIG,
            max_number=500,
            severity=AMLAITestSeverity.WARN,
            test_id="P021",
        ),
        common.CountFrequencyValues(
            column="account_id",
            table_config=TABLE_CONFIG,
            max_number=5e9,
            severity=AMLAITestSeverity.ERROR,
            test_id="P026",
        ),
        common.CountFrequencyValues(
            column="account_id",
            table_config=TABLE_CONFIG,
            max_number=1e9,
            severity=AMLAITestSeverity.WARN,
            test_id="P027",
        ),
        *[
            common.VerifyTypedValuePresence(
                column="type",
                table_config=TABLE_CONFIG,
                min_number=1,
                group_by=["transaction_id"],
                test_id="P022",
                value=typ,
            )
            for typ in TXN_TYPES
        ],
        # Implicitly checks CREDIT:DEBIT ratio - since these are
        # the only two values in the colum
        common.VerifyTypedValuePresence(
            column="direction",
            table_config=TABLE_CONFIG,
            min_proportion=0.4,
            max_proportion=0.6,
            group_by=["transaction_id"],
            test_id="P024",
            value="DEBIT",
        ),
        # Implicitly checks CREDIT:DEBIT ratio - since these are
        # the only two values in the colum
        common.VerifyTypedValuePresence(
            column="direction",
            table_config=TABLE_CONFIG,
            min_proportion=0.2,
            max_proportion=0.8,
            group_by=["transaction_id"],
            test_id="P025",
            severity=AMLAITestSeverity.ERROR,
            value="DEBIT",
        ),
        common.ColumnCardinalityTest(
            column="transaction_id",
            table_config=TABLE_CONFIG,
            max_number=10e6,
            group_by=["account_id", "counterparty_account.account_id"],
            severity=AMLAITestSeverity.ERROR,
            test_id="P028",
        ),
        common.ColumnCardinalityTest(
            column="transaction_id",
            table_config=TABLE_CONFIG,
            max_number=5e6,
            group_by=["account_id", "counterparty_account.account_id"],
            severity=AMLAITestSeverity.WARN,
            test_id="P029",
        ),
        # TODO: This only checks the nanos field, not all the columns
        common.CountFrequencyValues(
            column="normalized_booked_amount.nanos",
            table_config=TABLE_CONFIG,
            proportion=0.01,
            group_by=["type"],
            test_id="P051",
        ),
        common.TemporalProfileTest(
            column="book_time",
            table_config=TABLE_CONFIG,
            period="MONTH",
            threshold=0.90,
            test_id="P032",
        ),
        common.TemporalProfileTest(
            column="book_time",
            table_config=TABLE_CONFIG,
            period="MONTH",
            threshold=0.75,
            test_id="P033",
            severity=AMLAITestSeverity.WARN,
        ),
    ],
)
def test_profiling(connection, test):
    test(connection)
