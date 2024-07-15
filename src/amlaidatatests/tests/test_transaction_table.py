from amlaidatatests.utils import get_columns
import pytest

from amlaidatatests.base import AbstractColumnTest, AbstractTableTest
from amlaidatatests.schema.utils import resolve_table_config
from amlaidatatests.test_generators import (
    entity_columns,
    get_entity_mutation_tests,
    get_entity_tests,
    get_generic_table_tests,
    non_nullable_fields,
)
from amlaidatatests.tests import common

TABLE_CONFIG = resolve_table_config("transaction")


@pytest.mark.parametrize(
    "test", get_generic_table_tests(table_config=TABLE_CONFIG, max_rows_factor=50e9)
)
def test_table(connection, test: AbstractTableTest):
    test(connection=connection)


def test_primary_keys(connection):
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

@pytest.mark.parametrize("column", get_columns(TABLE_CONFIG))
def test_datetime_consistency(connection, column):
    test = common.ColumnTypeTest(table_config=TABLE_CONFIG, column=column)
    test(connection)


@pytest.mark.parametrize("column", non_nullable_fields(TABLE_CONFIG.table.schema()))
def test_non_nullable_fields(connection, column):
    test = common.FieldNeverNullTest(table_config=TABLE_CONFIG, column=column)
    test(connection)


def test_temporal_referential_integrity_account_party_link(connection):
    to_table_config = resolve_table_config("account_party_link")
    test = common.TemporalReferentialIntegrityTest(
        table_config=TABLE_CONFIG, to_table_config=to_table_config, key="account_id"
    )
    test(connection)



def test_referential_integrity_account_party_link(connection):
    to_table_config = resolve_table_config("account_party_link")
    test = common.ReferentialIntegrityTest(
        table_config=TABLE_CONFIG, to_table_config=to_table_config, keys=["account_id"]
    )
    test(connection)


@pytest.mark.parametrize(
    "test",
    get_entity_mutation_tests(table_config=TABLE_CONFIG, entity_ids=["transaction_id"]),
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
        ),
        common.ColumnValuesTest(
            column="direction", values=["DEBIT", "CREDIT"], table_config=TABLE_CONFIG
        ),
    ],
)
def test_column_values(connection, test):
    test(connection)


@pytest.mark.parametrize(
    "column",
    entity_columns(schema=TABLE_CONFIG.table.schema(), entity_types=["CurrencyValue"]),
)
@pytest.mark.parametrize(
    "test", get_entity_tests(table_config=TABLE_CONFIG, entity_name="CurrencyValue")
)
def test_currency_value_entity(connection, column, test: AbstractColumnTest):
    test(connection=connection, prefix=column)
