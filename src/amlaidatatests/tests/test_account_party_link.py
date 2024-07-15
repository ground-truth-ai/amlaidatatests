from amlaidatatests.utils import get_columns
import pytest

from amlaidatatests.base import AbstractColumnTest, AbstractTableTest, AMLAITestSeverity
from amlaidatatests.schema.utils import resolve_table_config
from amlaidatatests.test_generators import (
    get_entity_mutation_tests,
    get_generic_table_tests,
)
from amlaidatatests.tests import common

TABLE_CONFIG = resolve_table_config("account_party_link")


@pytest.mark.parametrize(
    "test", get_generic_table_tests(table_config=TABLE_CONFIG, max_rows_factor=500e6)
)
def test_table(connection, test: AbstractTableTest):
    test(connection=connection)


def test_primary_keys(connection):
    test = common.PrimaryKeyColumnsTest(
        table_config=TABLE_CONFIG,
        unique_combination_of_columns=["party_id", "account_id", "validity_start_time"],
    )
    test(connection)


# For each column in the schema, check all columns are all present
@pytest.mark.parametrize("column", get_columns(TABLE_CONFIG))
def test_column_presence(connection: common.BaseBackend, column: str):
    test = common.ColumnPresenceTest(table_config=TABLE_CONFIG, column=column)
    test(connection)


# For each column in the schema, check all columns are the correct type
@pytest.mark.parametrize("column", get_columns(TABLE_CONFIG))
def test_column_type(connection, column):
    test = common.ColumnTypeTest(table_config=TABLE_CONFIG, column=column)
    test(connection)


@pytest.mark.parametrize(
    "test",
    [
        common.ColumnValuesTest(
            values=["PRIMARY_HOLDER", "SECONDARY_HOLDER", "SUPPLEMENTARY_HOLDER"],
            column="role",
            table_config=TABLE_CONFIG,
        )
    ],
)
def test_column_values(connection, test):
    test(connection)


@pytest.mark.parametrize(
    "test",
    get_entity_mutation_tests(
        table_config=TABLE_CONFIG, entity_ids=["party_id", "account_id"]
    ),
)
def test_entity_mutation_tests(connection, test: AbstractColumnTest):
    test(connection=connection)


def test_temporal_referential_integrity_party(connection):
    # A warning here means that there are parties without linked accounts
    to_table_config = resolve_table_config("party")
    test = common.TemporalReferentialIntegrityTest(
        table_config=TABLE_CONFIG, to_table_config=to_table_config, key="party_id"
    )
    test(connection)


def test_temporal_referential_integrity_transaction(connection):
    # A warning here means that there are parties without linked accounts
    to_table_config = resolve_table_config("transaction")
    test = common.TemporalReferentialIntegrityTest(
        table_config=TABLE_CONFIG,
        to_table_config=to_table_config,
        key="account_id",
        severity=AMLAITestSeverity.WARN,
    )
    test(connection)


def test_transaction_referential_integrity(connection):
    """ Tests referential integrity between the """
    to_table_config = resolve_table_config("transaction")
    test = common.ReferentialIntegrityTest(
        table_config=TABLE_CONFIG,
        to_table_config=to_table_config,
        keys=["account_id"],
        severity=AMLAITestSeverity.WARN,
    )
    test(connection)
