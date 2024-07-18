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
        test_id="RI010",
    )
    test(connection)


@pytest.mark.parametrize(
    "test",
    [
        common.ReferentialIntegrityTest(
            table_config=TABLE_CONFIG,
            to_table_config=resolve_table_config("transaction"),
            keys=["account_id"],
            severity=AMLAITestSeverity.WARN,
            max_proportion=0.05,
            test_id="P031",
        ),
        common.ReferentialIntegrityTest(
            table_config=TABLE_CONFIG,
            to_table_config=resolve_table_config("transaction"),
            keys=["account_id"],
            severity=AMLAITestSeverity.ERROR,
            max_proportion=0.20,
            test_id="P030",
        ),
    ],
)
def test_transaction_referential_integrity(connection, test):
    """Tests referential integrity between the"""
    test(connection)


@pytest.mark.parametrize(
    "test",
    [
        common.ColumnCardinalityTest(
            column="account_id",
            table_config=TABLE_CONFIG,
            max_number=10000,
            severity=AMLAITestSeverity.ERROR,
            group_by=["party_id"],
            test_id="P015",
        ),
        common.ColumnCardinalityTest(
            column="account_id",
            table_config=TABLE_CONFIG,
            max_number=1000,
            severity=AMLAITestSeverity.WARN,
            group_by=["party_id"],
            test_id="P016",
        ),
        common.ColumnCardinalityTest(
            column="party_id",
            table_config=TABLE_CONFIG,
            max_number=10000,
            severity=AMLAITestSeverity.ERROR,
            group_by=["account_id"],
            test_id="P017",
        ),
        common.ColumnCardinalityTest(
            column="party_id",
            table_config=TABLE_CONFIG,
            max_number=5000,
            severity=AMLAITestSeverity.WARN,
            group_by=["account_id"],
            test_id="P018",
        ),
        common.ColumnCardinalityTest(
            column="source_system",
            table_config=TABLE_CONFIG,
            max_number=500,
            severity=AMLAITestSeverity.WARN,
            test_id="P019",
        ),
    ],
)
def test_profiling(connection, test):
    test(connection)
