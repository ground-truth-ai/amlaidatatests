"""Tests for the account_party_link table"""

import pytest

from amlaidatatests.base import AbstractColumnTest, AbstractTableTest
from amlaidatatests.exceptions import AMLAITestSeverity
from amlaidatatests.schema.utils import resolve_table_config
from amlaidatatests.test_generators import (
    get_entity_mutation_tests,
    get_generic_table_tests,
    non_nullable_field_tests,
    timestamp_field_tests,
)
from amlaidatatests.tests import common
from amlaidatatests.utils import get_columns

TABLE_CONFIG = resolve_table_config("account_party_link")


def test_RI001_referential_integrity_party(connection, request):
    # A warning here means that there are parties without linked accounts
    to_table_config = resolve_table_config("party")
    test = common.ReferentialIntegrityTest(
        table_config=TABLE_CONFIG,
        to_table_config=to_table_config,
        keys=["party_id"],
        test_id="RI001",
    )
    test(connection, request)


@pytest.mark.parametrize(
    "test", get_generic_table_tests(table_config=TABLE_CONFIG, expected_max_rows=500e6)
)
def test_table(connection, test: AbstractTableTest, request):
    test(connection=connection, request=request)


def test_PK002_primary_keys(connection, request):
    test = common.PrimaryKeyColumnsTest(
        table_config=TABLE_CONFIG,
        unique_combination_of_columns=["party_id", "account_id", "validity_start_time"],
        test_id="PK002",
    )
    test(connection, request)


# For each column in the schema, check all columns are all present
@pytest.mark.parametrize("column", get_columns(TABLE_CONFIG))
def test_F003_column_presence(connection: common.BaseBackend, column: str, request):
    test = common.ColumnPresenceTest(
        table_config=TABLE_CONFIG, column=column, test_id="F003"
    )
    test(connection, request)


# For each column in the schema, check all columns are the correct type
@pytest.mark.parametrize("column", get_columns(TABLE_CONFIG))
def test_F004_column_type(connection, column, request):
    test = common.ColumnTypeTest(
        table_config=TABLE_CONFIG, column=column, test_id="F004"
    )
    test(connection, request)


@pytest.mark.parametrize(
    "test",
    [
        common.ColumnValuesTest(
            allowed_values=[
                "PRIMARY_HOLDER",
                "SECONDARY_HOLDER",
                "SUPPLEMENTARY_HOLDER",
            ],
            column="role",
            table_config=TABLE_CONFIG,
            test_id="E004",
        )
    ],
)
def test_column_values(connection, test, request):
    test(connection, request)


@pytest.mark.parametrize(
    "test",
    get_entity_mutation_tests(table_config=TABLE_CONFIG),
)
def test_entity_mutation_tests(connection, test: AbstractColumnTest, request):
    test(connection=connection, request=request)


def test_RI009_temporal_referential_integrity_party(connection, request):
    # A warning here means that there are parties without linked accounts
    to_table_config = resolve_table_config("party")
    test = common.TemporalReferentialIntegrityTest(
        table_config=TABLE_CONFIG,
        to_table_config=to_table_config,
        key="party_id",
        test_id="RI009",
    )
    test(connection, request)


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
def test_transaction_referential_integrity(connection, test, request):
    """Tests referential integrity between the"""
    test(connection, request)


@pytest.mark.parametrize("test", non_nullable_field_tests(TABLE_CONFIG))
def test_non_nullable_fields(connection, test, request):
    test(connection, request)


@pytest.mark.parametrize("test", timestamp_field_tests(TABLE_CONFIG))
def test_timestamp_fields(connection, test, request):
    test(connection, request)


@pytest.mark.parametrize(
    "test",
    [
        common.ColumnCardinalityTest(
            column="account_id",
            table_config=TABLE_CONFIG,
            max_number=60000,
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
            max_number=60000,
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
def test_profiling(connection, test, request):
    test(connection, request)
