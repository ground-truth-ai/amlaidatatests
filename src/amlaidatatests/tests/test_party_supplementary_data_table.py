"""Tests for the party_supplementary_data table"""

import pytest

from amlaidatatests.exceptions import AMLAITestSeverity
from amlaidatatests.schema.utils import resolve_table_config
from amlaidatatests.test_generators import (
    get_entity_mutation_tests,
    get_generic_table_tests,
    non_nullable_field_tests,
    timestamp_field_tests,
)
from amlaidatatests.tests import common

TABLE_CONFIG = resolve_table_config("party_supplementary_data")


# Don't error or warn on an empty party_supplementary_data table
@pytest.mark.parametrize(
    "test", get_generic_table_tests(table_config=TABLE_CONFIG, expected_max_rows=1e12)
)
def test_table(connection, test, request):
    test(connection=connection, request=request)


def test_PK005_primary_keys(connection, request):
    test = common.PrimaryKeyColumnsTest(
        table_config=TABLE_CONFIG,
        unique_combination_of_columns=[
            "party_supplementary_data_id",
            "party_id",
            "validity_start_time",
        ],
        test_id="PK005",
    )
    test(connection, request)


@pytest.mark.parametrize(
    "test",
    get_entity_mutation_tests(table_config=TABLE_CONFIG),
)
def test_entity_mutation_tests(connection, test, request):
    test(connection=connection, request=request)


# For each column in the schema, check all columns are all present
@pytest.mark.parametrize("column", TABLE_CONFIG.table.schema().fields.keys())
def test_F003_column_presence(connection, column, request):
    test = common.ColumnPresenceTest(
        table_config=TABLE_CONFIG, column=column, test_id="F003"
    )
    test(connection, request)


# For each column in the schema, check all columns are the correct type
@pytest.mark.parametrize("column", TABLE_CONFIG.table.schema().fields.keys())
def test_F004_column_type(connection, column, request):
    test = common.ColumnTypeTest(
        table_config=TABLE_CONFIG, column=column, test_id="F004"
    )
    test(connection, request)


# Validate all fields marked in the schema as being non-nullable are
# non-nullable. This is in addition to the schema level tests, since it's not
# possible to enforce an embedded struct is non-nullable.


@pytest.mark.parametrize("test", non_nullable_field_tests(TABLE_CONFIG))
def test_non_nullable_fields(connection, test, request):
    test(connection, request)


@pytest.mark.parametrize("test", timestamp_field_tests(TABLE_CONFIG))
def test_timestamp_fields(connection, test, request):
    test(connection, request)


def test_RI006_referential_integrity_party(connection, request):
    to_table_config = resolve_table_config("party")
    test = common.ReferentialIntegrityTest(
        table_config=TABLE_CONFIG,
        to_table_config=to_table_config,
        keys=["party_id"],
        test_id="RI006",
    )
    test(connection, request)


def test_RI014_temporal_referential_integrity_party_supplementary_table(
    connection, request
):
    # A warning here means that there are parties without linked accounts
    to_table_config = resolve_table_config("party")
    test = common.TemporalReferentialIntegrityTest(
        table_config=TABLE_CONFIG,
        to_table_config=to_table_config,
        key="party_id",
        severity=AMLAITestSeverity.WARN,
        test_id="RI014",
    )
    test(connection, request)


@pytest.mark.parametrize(
    "test",
    [
        common.ColumnCardinalityTest(
            column="source_system",
            table_config=TABLE_CONFIG,
            max_number=500,
            severity=AMLAITestSeverity.WARN,
            test_id="P037",
        ),
        common.ColumnCardinalityTest(
            column="party_supplementary_data_id",
            table_config=TABLE_CONFIG,
            group_by=["party_id"],
            max_number=100,
            severity=AMLAITestSeverity.ERROR,
            test_id="P036",
        ),
    ],
)
def test_profiling(connection, test, request):
    test(connection, request)


def test_P035_psd_id_consistency(connection, request):
    test = common.ConsistentIDsPerColumn(
        table_config=TABLE_CONFIG,
        column="party_id",
        id_to_verify="party_supplementary_data_id",
        test_id="P035",
    )
    test(connection, request)


if __name__ == "__main__":
    retcode = pytest.main()
