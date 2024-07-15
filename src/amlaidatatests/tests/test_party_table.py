#!/usr/bin/env python

import pytest

from amlaidatatests.base import AbstractColumnTest, AbstractTableTest, AMLAITestSeverity
from amlaidatatests.io import get_valid_region_codes
from amlaidatatests.schema.utils import resolve_table_config
from amlaidatatests.test_generators import (
    entity_columns,
    get_entity_mutation_tests,
    get_entity_tests,
    get_generic_table_tests,
    non_nullable_field_tests,
)
from amlaidatatests.tests import common

TABLE_CONFIG = resolve_table_config("party")
TABLE = TABLE_CONFIG.table


@pytest.mark.parametrize(
    "test", get_generic_table_tests(table_config=TABLE_CONFIG, max_rows_factor=50e9)
)
def test_table(connection, test: AbstractTableTest):
    test(connection=connection)


def test_primary_keys(connection):
    test = common.PrimaryKeyColumnsTest(
        table_config=TABLE_CONFIG,
        unique_combination_of_columns=["party_id", "validity_start_time"],
    )
    test(connection)


@pytest.mark.parametrize(
    "test",
    get_entity_mutation_tests(table_config=TABLE_CONFIG, entity_ids=["party_id"]),
)
def test_entity_mutation_tests(connection, test: AbstractColumnTest):
    test(connection=connection)


@pytest.mark.parametrize(
    "column",
    entity_columns(schema=TABLE_CONFIG.table.schema(), entity_types=["CurrencyValue"]),
)
@pytest.mark.parametrize(
    "test", get_entity_tests(table_config=TABLE_CONFIG, entity_name="CurrencyValue")
)
def test_currency_value_entity(connection, column, test: AbstractColumnTest):
    test(connection=connection, prefix=column)


# For each column in the schema, check all columns are all present
@pytest.mark.parametrize("column", TABLE_CONFIG.table.schema().fields.keys())
def test_column_presence(connection, column: str):
    test = common.ColumnPresenceTest(table_config=TABLE_CONFIG, column=column)
    test(connection)


# For each column in the schema, check all columns are the correct type
@pytest.mark.parametrize("column", TABLE_CONFIG.table.schema().fields.keys())
def test_column_type(connection, column):
    test = common.ColumnTypeTest(table_config=TABLE_CONFIG, column=column)
    test(connection)


# Validate all fields marked in the schema as being non-nullable are non-nullable. This is in addition
# to the schema level tests, since it's not possible to enforce an embedded struct is non-nullable.


@pytest.mark.parametrize("test", non_nullable_field_tests(TABLE_CONFIG))
def test_non_nullable_fields(connection, test: AbstractColumnTest):
    test(connection)


@pytest.mark.parametrize(
    "test",
    [
        common.ColumnValuesTest(
            column="type", values=["COMPANY", "CONSUMER"], table_config=TABLE_CONFIG
        ),
        common.ColumnValuesTest(
            column="civil_status_code",
            values=[
                "SINGLE",
                "LEGALLY_DIVORCED",
                "DIVORCED",
                "WIDOW",
                "STABLE_UNION",
                "SEPARATED",
                "UNKNOWN",
            ],
            table_config=TABLE_CONFIG,
        ),
        common.ColumnValuesTest(
            column="education_level_code",
            values=[
                "LESS_THAN_PRIMARY_EDUCATION",
                "PRIMARY_EDUCATION",
                "LOWER_SECONDARY_EDUCATION",
                "UPPER_SECONDARY_EDUCATION",
                "POST_SECONDARY_NON_TERTIARY_EDUCATION",
                "SHORT_CYCLE_TERTIARY_EDUCATION",
                "BACHELORS_OR_EQUIVALENT",
                "MASTERS_OR_EQUIVALENT",
                "DOCTORAL_OR_EQUIVALENT",
                "NOT_ELSEWHERE_CLASSIFIED",
                "UNKNOWN",
            ],
            table_config=TABLE_CONFIG,
        ),
        common.ColumnValuesTest(
            column="nationalities.region_code",
            values=get_valid_region_codes(),
            table_config=TABLE_CONFIG,
        ),
        common.ColumnValuesTest(
            column="residencies.region_code",
            values=get_valid_region_codes(),
            table_config=TABLE_CONFIG,
        ),
    ],
)
def test_column_values(connection, test):
    test(connection)


@pytest.mark.parametrize(
    "test",
    [
        common.NullIfTest(
            column="birth_date",
            table_config=TABLE_CONFIG,
            expression=TABLE.type == "COMPANY",
        ),
        common.NullIfTest(
            column="gender",
            table_config=TABLE_CONFIG,
            expression=TABLE.type == "COMPANY",
        ),
        common.NullIfTest(
            column="establishment_date",
            table_config=TABLE_CONFIG,
            expression=TABLE.type == "CONSUMER",
        ),
        common.NullIfTest(
            column="occupation",
            table_config=TABLE_CONFIG,
            expression=TABLE.type == "CONSUMER",
        ),
    ],
)
def test_null_if(connection, test):
    test(connection)


def test_referential_integrity(connection):
    # A warning here means that there are parties without linked accounts
    to_table_config = resolve_table_config("account_party_link")
    test = common.ReferentialIntegrityTest(
        table_config=TABLE_CONFIG,
        to_table_config=to_table_config,
        keys=["party_id"],
        severity=AMLAITestSeverity.WARN,
    )
    test(connection)


def test_referential_integrity_party_supplementary_table(connection):
    # A warning here means that there are parties without linked accounts
    to_table_config = resolve_table_config("party_supplementary_data")
    test = common.TemporalReferentialIntegrityTest(
        table_config=TABLE_CONFIG,
        to_table_config=to_table_config,
        key="party_id",
        severity=AMLAITestSeverity.WARN,
    )
    test(connection)


if __name__ == "__main__":
    retcode = pytest.main()
