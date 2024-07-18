#!/usr/bin/env python

from amlaidatatests.config import cfg
import ibis
import pytest

from amlaidatatests.base import AMLAITestSeverity, AbstractTableTest
from amlaidatatests.schema.utils import resolve_table_config
from amlaidatatests.test_generators import get_generic_table_tests, non_nullable_fields
from amlaidatatests.tests import common

TABLE_CONFIG = resolve_table_config("risk_case_event")
TABLE = TABLE_CONFIG.table


@pytest.mark.parametrize(
    "test", get_generic_table_tests(table_config=TABLE_CONFIG, max_rows_factor=10e6)
)
def test_table(connection, test: AbstractTableTest):
    test(connection=connection)


def test_primary_keys(connection):
    test = common.PrimaryKeyColumnsTest(
        table_config=TABLE_CONFIG, unique_combination_of_columns=["risk_case_event_id"]
    )
    test(connection)


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


# Validate all fields marked in the schema as being non-nullable are
# non-nullable. This is in addition to the schema level tests, since it's not
# possible to enforce an embedded struct is non-nullable.


@pytest.mark.parametrize("column", non_nullable_fields(TABLE_CONFIG.table.schema()))
def test_non_nullable_fields(connection, column):
    test = common.FieldNeverNullTest(table_config=TABLE_CONFIG, column=column)
    test(connection)


@pytest.mark.parametrize(
    "test",
    [
        common.ColumnValuesTest(
            values=[
                "AML_SUSPICIOUS_ACTIVITY_START",
                "AML_SUSPICIOUS_ACTIVITY_END",
                "AML_PROCESS_START",
                "AML_PROCESS_END",
                "AML_ALERT_GOOGLE",
                "AML_ALERT_LEGACY",
                "AML_ALERT_ADHOC",
                "AML_ALERT_EXPLORATORY",
                "AML_SAR",
                "AML_EXTERNAL",
                "AML_EXIT",
            ],
            table_config=TABLE_CONFIG,
            column="type",
        )
    ],
)
def test_column_values(connection, test):
    test(connection)


def test_referential_integrity_party(connection):
    to_table_config = resolve_table_config("party")
    test = common.ReferentialIntegrityTest(
        table_config=TABLE_CONFIG, to_table_config=to_table_config, keys=["party_id"]
    )
    test(connection)


w = ibis.window(
    group_by=[TABLE.risk_case_id, TABLE.party_id], order_by=TABLE.event_time
)


@pytest.mark.parametrize(
    "test",
    [
        common.NoMatchingRows(
            column="event_time",
            table_config=TABLE_CONFIG,
            expression=lambda: TABLE.event_time >= cfg().interval_end_date,
            severity=AMLAITestSeverity.ERROR,
        ),
    ],
)
def test_date_consistency(connection, test):
    test(connection)


def test_event_order(connection):
    t = common.EventOrder(
        time_column="event_time",
        column="type",
        table_config=TABLE_CONFIG,
        events=["AML_PROCESS_START", "AML_SAR", "AML_EXIT", "AML_PROCESS_END"],
        severity=AMLAITestSeverity.ERROR,
    )
    t(connection)


@pytest.mark.parametrize(
    "test",
    [
        common.ColumnCardinalityTest(
            column="type",
            table_config=TABLE_CONFIG,
            group_by=["party_id", "risk_case_event_id"],
            where=lambda t: t["type"] == "AML_SUSPICIOUS_ACTIVITY_START",
            max_number=1,
            severity=AMLAITestSeverity.ERROR,
            test_id="P060",
        ),
        common.ColumnCardinalityTest(
            column="type",
            table_config=TABLE_CONFIG,
            group_by=["party_id", "risk_case_event_id"],
            where=lambda t: t["type"] == "AML_SUSPICIOUS_ACTIVITY_END",
            max_number=1,
            severity=AMLAITestSeverity.ERROR,
            test_id="P061",
        ),
        common.ColumnCardinalityTest(
            column="type",
            table_config=TABLE_CONFIG,
            group_by=["party_id"],
            where=lambda t: t["type"] == "AML_EXIT",
            max_number=1,
            severity=AMLAITestSeverity.WARN,
            test_id="P066",
        ),
        common.ColumnCardinalityTest(
            column="risk_case_event_id",
            table_config=TABLE_CONFIG,
            group_by=["party_id"],
            max_number=1000,
            severity=AMLAITestSeverity.WARN,
            test_id="P056",
        ),
        common.ColumnCardinalityTest(
            column="risk_case_event_id",
            table_config=TABLE_CONFIG,
            group_by=["party_id"],
            max_number=5000,
            severity=AMLAITestSeverity.ERROR,
            test_id="P055",
        ),
        common.ColumnCardinalityTest(
            column="risk_case_event_id",
            table_config=TABLE_CONFIG,
            group_by=["risk_case_id"],
            max_number=1000,
            severity=AMLAITestSeverity.WARN,
            test_id="P054",
        ),
        common.ColumnCardinalityTest(
            column="risk_case_event_id",
            table_config=TABLE_CONFIG,
            group_by=["risk_case_id"],
            max_number=5000,
            severity=AMLAITestSeverity.ERROR,
            test_id="P053",
        ),
        common.CountFrequencyValues(
            column="event_time",
            table_config=TABLE_CONFIG,
            max_number=100e3,
            severity=AMLAITestSeverity.WARN,
            test_id="P038",
        ),
        common.VerifyTypedValuePresence(
            column="type",
            table_config=TABLE_CONFIG,
            min_number=1,
            group_by=["party_id"],
            value="AML_PROCESS_START",
            test_id="P039",
        ),
        common.VerifyTypedValuePresence(
            column="type",
            table_config=TABLE_CONFIG,
            min_number=1,
            group_by=["party_id"],
            value="AML_PROCESS_END",
            test_id="P040",
        ),
        common.VerifyTypedValuePresence(
            column="type",
            table_config=TABLE_CONFIG,
            min_number=1,
            group_by=["party_id"],
            value="AML_EXIT",
            test_id="P041",
        ),
        common.VerifyTypedValuePresence(
            column="type",
            table_config=TABLE_CONFIG,
            min_number=1,
            group_by=["party_id"],
            value="AML_SAR",
            test_id="P043",
        ),
        common.VerifyTypedValuePresence(
            column="type",
            table_config=TABLE_CONFIG,
            max_proportion=1,
            group_by=["party_id"],
            test_id="P042",
            value="AML_EXIT",
        ),
        common.VerifyTypedValuePresence(
            column="type",
            table_config=TABLE_CONFIG,
            max_proportion=1,
            group_by=["party_id"],
            test_id="P044",
            value="AML_EXIT",
        ),
        # TODO: This is not a precise test. We are comparing the number
        # of values of party_id where AML_PROCESS_START over
        # the number where AML_EXIT. This could be a co-incidence, but is unlikely
        common.VerifyTypedValuePresence(
            column="type",
            table_config=TABLE_CONFIG,
            min_proportion=1,
            group_by=["party_id", "risk_case_id"],
            where=lambda t: t.type == "AML_EXIT",
            test_id="P048",
            value="AML_PROCESS_START",
        ),
        common.CountFrequencyValues(
            column="type",
            table_config=TABLE_CONFIG,
            group_by=["risk_case_id", "party_id"],
            max_number=1,
            severity=AMLAITestSeverity.ERROR,
            having=lambda t: t["type"] == "AML_PROCESS_START",
            test_id="P045",
        ),
        common.CountFrequencyValues(
            column="type",
            table_config=TABLE_CONFIG,
            group_by=["risk_case_id", "party_id"],
            max_number=1,
            severity=AMLAITestSeverity.ERROR,
            having=lambda t: t["type"] == "AML_PROCESS_END",
            test_id="P046",
        ),
        common.CountFrequencyValues(
            column="type",
            table_config=TABLE_CONFIG,
            group_by=["risk_case_id", "party_id"],
            max_number=1,
            severity=AMLAITestSeverity.ERROR,
            having=lambda t: t["type"] == "AML_EXIT",
            test_id="P047",
        ),
        common.VerifyTypedValuePresence(
            column="type",
            table_config=TABLE_CONFIG,
            min_proportion=1,
            group_by=["party_id", "risk_case_id"],
            where=lambda t: t.type == "AML_SUSPICIOUS_ACTIVITY_END",
            test_id="P062",
            value="AML_SUSPICIOUS_ACTIVITY_START",
        ),
    ],
)
def test_profiling(connection, test):
    test(connection)


if __name__ == "__main__":
    retcode = pytest.main()
