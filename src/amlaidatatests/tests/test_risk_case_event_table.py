"""Tests for the risk_case_event table"""

from typing import Optional

import ibis
import pytest
from ibis import _

from amlaidatatests.config import cfg
from amlaidatatests.exceptions import AMLAITestSeverity
from amlaidatatests.schema.utils import resolve_table_config
from amlaidatatests.test_generators import (
    get_generic_table_tests,
    non_nullable_field_tests,
    timestamp_field_tests,
)
from amlaidatatests.tests import common
from amlaidatatests.tests.common import NoTransactionsWithinSuspiciousPeriod

TABLE_CONFIG = resolve_table_config("risk_case_event")
TABLE = TABLE_CONFIG.table


@pytest.mark.parametrize(
    "test", get_generic_table_tests(table_config=TABLE_CONFIG, expected_max_rows=10e6)
)
def test_table(connection, test, request):
    test(connection=connection, request=request)


def test_PK004_primary_keys(connection, request):
    test = common.PrimaryKeyColumnsTest(
        table_config=TABLE_CONFIG,
        unique_combination_of_columns=["risk_case_event_id"],
        test_id="PK004",
    )
    test(connection, request)


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


@pytest.mark.parametrize("test", non_nullable_field_tests(TABLE_CONFIG))
def test_non_nullable_fields(connection, test, request):
    test(connection, request)


@pytest.mark.parametrize("test", timestamp_field_tests(TABLE_CONFIG))
def test_timestamp_fields(connection, test, request):
    test(connection, request)


@pytest.mark.parametrize(
    "test",
    [
        common.ColumnValuesTest(
            allowed_values=[
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
            test_id="E007",
        )
    ],
)
def test_column_values(connection, test, request):
    test(connection, request)


w = ibis.window(
    group_by=[TABLE.risk_case_id, TABLE.party_id], order_by=TABLE.event_time
)


@pytest.mark.parametrize(
    "test",
    [
        common.CountMatchingRows(
            column="event_time",
            table_config=TABLE_CONFIG,
            max_number=0,
            expression=lambda t: t.event_time >= ibis.today(),
            severity=AMLAITestSeverity.ERROR,
            test_id="DT011",
        ),
    ],
)
def test_date_consistency(connection, test, request):
    test(connection, request)


def test_DT014_event_order(connection, request):
    t = common.EventOrder(
        time_column="event_time",
        column="type",
        table_config=TABLE_CONFIG,
        events=["AML_PROCESS_START", "AML_SAR", "AML_EXIT", "AML_PROCESS_END"],
        severity=AMLAITestSeverity.ERROR,
        test_id="DT014",
        group_by=["risk_case_id", "party_id"],
    )
    t(connection, request)


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
        common.CountMatchingRows(
            column="type",
            table_config=TABLE_CONFIG,
            min_number=1,
            expression=lambda t: t["type"] == "AML_PROCESS_START",
            test_id="P039",
        ),
        common.CountMatchingRows(
            column="type",
            table_config=TABLE_CONFIG,
            min_number=1,
            expression=lambda t: t["type"] == "AML_PROCESS_END",
            test_id="P040",
        ),
        common.CountMatchingRows(
            column="type",
            table_config=TABLE_CONFIG,
            min_number=1,
            expression=lambda t: t["type"] == "AML_EXIT",
            test_id="P041",
        ),
        common.CountMatchingRows(
            column="type",
            table_config=TABLE_CONFIG,
            min_number=1,
            expression=lambda t: t["type"] == "AML_SAR",
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
        # TODO: This is not a precise test. We are comparing the number of
        # values of party_id, risk_case_id which have an AML_PROCESS_START over
        # the number which have an AML_EXIT. This could be a co-incidence, so we
        # test this in a few different ways
        common.VerifyTypedValuePresence(
            column="type",
            table_config=TABLE_CONFIG,
            min_proportion=1,
            group_by=["party_id", "risk_case_id"],
            compare_group_by_where=lambda t: t.type == "AML_EXIT",
            test_id="P048",
            value="AML_PROCESS_START",
        ),
        common.VerifyTypedValuePresence(
            column="type",
            table_config=TABLE_CONFIG,
            min_proportion=1,
            group_by=["party_id", "risk_case_id"],
            compare_group_by_where=lambda t: t.type == "AML_SAR",
            test_id="P068",
            value="AML_PROCESS_START",
        ),
        common.VerifyTypedValuePresence(
            column="type",
            table_config=TABLE_CONFIG,
            min_proportion=1,
            group_by=["party_id", "risk_case_id"],
            compare_group_by_where=lambda t: (t.type == "AML_SAR")
            | (t.type == "AML_EXIT"),
            test_id="P067",
            value="AML_PROCESS_START",
            # This errors in the API
            severity=AMLAITestSeverity.ERROR,
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
            compare_group_by_where=lambda t: t.type == "AML_SUSPICIOUS_ACTIVITY_END",
            test_id="P062",
            value="AML_SUSPICIOUS_ACTIVITY_START",
        ),
        NoTransactionsWithinSuspiciousPeriod(
            table_config=TABLE_CONFIG, lookback_period=365, test_id="P059"
        ),
    ],
)
def test_profiling(connection, test, request):
    test(connection, request)


def test_RI012_temporal_referential_integrity_party(connection, request):
    # A warning here means that there are parties without linked accounts
    to_table_config = resolve_table_config("party")
    test = common.TemporalReferentialIntegrityTest(
        table_config=TABLE_CONFIG,
        to_table_config=to_table_config,
        key="party_id",
        severity=AMLAITestSeverity.WARN,
        test_id="RI012",
    )
    test(connection, request)


def test_RI014_temporal_referential_integrity_party(connection, request):
    # A warning here means that there are parties without linked accounts
    to_table_config = resolve_table_config("party")
    test = common.TemporalReferentialIntegrityTest(
        table_config=TABLE_CONFIG,
        to_table_config=to_table_config,
        key="party_id",
        severity=AMLAITestSeverity.WARN,
        test_id="RI005",
    )
    test(connection, request)


if __name__ == "__main__":
    retcode = pytest.main()
