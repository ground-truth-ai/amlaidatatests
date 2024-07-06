from amlaidatatests.schema.utils import get_unbound_table
from amlaidatatests.test_generators import (
    get_entity_mutation_tests,
    get_generic_table_tests,
)
from amlaidatatests.tests import common
from amlaidatatests.base import AbstractColumnTest, AbstractTableTest
import pytest

TABLE = get_unbound_table("account_party_link")


@pytest.mark.parametrize(
    "test", get_generic_table_tests(table=TABLE, max_rows_factor=500e6)
)
def test_table(connection, test: AbstractTableTest):
    test(connection=connection)


def test_primary_keys(connection):
    test = common.TestPrimaryKeyColumns(
        table=TABLE,
        unique_combination_of_columns=["party_id", "account_id", "validity_start_time"],
    )
    test(connection)


# For each column in the schema, check all columns are all present
@pytest.mark.parametrize("column", TABLE.schema().fields.keys())
def test_column_presence(connection: common.BaseBackend, column: str):
    test = common.TestColumnPresence(table=TABLE, column=column)
    test(connection)


# For each column in the schema, check all columns are the correct type
@pytest.mark.parametrize("column", TABLE.schema().fields.keys())
def test_column_type(connection, column):
    test = common.TestColumnType(table=TABLE, column=column)
    test(connection)


def test_referential_integrity(connection):
    to_table_obj = get_unbound_table("party")
    test = common.TestReferentialIntegrity(
        table=TABLE, to_table=to_table_obj, keys=["party_id"]
    )
    test(connection)


@pytest.mark.parametrize(
    "test",
    [
        common.TestColumnValues(
            values=["PRIMARY_HOLDER", "SECONDARY_HOLDER", "SUPPLEMENTARY_HOLDER"],
            column="role",
            table=TABLE,
        )
    ],
)
def test_column_values(connection, test):
    test(connection)


@pytest.mark.parametrize(
    "test",
    get_entity_mutation_tests(table=TABLE, primary_keys=["party_id", "account_id"]),
)
def test_entity_mutation_tests(connection, test: AbstractColumnTest):
    test(connection=connection)


def test_temporal_referential_integrity(connection):
    # A warning here means that there are parties without linked accounts
    to_table_obj = get_unbound_table("party")
    test = common.TestTemporalReferentialIntegrity(
        table=TABLE, to_table=to_table_obj, keys=["party_id"]
    )
    test(connection)
