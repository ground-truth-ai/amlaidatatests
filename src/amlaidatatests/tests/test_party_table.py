#!/usr/bin/env python

from typing import Callable, Tuple
from amlaidatatests.io import get_valid_region_codes
from amlaidatatests.schema.v1.common import entity_fields, get_entity_tests, non_nullable_fields
from amlaidatatests.schema.v1 import party_schema
from amlaidatatests.tests.fixtures import common_tests
import ibis
import pytest
from ..connection import connection



SUFFIX = "1234"

variables = {
    "suffix": SUFFIX
}


def get_table(t: str) -> ibis.Table:
    table = connection.table(t)
    return table


# class Validator(object):
    
#     def __init__(self, config: dict, tables_available: dict[str, str]):
#         """_summary_

#         Args:
#             config (dict): _description_
#             tables_available (dict[str, str]): _description_
#         """
#         self.config = config
#         self.tables_available = tables_available

#     def validate_available_tables(self):
#         for t in config["tables"]:
#             try:
#                 self.tables_available.get(t["name"])
#             except:
#                 pass

#     def validate(self):
#         pass

# class TestSampleWithScenarios:
#     scenarios = [scenario1, scenario2]

#     def test_demo1(self, attribute) -> None:
#         assert isinstance(attribute, str)

#     def test_demo2(self, attribute) -> None:
#         assert isinstance(attribute, str)

table = get_table("party_1234")

def pytest_generate_tests(metafunc):
    # called once per each test function
    # funcarglist = metafunc.cls.params[metafunc.function.__name__]
    # argnames = sorted(funcarglist[0])
    # metafunc.parametrize(
    #     # argnames, [[funcargs[name] for name in argnames] for funcargs in funcarglist]
    # )
    if "test_schema_entities" in metafunc.fixturenames:
        fields = entity_fields(party_schema)
        tests: list[Tuple[str, Callable]] = []
        test_ids: list[str] = []

        for column_name, entity_name in fields.items():
            entity_tests_objs = get_entity_tests(entity_name)
            tests += [(f"{column_name}.{t.test_field}", t.test) for t in entity_tests_objs]
            test_ids += [f"({entity_name})-{column_name}-{t.test_name}" for t in entity_tests_objs]

        metafunc.parametrize(["column", "test"], tests, ids=test_ids)

def test_entities_in_schema(test_schema_entities, column, test):
    test(column=column, table=table)

def test_unique_combination_of_columns(unique_combination_of_columns):
    unique_combination_of_columns(table=table, unique_combination_of_columns=["party_id", "validity_start_time"])

# For each column in the schema, check all columns are all present
@pytest.mark.parametrize("column", party_schema.fields.keys())
def test_column_presence(column: str, test_column_presence):
    test_column_presence(schema_column=column, table=table)

# For each column in the schema, check all columns are the correct type
@pytest.mark.parametrize("column", party_schema.fields.keys())
def test_column_type(column, test_column_type):
    test_column_type(schema_name=column, schema_data_type=party_schema[column], table=table)

# Validate all fields marked in the schema as being non-nullable are non-nullable. This is in addition
# to the schema level tests, since it's not possible to enforce an embedded struct is non-nullable.

@pytest.mark.parametrize("path", non_nullable_fields(party_schema))
def test_non_nullable_fields(path, test_non_nullable_fields):
    test_non_nullable_fields(path=path, table=table)

@pytest.mark.parametrize("column,values", [
    ("type", ["COMPANY", "CONSUMER"]),
    ("civil_status_code", ['SINGLE', 'LEGALLY_DIVORCED', 'DIVORCED', 'WIDOW', 'STABLE_UNION', 'SEPARATED', 'UNKNOWN']),
    ("education_level_code", ['LESS_THAN_PRIMARY_EDUCATION', 'PRIMARY_EDUCATION', 'LOWER_SECONDARY_EDUCATION', 'UPPER_SECONDARY_EDUCATION', 'POST_SECONDARY_NON_TERTIARY_EDUCATION', 'SHORT_CYCLE_TERTIARY_EDUCATION', 'BACHELORS_OR_EQUIVALENT', 'MASTERS_OR_EQUIVALENT', 'DOCTORAL_OR_EQUIVALENT', 'NOT_ELSEWHERE_CLASSIFIED', 'UNKNOWN']),
    ("nationalities.region_code", get_valid_region_codes()),
    ("residencies.region_code", get_valid_region_codes())
])
def test_column_values(column, values, test_column_values):
    common_tests.test_column_values(table=table, column=column, values=values)

@pytest.mark.parametrize("column,expression", [
    ("birth_date", table.type == "COMPANY"),
    ("gender", table.type == "COMPANY"),
    ("establishment_date", table.type == "CONSUMER"),
    ("occupation", table.type == "CONSUMER"),
])
def test_null_if(column, expression):
    test = common_tests.test_null_if()
    test(table=table, expression=expression, column=column)

if __name__ == "__main__":
    retcode = pytest.main()
