from typing import Callable

import ibis
import pytest
from ibis import BaseBackend, Table

from amlaidatatests.connection import connection_factory
from amlaidatatests.tests.conftest import (
    pytest_addoption as passthrough_pytest_addoption,
)

from .utils import temporary_table_name


def pytest_configure(config):
    pass


@pytest.hookimpl(tryfirst=True)
def pytest_addoption(parser):
    passthrough_pytest_addoption(
        parser=parser, defaults={"connection_string": "duckdb://"}
    )


@pytest.fixture(scope="session")
def test_connection() -> BaseBackend:
    """Test connection fixture which defaults to in memory
    duckdb"""
    connection = connection_factory("duckdb://")
    return connection


@pytest.fixture(scope="session")
def create_test_table(test_connection: BaseBackend) -> Callable[..., Table]:
    """Utility for the creation of a temporary test table"""

    def _create_test_table(obj):
        tbl_name = temporary_table_name()
        table = test_connection.create_table(
            name=tbl_name, obj=obj, temp=True, schema=obj.schema()
        )
        # Workaround for https://github.com/ibis-project/ibis/issues/9502
        if ibis.get_backend(table).name == "duckdb":
            return tbl_name
        return table.get_name()

    return _create_test_table


@pytest.fixture()
def test_raise_on_skip():
    """Patch pytest during associated tests to raise a SkipTest exception
    instead of calling pytest.skip(), which marks the test as skipped

    The pytest skip exception isn't an exposed exception so instead we use this
    as a switch to decide if we should raise an exception with the same message,
    or skip the test"""
    pytest.__AML_AI_TESTING_THE_TESTS = True
    yield
    del pytest.__AML_AI_TESTING_THE_TESTS
