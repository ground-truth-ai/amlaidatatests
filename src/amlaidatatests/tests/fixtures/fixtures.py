from amlaidatatests.io import get_table_name
import pytest
from amlaidatatests.connection import connection_factory

SUFFIX = "1234"


@pytest.fixture
def connection():
    connection = connection_factory()
    return connection

@pytest.fixture
def table_factory():
    def _table(name):
        connection = connection_factory()
        return connection.table(get_table_name(name))

    return _table