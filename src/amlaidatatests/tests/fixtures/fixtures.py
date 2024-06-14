import pytest
from amlaidatatests.connection import connection

SUFFIX = "1234"

@pytest.fixture
def table_factory():
    def _table(name):
        return connection.table(f"{name}_{SUFFIX}")

    return _table