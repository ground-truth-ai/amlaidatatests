import pytest
from amlaidatatests.connection import connection_factory


@pytest.fixture(scope="session")
def connection():
    connection = connection_factory()
    return connection
