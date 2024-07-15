import pytest

from amlaidatatests.connection import connection_factory


@pytest.fixture(scope="session")
def connection():
    con = connection_factory()
    return con
