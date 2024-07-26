"""Fixtures for amlaidatatests"""

import pytest
from ibis import BaseBackend

from amlaidatatests.connection import connection_factory


@pytest.fixture(scope="session")
def connection() -> BaseBackend:
    """Pytest fixture returning the configured backend

    Session scoped to avoid repeatedly creating connections objects, which is
    slow

    Returns:
        ibis connection object
    """
    con = connection_factory()
    return con
