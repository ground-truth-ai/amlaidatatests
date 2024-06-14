import pytest
import ibis

@pytest.fixture()
def in_memory_connection():
    return ibis.pandas.connect()
