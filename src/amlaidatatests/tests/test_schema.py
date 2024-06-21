import pytest

TABLES = ["transaction"]

@pytest.mark.parametrize("table", TABLES)
def test_for_extraneous_fields(table):
    pass