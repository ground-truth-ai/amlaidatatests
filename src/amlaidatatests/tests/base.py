from abc import ABC
from ibis import Table

class FailTest(Exception):
    pass

class AbstractBaseTest(ABC):

    def test(table: Table) -> None:
        ...