from abc import ABC
from ibis import Table

class FailTest(Exception):
    pass

class AbstractBaseTest(ABC):
    pass


class AbstractColumnTest(AbstractBaseTest):

    def test(self, *, table: Table, column: str) -> None:
        ...

class AbstractTableTest(AbstractBaseTest):

    def test(self, *, table: Table) -> None:
        ...