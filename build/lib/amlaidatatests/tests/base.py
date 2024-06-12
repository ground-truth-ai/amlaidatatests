from abc import ABC
from ibis import Table

class AbstractBaseTest(ABC):

    def test(self, table: Table, **kwargs):
        ...