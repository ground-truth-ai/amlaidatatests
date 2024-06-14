from abc import ABC
from ibis import Table, Schema
from ibis import Expr, Schema, _
from ibis.common.exceptions import IbisTypeError
import pytest

class FailTest(Exception):
    pass

def resolve_field(table: Table, column: str) -> Expr:
    # Given a path x.y.z, resolve the field object
    # on the table
    field = table
    for i, p in enumerate(column.split(".")):
        # The first field is a table. If the table
        # has a field called table, this loo
        if i > 0 and field.type().is_array():
            # Arrays must be unnested and then addressed so
            # we can access all the levels of the array
            table = table.mutate(unest=field.unnest())
            field = table["unest"]
        field = field[p]
    return table, field

def resolve_field_to_level(table: Table, column: str, level: int):
    parent_column_split = column.split(".")
    parent_column = ".".join(parent_column_split[:level])
    return resolve_field(table, parent_column)

class AbstractBaseTest(ABC):
    pass


class AbstractTableTest(AbstractBaseTest):

    def __init__(self, schema: Schema) -> None:
        self.schema = schema
        super().__init__()


    def test(self, *, table: Table) -> None:
        ...

    def __call__(self, table: Table):
        self.test(table=table)

class AbstractColumnTest(AbstractBaseTest):

    def __init__(self, schema: Schema) -> None:
        self.schema = schema
        super().__init__()

    def test(self, *, table: Table, column: str) -> None:
        ...

    def __call__(self, table: Table, column: str):
        # It's fine for the top level column to be missing if it's 
        # an optional field. If it is, we can skip the whole test
        try:
            f = resolve_field_to_level(table=table, column=column, level=1)
        except IbisTypeError:
            parent_column = column.split(".")[0]
            if self.schema[parent_column].nullable:
                pytest.skip(f"Skipping running test on non-existent (but not required) column {column}")
            # Deliberately do not error - the test should continue and will most likely fail
            pass
        # if self.schema
        self.test(table=table, column=column)