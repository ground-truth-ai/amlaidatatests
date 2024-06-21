from abc import ABC
from typing import List, Optional
from ibis import Table, Schema
from ibis import Expr, Schema, _
from ibis.common.exceptions import IbisTypeError
from ibis import literal
from ibis import BaseBackend
import pytest

class FailTest(Exception):
    pass

OPTIONAL_TABLES = ["party_supplementary_table"]

def resolve_field(table: Table, column: str) -> Expr:
    # Given a path x.y.z, resolve the field object
    # on the table
    splits = column.split(".")
    table = table.select(splits[0])
    field = table
    for i, p in enumerate(splits):
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

    def __init__(self, table: Table) -> None:
        self.table = table
        super().__init__()


    def test(self, *, connection: BaseBackend) -> None:
        ...

    def check_table_exists(self, connection: BaseBackend):
        try:
            connection.execute(self.table.select(literal(1)).limit(0))
            return
        except:
            if self.table.get_name(): # is optional
                pytest.skip(f"Skipping test: optional table {self.table.get_name()} does not exist")
            else:
                raise FailTest(f"Required table {self.table.get_name()} does not exist")

    def __call__(self, connection: BaseBackend):
        # Check if table exists
        self.check_table_exists(connection)
        self.test(connection=connection)


class AbstractColumnTest(AbstractTableTest):

    def __init__(self, table: Table, column: str, validate: bool = True) -> None:
        """ """
        self.column = column
        # Ensure the column is specified on the unbound table
        # provided
        if validate:
            resolve_field(table, column)
        super().__init__(table=table)

    def test(self, *, connection: BaseBackend) -> None:
        ...

    def get_bound_table(self, connection: BaseBackend):
        return connection.table(self.table.get_name())

    def __call__(self, connection: BaseBackend, prefix: Optional[str] = None):
        # It's fine for the top level column to be missing if it's 
        # an optional field. If it is, we can skip the whole test
        self.check_table_exists(connection)
        __prefix_revert = None
        if prefix:
            __prefix_revert = self.column
            self.column = f"{prefix}.{self.column}"
        try:
            f = resolve_field_to_level(table=self.get_bound_table(connection), column=self.column, level=1)
        except IbisTypeError:
            parent_column = self.column.split(".")[0]
            if self.table.schema()[parent_column].nullable:
                pytest.skip(f"Skipping running test on non-existent (but not required) column {self.column}")
            # Deliberately do not error - the test should continue and will most likely fail
            pass
        # if self.schema
        self.test(connection=connection)
        if __prefix_revert:
            # Deliberately bypass the validating setter
            self.column = __prefix_revert