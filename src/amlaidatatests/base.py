from abc import ABC
from enum import Enum, auto
import enum
from typing import Optional
import warnings
from ibis import Table
from ibis import Expr, _
import ibis
from ibis.common.exceptions import IbisTypeError
from ibis import BaseBackend
from ibis import Expr
import pytest
from google.api_core.exceptions import NotFound as GoogleTableNotFound


class AMLAITestSeverity(enum.Enum):
    ERROR = auto()
    WARN = auto()
    INFO = auto()


class FailTest(Exception):
    def __init__(self, message: str, expr: Optional[Expr] = None) -> None:
        self.message = message

        self.sql = str(ibis.get_backend().compile(expr)) if expr is not None else None

    def friendly_message(self):
        msg = self.message
        if self.sql:
            msg += "\nTo reproduce this result, run:\n"
            msg += self.sql
        return msg

    def __str__(self):
        return self.friendly_message()


class WarnTest(Warning):
    pass


OPTIONAL_TABLES = ["party_supplementary_table"]


def resolve_field(table: Table, column: str) -> tuple[Table, Expr]:
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

    def __init__(
        self, table: Table, severity: AMLAITestSeverity = AMLAITestSeverity.ERROR
    ) -> None:
        self.table = table
        self.severity = severity

    @property
    def id(self) -> Optional[str]:
        """Override to provide additional information about the
        test to pytest"""
        return None

    def _test(self, *, connection: BaseBackend) -> None: ...

    def _run_test_with_severity(self, connection: BaseBackend):
        try:
            self._test(connection=connection)
        except FailTest as e:
            if self.severity == AMLAITestSeverity.ERROR:
                raise e
            if self.severity == AMLAITestSeverity.WARN:
                warnings.warn(e.message)
            if self.severity == AMLAITestSeverity.INFO:
                pytest.skip(e.message)
        except WarnTest as e:
            warnings.warn(e)


class AbstractTableTest(AbstractBaseTest):

    def __init__(
        self, table: Table, severity: AMLAITestSeverity = AMLAITestSeverity.ERROR
    ) -> None:
        self.table = table
        super().__init__(table=table, severity=severity)

    @property
    def id(self) -> Optional[str]:
        """Override to provide additional information about the
        test to pytest to identify the test"""
        return f"{self.__class__.__name__}"

    def _test(self, *, connection: BaseBackend) -> None: ...

    def check_table_exists(self, connection: BaseBackend):
        try:
            connection.table(self.table.get_name())
            return
        except GoogleTableNotFound:
            if self.table.get_name():  # is optional
                pytest.skip(
                    f"Skipping test: optional table {self.table.get_name()} does not exist"
                )
            else:
                raise FailTest(f"Required table {self.table.get_name()} does not exist")

    def optional_table(self):
        pass

    def __call__(self, connection: BaseBackend):
        # Check if table exists
        self.check_table_exists(connection)
        self._run_test_with_severity(connection=connection)


class AbstractColumnTest(AbstractTableTest):

    def __init__(
        self,
        table: Table,
        column: str,
        validate: bool = True,
        severity: AMLAITestSeverity = AMLAITestSeverity.ERROR,
    ) -> None:
        """ """
        self.column = column
        # Ensure the column is specified on the unbound table
        # provided
        if validate:
            resolve_field(table, column)
        super().__init__(table=table, severity=severity)

    @property
    def id(self) -> Optional[str]:
        """Override to provide additional information about the
        test to pytest to identify the test"""
        return f"{self.__class__.__name__}-{self.column}"

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
            f = resolve_field_to_level(
                table=self.get_bound_table(connection), column=self.column, level=1
            )
        except IbisTypeError:
            parent_column = self.column.split(".")[0]
            if self.table.schema()[parent_column].nullable:
                pytest.skip(
                    f"Skipping running test on non-existent (but not required) column {self.column}"
                )
            # Deliberately do not error - the test should continue and will most likely fail
            pass
        self._run_test_with_severity(connection=connection)
        if __prefix_revert:
            self.column = __prefix_revert
