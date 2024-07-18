import copy
import enum
import logging
import warnings
from abc import ABC
from enum import auto
from typing import Callable, Optional

import ibis
from ibis import _, selectors as s
import pytest
from google.api_core.exceptions import NotFound as GoogleTableNotFound
from ibis import BaseBackend, Expr, IbisError, Table
from ibis.common.exceptions import IbisTypeError

from amlaidatatests.schema.base import ResolvedTableConfig, TableType

logger = logging.getLogger(__name__)


class AMLAITestSeverity(enum.Enum):
    ERROR = auto()
    WARN = auto()
    INFO = auto()


class FailTest(Exception):
    def __init__(
        self,
        message: str,
        expr: Optional[Expr] = None,
        description: Optional[str] = None,
    ) -> None:
        self.message = message
        self.expr = expr
        self.description = description

        self.sql = str(ibis.get_backend().compile(expr)) if expr is not None else None

    def friendly_message(self):
        msg = f"{self.description}\n" if self.description else ""
        msg += self.message
        if self.sql:
            msg += "\nTo reproduce this result, run:\n"
            msg += self.sql
        return msg

    def __str__(self):
        return self.friendly_message()


class WarnTest(Warning, FailTest):
    def __init__(
        self,
        message: str,
        expr: Optional[Expr] = None,
        description: Optional[str] = None,
    ) -> None:
        self.message = message
        self.description = description

        self.sql = str(ibis.get_backend().compile(expr)) if expr is not None else None

    def __str__(self):
        return self.friendly_message()


class SkipTest(Exception):
    def __init__(self, message: str) -> None:
        self.message = message


def resolve_field(table: Table, column: str) -> tuple[Table, Expr]:
    # Given a path x.y.z, resolve the field object
    # on the table
    splits = column.split(".")

    table = table.mutate({splits[0]: splits[0]})
    field = table[splits[0]]
    for p in splits[1:]:
        # The first field is a table. If the table
        # has a field called table, this loo
        if field.type().is_struct():
            field = field[p]
        if field.type().is_array():
            # Arrays must be unnested and then addressed so
            # we can access all the levels of the array
            table = table.mutate(unest=field.unnest())
            field = table["unest"]
    return table, field


def resolve_field_to_level(table: Table, column: str, level: int):
    parent_column_split = column.split(".")
    parent_column = ".".join(parent_column_split[:level])
    return resolve_field(table, parent_column)


class AbstractBaseTest(ABC):

    def __init__(
        self,
        table_config: ResolvedTableConfig,
        severity: AMLAITestSeverity = AMLAITestSeverity.ERROR,
        test_id: Optional[str] = None,
    ) -> None:
        self.table: Optional[Table] = None
        self.table_config = table_config
        self.severity = severity
        self.test_id = test_id

    @property
    def id(self) -> Optional[str]:
        """Override to provide additional information about the
        test to pytest to identify the test"""
        if self.test_id:
            return f"{self.test_id}-{self.__class__.__name__}-{self.table_config.name}"
        else:
            return f"{self.__class__.__name__}-{self.table_config.name}"

    def _test(self, *, connection: BaseBackend) -> None: ...

    def _raise_warning(self, warning: WarnTest):
        # We double log here to try and capture the logs
        # both to pytest and to pytest-html
        logging.warning(warning)
        warnings.warn(warning)

    def _run_with_severity(self, f: Callable, **kwargs):
        try:
            return f(**kwargs)
        except FailTest as e:
            if isinstance(e, WarnTest):
                self._raise_warning(e)
                return
            if self.severity == AMLAITestSeverity.ERROR:
                raise e
            if self.severity == AMLAITestSeverity.WARN:
                warning = WarnTest(e.message, expr=e.expr)
                self._raise_warning(warning)
            if self.severity == AMLAITestSeverity.INFO:
                # We need to know if we're running in unittest
                # mode or not
                pytest.skip(e.message)
        except SkipTest as e:
            if hasattr(pytest, "__AML_AI_TESTING_THE_TESTS"):
                raise e
            else:
                pytest.skip(e.message)
        except WarnTest as e:
            warnings.warn(e)


class AbstractTableTest(AbstractBaseTest):
    """_summary_

    Args:
        AbstractBaseTest: _description_
    """

    def __init__(
        self,
        table_config: ResolvedTableConfig,
        severity: AMLAITestSeverity = AMLAITestSeverity.ERROR,
        test_id: Optional[str] = None,
    ) -> None:
        """_summary_

        Args:
            table_config: _description_
            severity: _description_. Defaults to AMLAITestSeverity.ERROR.
        """
        table_config = copy.deepcopy(table_config)
        # We don't want to get resolved table at test definition time, only at test time
        self.resolved_table: Optional[Table] = None
        super().__init__(table_config=table_config, severity=severity, test_id=test_id)

    def check_table_exists(
        self, connection: BaseBackend, table_config: ResolvedTableConfig
    ):
        try:
            return connection.table(table_config.table.get_name())
        # Ibis has no consistent API around missing tables:
        # https://github.com/ibis-project/ibis/issues/9468
        # We have to workaround this whilst ensuring we don't
        # catch any errors we don't want to catch. This is
        # easier with some backends than others
        except GoogleTableNotFound as e:
            if connection.name != "bigquery":
                raise e
        except IbisError as e:
            if connection.name != "duckdb":
                raise e
        self._skip_test_if_optional_table(table_config=table_config)

    def _skip_test_if_optional_table(self, table_config: ResolvedTableConfig):
        if table_config.optional:  # is optional
            raise SkipTest(
                f"Skipping test: optional table {table_config.table.get_name()}"
                "does not exist"
            )
        else:
            raise FailTest(
                f"Required table {table_config.table.get_name()} does not exist"
            )

    def _test(self, *, connection: BaseBackend) -> None: ...

    def optional_table(self):
        pass

    def get_latest_rows(self, table: Table):
        if self.table_config.table_type not in (
            TableType.CLOSED_ENDED_ENTITY,
            TableType.OPEN_ENDED_ENTITY,
        ):
            raise ValueError(
                f"{self.table_config.table_type} is not a valid table type"
            )
        return (
            table.filter(_["is_entity_deleted"] == ibis.literal(False))
            .select(
                s.all(),
                row_num=ibis.row_number().over(
                    group_by=self.table_config.entity_keys,
                    order_by=ibis.desc("validity_start_time"),
                ),
            )
            .filter(_.row_num == 0)
        )

    def __call__(self, connection: BaseBackend):
        # Check if table exists
        self.table = self._run_with_severity(
            connection=connection,
            f=self.check_table_exists,
            table_config=self.table_config,
        )
        self._run_with_severity(connection=connection, f=self._test)


class AbstractColumnTest(AbstractTableTest):

    def __init__(
        self,
        table_config: ResolvedTableConfig,
        column: str,
        severity: AMLAITestSeverity = AMLAITestSeverity.ERROR,
        test_id: Optional[str] = None,
    ) -> None:
        """ """
        self.column = column
        super().__init__(table_config=table_config, severity=severity, test_id=test_id)

    @property
    def id(self) -> Optional[str]:
        """Override to provide additional information about the
        test to pytest to identify the test"""
        if self.test_id:
            return f"{self.test_id}-{self.__class__.__name__}-{self.column}"
        else:
            return f"{self.__class__.__name__}-{self.column}"

    @property
    def full_column_path(self):
        return f"{self.table_config.table.get_name()}.{self.column}"

    def _check_column_exists(self, connection):
        try:
            resolve_field_to_level(table=self.table, column=self.column, level=1)
        except IbisTypeError as e:
            parent_column = self.column.split(".")[0]
            if self.table_config.schema[parent_column].nullable:
                raise SkipTest(
                    "Skipping running test on non-existent (but not required) column"
                    f" {self.column}"
                ) from e
            # Deliberately do not error - the test should continue and will most
            # likely fail
            pass

    def __call__(self, connection: BaseBackend, prefix: Optional[str] = None):
        # It's fine for the top level column to be missing if it's
        # an optional field. If it is, we can skip the whole test
        self.table = self._run_with_severity(
            connection=connection,
            f=self.check_table_exists,
            table_config=self.table_config,
        )
        __prefix_revert = None
        if prefix:
            __prefix_revert = self.column
            self.column = f"{prefix}.{self.column}"
        self._run_with_severity(connection=connection, f=self._check_column_exists)
        self._run_with_severity(connection=connection, f=self._test)
        if __prefix_revert:
            self.column = __prefix_revert
