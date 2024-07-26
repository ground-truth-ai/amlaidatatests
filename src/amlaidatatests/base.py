import copy
import logging
import warnings
from abc import ABC
from typing import Any, Callable, Optional

import ibis
import pytest
from google.api_core.exceptions import NotFound as GoogleTableNotFound
from ibis import BaseBackend, Expr, IbisError, Table, _
from ibis import selectors as s
from ibis.common.exceptions import IbisTypeError

from amlaidatatests.config import cfg
from amlaidatatests.exceptions import (
    AMLAITestSeverity,
    DataTestFailure,
    DataTestWarning,
    SkipTest,
)
from amlaidatatests.schema.base import ResolvedTableConfig, TableType

logger = logging.getLogger(__name__)


def resolve_field(table: Table, column: str) -> tuple[Table, Expr]:
    # Given a path x.y.z, resolve the field object
    # on the table
    splits = column.split(".")

    field = table
    for i, p in enumerate(splits):
        field_name = ".".join(splits[: i + 1])
        field = field[p].name(field_name)
        if field.type().is_array():
            # Arrays must be unnested and then addressed so
            # we can access all the levels of the array
            table = table.mutate(**{field_name: field.unnest()})
            field = table[field_name]
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
        """ populated at test call time as part of a table call """
        self.table_config = table_config
        self.severity = severity
        if (not cfg().testing_mode) and test_id is None:
            raise ValueError("test_id must be set")
        self.test_id = test_id

    def set_pytest_attributes(self, request: pytest.FixtureRequest):
        if self.test_id:
            self._add_pytest_attribute(request, "test_id", self.test_id)
        self._add_pytest_attribute(request, "table", self.table_config.name)

    def process_test_request(self, request: pytest.FixtureRequest):
        self.set_pytest_attributes(request)
        self.set_extra_pytest_attributes(request)

    def set_extra_pytest_attributes(self, request) -> None:
        """Override to provide additional attributes to pytest in implementing
        classes
        """
        pass

    @property
    def id(self) -> Optional[str]:
        """Override to provide additional information about the
        test to pytest to identify the test"""
        if self.test_id:
            return f"{self.test_id}-{self.__class__.__name__}"
        else:
            return f"{self.__class__.__name__}"

    def _test(self, *, connection: BaseBackend) -> None: ...

    def _raise_warning(self, warning: DataTestWarning):
        # We double log here to try and capture the logs
        # both to pytest and to pytest-html
        logging.warning(warning)
        warnings.warn(warning)

    def _add_pytest_attribute(self, request, key, value):
        """Add a user attribute for pytest to use"""
        request.node.user_properties.append((key, value))

    def _run_with_severity(self, f: Callable, **kwargs) -> Any | None:
        """Execute an arbitrary function, catching errors attributed to
        amlaidatatest failures. Failures are then handled according to the
        configuration of the parent test.

        SkipTest failures are also handled as they allow us to catch
        skips during unittesting in a way we cannot if pytest.skip is called

        Args:
            f: The function to run, with **kwargs specified.

        Raises:
            e: TestWarning. Raising WarnTest from a test will always generate a
                warning, but TestWarning could also be converted from a WarnTest
                into a TestWarning if necessary.
            e: TestFailure.

        Returns:
            The result of f(**kwargs). Note that a test warning could result in
            None being returned. This is because we use the exception machinery
            to handle the conversion of exceptions to testfailures.
        """
        try:
            return f(**kwargs)
        except DataTestWarning as e:
            e.test_id = self.test_id
            warnings.warn(e)
            return None
        except DataTestFailure as e:
            e.test_id = self.test_id
            if isinstance(e, DataTestWarning):
                self._raise_warning(e)
                return None
            if self.severity == AMLAITestSeverity.ERROR:
                raise e
            if self.severity == AMLAITestSeverity.WARN:
                warning = DataTestWarning(e.message, expr=e.expr)
                self._raise_warning(warning)
            if self.severity == AMLAITestSeverity.INFO:
                logging.info(e.message)
        except SkipTest as e:
            # Used to detect if we're running the unit tests.
            # This allows us to catch (and test) that test
            # skipping is being correctly raised
            if hasattr(pytest, "__AML_AI_TESTING_THE_TESTS"):
                raise e
            else:
                pytest.skip(e.message)
        # # This should *never* happen - either the function succeeded and return
        # # something, or another error and handled. The linter requires this
        # # because it doesn't know that pytest.skip will skip the test.
        # raise RuntimeError("A test failed for an unknown reason")


class AbstractTableTest(AbstractBaseTest):
    """Base class for test which are across an entire table
    and do not specify a single column

    Args:
        table_config: The resolved table config to test
        severity: The error type to emit on test failure
                  Defaults to AMLAITestSeverity.ERROR
        test_id:  A unique identifier for the test. Useful when used via
                  @pytest.parameter as it allows us to uniquely identify the test
    """

    def __init__(
        self,
        table_config: ResolvedTableConfig,
        severity: AMLAITestSeverity = AMLAITestSeverity.ERROR,
        test_id: Optional[str] = None,
    ) -> None:
        table_config = copy.deepcopy(table_config)
        # We don't want to get resolved table at test definition time, only at test time
        self.resolved_table: Optional[Table] = None
        super().__init__(table_config=table_config, severity=severity, test_id=test_id)

    def check_table_exists(
        self,
        connection: BaseBackend,
        table_config: ResolvedTableConfig,
        request: Optional[pytest.FixtureRequest] = None,
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
        if request:
            self._add_pytest_attribute(request, "table_missing", True)
        self._skip_test_if_optional_table(table_config=table_config)

    def _skip_test_if_optional_table(self, table_config: ResolvedTableConfig):
        if table_config.optional:  # is optional
            raise SkipTest(
                f"Skipping test: optional table {table_config.table.get_name()} "
                "does not exist"
            )
        else:
            # Deliberately not a test failure - the test cannot continue
            raise ValueError(
                f"Required table {table_config.table.get_name()} does not exist"
            )

    def get_latest_rows(
        self, table: Table, table_config: Optional[ResolvedTableConfig] = None
    ):
        """Get the latest, not deleted version of the row"""
        table_config = self.table_config if table_config is None else table_config
        if table_config.table_type == TableType.EVENT:
            return table
        if table_config.table_type not in (
            TableType.CLOSED_ENDED_ENTITY,
            TableType.OPEN_ENDED_ENTITY,
        ):
            raise ValueError(f"{table_config.table_type} is not a valid table type")
        table = table.filter(_["is_entity_deleted"].isin([ibis.literal(False), None]))
        return table.select(
            s.all(),
            row_num=ibis.row_number().over(
                group_by=table_config.entity_keys,
                order_by=ibis.desc("validity_start_time"),
            ),
        ).filter(_.row_num == 0)

    def __call__(self, connection: BaseBackend, request):
        self.process_test_request(request)
        # Check if table exists
        self.table = self._run_with_severity(
            connection=connection,
            f=self.check_table_exists,
            table_config=self.table_config,
            request=request,
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

    def set_extra_pytest_attributes(self, request):
        self._add_pytest_attribute(request, "column", self.column)

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

    def _check_column_exists(self):
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

    def __call__(
        self,
        connection: BaseBackend,
        request: pytest.FixtureRequest,
        prefix: Optional[str] = None,
    ):
        """Execute the test.

        1) Checks the table exists. If the table is required, amlaidatatests will
        fail the test, otherwise it will skip the test.
        2) Checks if the column exists. If the column is required, amlaidatatests will
        fail the test, otherwise it will skip the test.

        Args:
            connection: ibis backend object to execute the tests against
            prefix: _description_. Defaults to None.
        """
        self.process_test_request(request)
        # It's fine for the top level column to be missing if it's
        # an optional field. If it is, we can skip the whole test
        self.table = self._run_with_severity(
            connection=connection,
            f=self.check_table_exists,
            table_config=self.table_config,
            request=request,
        )
        __prefix_revert = None
        if prefix:
            __prefix_revert = self.column
            self.column = f"{prefix}.{self.column}"
        self._run_with_severity(f=self._check_column_exists)
        self._run_with_severity(connection=connection, f=self._test)
        if __prefix_revert:
            self.column = __prefix_revert

    def filter_null_parent_fields(self):
        """Get

        Returns:
            _description_
        """
        # If subfields exist (struct or array), we need to compare the nullness
        # of the parent as well - it doesn't make any sense to check the parent
        # if the child is also null
        predicates = []
        if self.column.count(".") >= 1:
            _, parent_field = resolve_field_to_level(self.table, self.column, -1)
            # We want to check for cases only where field is null but its parent
            # isn't
            predicates += [parent_field.notnull()]
        return predicates
