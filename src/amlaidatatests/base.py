import contextlib
import copy
import logging
import warnings
from abc import ABC
from functools import partial
from typing import Any, Callable, Optional
from urllib.parse import parse_qsl, urlparse

import ibis
import pytest
from google.api_core.exceptions import NotFound as GoogleTableNotFound
from ibis import BaseBackend, Expr, IbisError, Table, _
from ibis import selectors as s
from ibis.common.exceptions import IbisTypeError

from amlaidatatests.config import ConfigSingleton, cfg
from amlaidatatests.exceptions import (
    AMLAITestSeverity,
    DataTestFailure,
    DataTestWarning,
    SkipTest,
    get_test_failure_descriptions,
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
        # In testing mode tests can be anonymous
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
        self._pre_test_hooks(connection)
        self.table = self._run_with_severity(
            connection=connection,
            f=self.check_table_exists,
            table_config=self.table_config,
            request=request,
        )
        self._run_with_severity(connection=connection, f=self._test)

    def _pre_test_hooks(self, connection: BaseBackend):
        """Using the global configuration, modifies the
        state for any pre-execution configuration.

        Args:
            connection: The ibis connection object
        """
        _cfg = ConfigSingleton.get()
        # Configure dry run
        execute = connection.execute
        if _cfg.dry_run:

            def __no_op(*args, **kwargs):
                pytest.xfail("Skipping test because dry_run selected")

            # Dry run does not execute against the connection,
            # so patch in a skip/no-op
            # so patch in a skip/no-op. We don't add to the patch list
            execute = __no_op

        # Configure sql logging

        if path := _cfg.log_sql_path:
            # The class uses a flavour name to class map to
            # identify the map, so we get the first map. This
            # gets the first match from the generator function
            def __compile_sql(
                execute: Callable[[ibis.Expr], Any], expr: ibis.Expr
            ) -> str:

                with open(path.joinpath(f"{self.test_id}.sql"), "wb") as f:
                    connection_string = cfg().get("connection_string")
                    result = urlparse(connection_string)

                    # Write the test description to the top of the file as a comment
                    f.write(
                        f"-- {get_test_failure_descriptions(self.test_id)} \n".encode(
                            "utf-8"
                        )
                    )
                    # Important to get the dialect here **from the connection string**
                    # since the dryrun mode sets the internal configuration
                    f.write(
                        str(
                            ibis.to_sql(expr, pretty=True, dialect=result.scheme)
                        ).encode("utf-8")
                    )
                    return execute(expr)

            connection.execute = partial(__compile_sql, execute)


@contextlib.contextmanager
def use_column_prefix(cls: "AbstractColumnTest", prefix: str):
    """Context manager for managing the column prefix. If a test fails without
    cleaning up the value of cls.column, it still gets reverted due to the
    finally statement"""
    revert = copy.deepcopy(cls.column)
    try:
        if prefix:
            cls.column = f"{prefix}.{cls.column}"
        yield
    finally:
        cls.column = revert


class AbstractColumnTest(AbstractTableTest):
    def __init__(
        self,
        table_config: ResolvedTableConfig,
        column: str | Callable[[Table], Expr],
        severity: AMLAITestSeverity = AMLAITestSeverity.ERROR,
        test_id: Optional[str] = None,
    ) -> None:
        """ """
        # Deep copy to avoid modifying the column variable globally if
        # a prefix is provided
        self.column = column
        super().__init__(table_config=table_config, severity=severity, test_id=test_id)

    def set_extra_pytest_attributes(self, request):
        # Callables do not parse the columns they're using
        if isinstance(self.column, str):
            self._add_pytest_attribute(request, "column", self.column)

    @property
    def id(self) -> Optional[str]:
        """Override to provide additional information about the
        test to pytest to identify the test"""
        if self.test_id:
            id_ = f"{self.test_id}-{self.__class__.__name__}"
        else:
            id_ = f"{self.__class__.__name__}"

        if isinstance(self.column, str):
            return f"{id_}-{self.column}"
        else:
            return id_

    @property
    def full_column_path(self):
        if isinstance(self.column, str):
            return f"{self.table_config.table.get_name()}.{self.column}"
        else:
            return self.table_config.table.get_name()

    def _get_nested_field(self, schema, pth: str):
        elements = pth.split(".")
        e = elements.pop(0)
        if len(elements) == 0:
            # Top level element
            if e == "":
                return schema
            if schema.is_struct():
                return schema[e]
            return schema
        return self._get_nested_field(schema[e], ".".join(elements))

    def _check_column_exists(self):
        if isinstance(self.column, Callable):
            self.column(self.table_config.table)
            # TODO: We don't check optional columns here
            return
        # First, check the parent column.
        # TODO: Check with multiple levels. This code doesn't check deeply nested structures
        elements = self.column.split(".")
        column_schema = self.table_config.schema[elements[0]]
        for i, e in enumerate(elements):
            try:
                resolve_field_to_level(
                    table=self.table, column=self.column, level=i + 1
                )
            except (IbisTypeError, KeyError, NotImplementedError) as e:
                # Find out if the field was nullable in the schema definition
                if self._get_nested_field(
                    column_schema, ".".join(elements[1 : i + 1])
                ).nullable:
                    raise SkipTest(
                        "Skipping running test on non-existent (but not required) column"
                        f" {self.column}"
                    ) from e
                raise DataTestFailure(
                    f"Required column {self.column} does not exist"
                ) from e

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
            prefix:     a prefix for the column to be tested. Used to specify a
                        specific column to be tested for an entity. Defaults to None.
        """
        # It's fine for the top level column to be missing if it's
        # an optional field. If it is, we can skip the whole test
        self.table = self._run_with_severity(
            connection=connection,
            f=self.check_table_exists,
            table_config=self.table_config,
            request=request,
        )
        self.process_test_request(request)
        with use_column_prefix(self, prefix):
            self._pre_test_hooks(connection)
            self._run_with_severity(f=self._check_column_exists)
            self._run_with_severity(connection=connection, f=self._test)

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
