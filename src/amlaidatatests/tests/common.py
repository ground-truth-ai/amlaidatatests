"""Common tests used by amlaidatatests"""

import datetime
import functools
import itertools
import warnings
from functools import reduce
from typing import Any, Callable, List, Literal, Optional, cast

import ibis
from ibis import BaseBackend, Expr, _
from ibis.common.exceptions import IbisTypeError
from ibis.expr.datatypes import Array, DataType, Struct, Timestamp

from amlaidatatests.base import AbstractColumnTest, AbstractTableTest, resolve_field
from amlaidatatests.exceptions import (
    AMLAITestSeverity,
    DataTestFailure,
    DataTestWarning,
)
from amlaidatatests.schema.base import ResolvedTableConfig, TableType


class TableExcessColumnsTest(AbstractTableTest):
    """Verify there are no excess columns on the table. If there are, raise an
    error.

    By default, this test raises a Warning.

    Args:
        table_config:   The resolved table config to test
        severity:       The error type to emit on test failure
                        Defaults to AMLAITestSeverity.WARN
        test_id:        A unique identifier for the test
    """

    def __init__(
        self,
        table_config: ResolvedTableConfig,
        test_id: Optional[str] = None,
        severity: AMLAITestSeverity = AMLAITestSeverity.WARN,
    ) -> None:
        super().__init__(table_config, test_id=test_id, severity=severity)

    def _test(self, *, connection: BaseBackend):
        actual_columns = set(connection.table(name=self.table.get_name()).columns)
        schema_columns = set(self.table_config.table.columns)
        excess_columns = actual_columns.difference(schema_columns)
        if len(excess_columns) > 0:
            raise DataTestWarning(
                f"{len(excess_columns)} unexpected columns found in table"
                f" {self.table.get_name()}"
            )
        # Schema table


class TableCountTest(AbstractTableTest):
    """Test the number of rows in the table.

    This test does not consider entity deletions or creation and simply counts
    the number of rows in the table.

    Args:
        table_config:   The resolved table config to test
        severity:       The error type to emit on test failure
                        Defaults to AMLAITestSeverity.ERROR
        test_id:        A unique identifier for the test
        max_rows:       The maximum number of rows. The test will error if there
                        are more rows than this in the overall table
    """

    def __init__(
        self,
        table_config: ResolvedTableConfig,
        max_rows: int,
        severity: AMLAITestSeverity = AMLAITestSeverity.ERROR,
        test_id: Optional[str] = None,
    ) -> None:
        self.max_rows = max_rows
        super().__init__(table_config, severity, test_id=test_id)

    def _test(self, *, connection: BaseBackend):
        count = connection.execute(self.table.count())
        if count == 0:
            raise DataTestFailure(f"Table {self.table.get_name()} is empty")
        if count > self.max_rows:
            raise DataTestFailure(
                f"Table {self.table.get_name()} has more rows "
                f"than seems feasible: {count} vs maximum {self.max_rows}. "
                "To stop this error triggering, review "
                "the data provided or increase the scale setting"
            )
        if count > (self.max_rows) * 0.9:
            raise DataTestWarning(
                f"Table {self.table.get_name()} is close to "
                f"the feasibility ceiling: {count} vs maximum {self.max_rows}. "
                "To stop this error triggering, review "
                "the data provided or increase the scale setting"
            )


class PrimaryKeyColumnsTest(AbstractTableTest):
    """Validate the primary key of the table. If the table has more than one
    primary key field, the test will fail.

    Args:
        table_config:   The resolved table config to test
        severity:       The error type to emit on test failure
                        Defaults to AMLAITestSeverity.ERROR
        test_id:        A unique identifier for the test
        column:         The column being tested
    """

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        unique_combination_of_columns: List[str],
        test_id: Optional[str] = None,
    ) -> None:
        """_summary_

        Args:
            table_config: _description_
            unique_combination_of_columns: _description_
        """
        super().__init__(table_config=table_config, test_id=test_id)
        # Check columns provided are in table
        for col in unique_combination_of_columns:
            resolve_field(self.table_config.table, col)
        self.unique_combination_of_columns = unique_combination_of_columns

    def _test(self, *, connection: BaseBackend) -> None:
        # TODO: Make into a single call rather than two
        expr = self.table[self.unique_combination_of_columns].agg(
            unique_rows=_.nunique(), count=_.count()
        )
        result = connection.execute(expr).loc[0]
        n_pairs = result["unique_rows"]
        n_total = result["count"]
        if n_pairs != n_total:
            raise DataTestFailure(f"Found {n_total - n_pairs} duplicate values")


class ColumnCardinalityTest(AbstractColumnTest):
    """Check for the number of values of column, optionally
    grouped by group_by.

    If a group_by field is

    For example:
        if column = party_id, group_by = None
            - Check for the number of distinct party_id
        if column = party_id, group_by = ["account_id"]
            - Check for the number of distinct party_ids
                for each account

    Args:
        table_config:   The resolved table config to test
        severity:       The error type to emit on test failure
                        Defaults to AMLAITestSeverity.ERROR
        test_id:        A unique identifier for the test
        column:         The column being tested
        max_number:     The maximum number of distinct column values this test
                        accepts, inclusive. If more are found the test will fail.
                        If none, this value will not be enforced. Defaults to None.
        min_number:     The minimum number of distinct column values this test
                        accepts, inclusive. If fewer are found the test will fail.
                        If none, this value will not be enforced. Defaults to None.
        where:          A predicate filter applied to the table. Column values
                        will be filtered by this where clause prior to the test.
                        Use to select specific combinations of values to test.
                        Defaults to None.
        having:         _description_. Defaults to None.
        severity:       _description_. Defaults to AMLAITestSeverity.ERROR.
        group_by:       _description_. Defaults to None.
        keep_nulls:     By default, nulls are not included as a valid value to count
    """

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        column: str,
        max_number: Optional[int] = None,
        min_number: Optional[int] = None,
        test_id: Optional[str] = None,
        where: Optional[Callable[[Expr], Expr]] = None,
        having: Optional[Callable[[Expr], Expr]] = None,
        severity: AMLAITestSeverity = AMLAITestSeverity.ERROR,
        group_by: Optional[list[str]] = None,
        keep_nulls: bool = False,
    ) -> None:
        super().__init__(
            table_config=table_config, column=column, severity=severity, test_id=test_id
        )
        self.max_number = max_number
        self.min_number = min_number
        self.group_by = group_by
        self.where = where
        self.having = having
        self.keep_nulls = keep_nulls

    def _test(self, *, connection: BaseBackend) -> None:
        # References to validity_start_time are unnecessary, but they do ensure
        # that the column is present on the table
        table = self.table
        if self.table_config.table_type in (
            TableType.CLOSED_ENDED_ENTITY,
            TableType.OPEN_ENDED_ENTITY,
        ):
            # For these tables, we need to identify the latest row
            table = self.get_latest_rows(table)

        table, column = resolve_field(table=table, column=self.column)

        if not self.keep_nulls:
            table = table.filter(column.notnull())

        grp_columns = None
        if self.group_by:
            grp_columns = []
            for grp in self.group_by:
                _, col = resolve_field(table=table, column=grp)
                grp_columns.append(col)

        if self.where is not None:
            table = table.filter(self.where)
        if grp_columns:
            expr = table.group_by(grp_columns).agg(value_cnt=column.nunique())
        else:
            expr = table.agg(value_cnt=column.nunique())

        expr = expr.filter(
            (expr.value_cnt > self.max_number if self.max_number else False)
            | (expr.value_cnt < self.min_number if self.min_number else False)
        )

        if self.having is not None:
            expr = expr.filter(self.having)

        results = connection.execute(expr.count())

        if results > 0:
            direction = "high" if self.max_number else "low"
            message = (
                f"column has an "
                f"unexpectedly {direction} number of distinct values "
                f"for each {self.group_by} "
                if self.group_by
                else ""
            )
            raise DataTestFailure(
                message=message,
                expr=expr,
            )


class CountFrequencyValues(AbstractColumnTest):
    """Count the number of occurences of each value in a column. Identify cases
    which exceed the expected maximum number of values, or represent a larger
    than expected proportion of values


    Args:
        table_config:   The resolved table config to test
        severity:       The error type to emit on test failure
                        Defaults to AMLAITestSeverity.ERROR
        test_id:        A unique identifier for the test
        column:         The column being tested
        max_proportion: The maximum proportion of the overall number of values.
                        If group_by is set, the result is relative to the
                        columns specified in the group_by parameter. Defaults to
                        None.
        max_number:     The maximum number of occurrences of a single value
                        before an error is raised. Defaults to None.
        where:          Filter the input table by this predicate, Defaults to None.
        having:         Filter the results by this predicate. Use to remove
                        values or results which should not have their max_number or
                        max_proportion tested. Defaults to None.
        group_by:       If set, proportions and number counts are for the column
                        rows in this list. Defaults to None.
        keep_nulls:     By default, nulls are not included in any proportion count or
                        as a valid value to count
    """

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        column: str | Expr,
        test_id: Optional[str] = None,
        max_proportion: Optional[float] = None,
        max_number: Optional[int] = None,
        where: Optional[Callable[[Expr], Expr]] = None,
        having: Optional[Callable[[Expr], Expr]] = None,
        severity: AMLAITestSeverity = AMLAITestSeverity.ERROR,
        group_by: Optional[list[str]] = None,
        keep_nulls: bool = False,
    ) -> None:
        super().__init__(
            table_config=table_config, column=column, severity=severity, test_id=test_id
        )
        if (max_proportion) and any([max_number]):
            raise ValueError("Only proportion or number must be set, not both")
        if not any([max_proportion, max_number]):
            raise ValueError("One of proportion and number must be set")
        if (max_proportion) and (max_proportion < 0 or max_proportion > 1):
            raise ValueError("Proportion must be between 0 and 1")
        self.max_proportion = max_proportion
        self.max_number = max_number
        self.where = where
        self.having = having
        self.group_by = group_by if group_by else []
        self.keep_nulls = keep_nulls

    def _test(self, *, connection: BaseBackend) -> None:
        table = self.table
        if self.table_config.table_type in (
            TableType.CLOSED_ENDED_ENTITY,
            TableType.OPEN_ENDED_ENTITY,
        ):
            # For these tables, we need to identify the latest row
            table = self.get_latest_rows(table)

        if self.where is not None:
            table = table.filter(self.where)

        if isinstance(self.column, str):
            table, column = resolve_field(table=table, column=self.column)
        else:
            column = self.column(table)

        if not self.keep_nulls:
            table = table.filter(column.notnull())

        grp_columns = []
        for grp in self.group_by:
            _, col = resolve_field(table=table, column=grp)
            grp_columns.append(col)

        expr = table.group_by([column, *grp_columns]).agg(value_cnt=table.count())
        expr = expr.mutate(
            proportion=expr.value_cnt / expr.value_cnt.sum().over(group_by=grp_columns)
        )

        boolean_expr = None
        if self.max_proportion:
            boolean_expr |= expr.proportion >= self.max_proportion
        if self.max_number:  # if self.number: - checked during init
            boolean_expr |= expr.value_cnt > self.max_number

        expr = expr.filter(boolean_expr)

        if self.having is not None:
            expr = expr.filter(self.having)

        results = connection.execute(expr.count())

        if results > 0:
            raise DataTestFailure(
                message=f"{results} column values appeared unusually frequently",
                expr=expr,
            )


class VerifyTypedValuePresence(AbstractColumnTest):
    """Checks for the proportion or number of rows containing any
    particular value in column relative to group_by. This is mainly used
    for verifying the presence of a value in a column across a table.

    For example:
    group_by = account_id, column = type, value = CARD,
    max_proportion = 0.1
    will:
        * Count the number of account_id which have a type = CARD (value_cnt)
        * Count the number of account_id which have type = CARD
        * Error if max_proportion of account_id having a type = CARD is greater
            than 10%.

    This is akin to the pseudo sql:
        select count(where type = 'CARD') / count(*) over (group by account_id)
        from table

    Args:
        table_config:           The resolved table config to test
        severity:               The error type to emit on test failure
                                Defaults to AMLAITestSeverity.ERROR
        test_id:                A unique identifier for the test
        column:                 The column being tested
        group_by:               list of column_ids to group by
        max_proportion:         The maximum proportion, inclusive. Defaults to None.
        min_proportion:         The minimum proportion, inclusive. Defaults to None.
        compare_group_by_where: Applies a filter to the proportion denominator.
                                the denominator = count(*)
        value:                  The value in the numerator to count for, equivalent to
                                `where column = value`
        keep_nulls:             By default, nulls are not included in any proportion
                                count or as a valid value to count
    """

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        column: str,
        value: str,
        group_by: list[str],
        test_id: Optional[str] = None,
        max_proportion: Optional[float] = None,
        min_proportion: Optional[float] = None,
        compare_group_by_where: Optional[Callable[[Expr], Expr]] = None,
        severity: AMLAITestSeverity = AMLAITestSeverity.WARN,
        keep_nulls: bool = False,
    ) -> None:
        super().__init__(
            table_config=table_config, column=column, severity=severity, test_id=test_id
        )
        self.max_proportion = max_proportion
        self.min_proportion = min_proportion
        self.value = value
        self.group_by = group_by
        self.where = compare_group_by_where
        self.keep_nulls = keep_nulls

    def _test(self, *, connection: BaseBackend) -> None:
        table = self.table
        if self.table_config.table_type in (
            TableType.CLOSED_ENDED_ENTITY,
            TableType.OPEN_ENDED_ENTITY,
        ):
            # For these tables, we need to identify the latest row
            table = self.get_latest_rows(table)
        table, column = resolve_field(table=table, column=self.column)

        if not self.keep_nulls:
            table = table.filter(column.notnull())

        where_group_kwargs = {"where": self.where(_)} if self.where else {}

        expr = (
            # Concatenate the group by so we can count unique combinations. We append
            # column IDs because there might be overlapping individual IDs
            table.mutate(
                concat=reduce(lambda x, y: x + y, [i + _[i] for i in self.group_by], "")
            )
            .agg(
                value_cnt=_["concat"].nunique(column == self.value),
                group_count=_["concat"].nunique(**where_group_kwargs),
            )
            .mutate(proportion=_.value_cnt / _.group_count)
        )
        results = connection.execute(expr).iloc[0]
        proportion = results["proportion"]

        group_by_narrative = "all rows"
        if len(self.group_by) == 1:
            group_by_narrative = f"unique {self.group_by[0]}"
        else:
            group_by_narrative = f"unique combinations of {self.group_by}"
        if self.max_proportion and results["proportion"] >= self.max_proportion:
            raise DataTestFailure(
                message=f"{proportion:.0%} of {group_by_narrative} "
                f"had values of {self.value} in column. "
                f"Expected at most {self.max_proportion:.0%}",
                expr=expr,
            )
        if self.min_proportion and results["proportion"] <= self.min_proportion:
            proportion = results["proportion"]
            raise DataTestFailure(
                message=f"Only {proportion:.0%} of {group_by_narrative} "
                f"had values of {self.value} in column. "
                f"Expected at least {self.min_proportion:.0%}",
                expr=expr,
            )


class ConsecutiveEntityDeletionsTest(AbstractTableTest):
    """Check for the proportion or number of rows containing any
    particular value in column

    Args:
        table_config:   The resolved table config to test
        severity:       The error type to emit on test failure
                        Defaults to AMLAITestSeverity.ERROR
        test_id:        A unique identifier for the test
    """

    def __init__(
        self, *, table_config: ResolvedTableConfig, entity_ids: List[str], test_id: str
    ) -> None:
        super().__init__(
            table_config=table_config, severity=AMLAITestSeverity.WARN, test_id=test_id
        )
        self.entity_ids = entity_ids

    def _test(self, *, connection: BaseBackend) -> None:

        counted = (
            self.table.filter(_.is_entity_deleted)
            .group_by(self.entity_ids)
            .agg(count_per_pk=_.count())
        )
        expr = counted.count(where=_.count_per_pk > 0)
        results = connection.execute(expr)
        if results > 0:
            raise DataTestFailure(
                f"{results} rows found with consecutive entity deletions. Entities"
                " should generally only be deleted once.",
                expr=expr,
            )


class OrphanDeletionsTest(AbstractTableTest):
    """Find instances where an entity (normally the table primary_key without
    validity_start_date) is deleted without having a before it was deleted.

    Args:
        table_config:   The resolved table config to test
        severity:       The error type to emit on test failure
                        Defaults to AMLAITestSeverity.ERROR
        test_id:        A unique identifier for the test
        column:         The column being tested
    """

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        entity_ids: List[str],
        test_id: Optional[str] = None,
        severity: AMLAITestSeverity = AMLAITestSeverity.WARN,
    ) -> None:
        super().__init__(table_config=table_config, severity=severity, test_id=test_id)
        self.entity_ids = entity_ids

    def _test(self, *, connection: BaseBackend) -> None:
        # Field is nullable, need to correct null values to False

        w = ibis.window(group_by=self.entity_ids, order_by="validity_start_time")
        # is_entity_deleted is a nullable field, so we assume if it is null, we
        # mean False
        cte0 = ibis.coalesce(self.table.is_entity_deleted, False)

        cte1 = self.table.select(
            *self.entity_ids,
            "validity_start_time",
            is_entity_deleted=cte0,
            previous_row_validity_start_time=_.validity_start_time.lag().over(w),
        )

        expr = cte1.filter(
            _.is_entity_deleted & _.previous_row_validity_start_time.isnull()
        )
        results = connection.execute(expr.count())
        if results > 0:
            raise DataTestFailure(
                f"{results} rows found with orphaned entity deletions. These rows had"
                " no previously values where is_entity_deleted = True",
                expr=expr.select(*self.entity_ids),
            )


class ColumnPresenceTest(AbstractColumnTest):
    """Test if the column is present.

    Optional columns are already ignored by the test wrapper, so only the
    primary exception is passed forward.

    Args:
        table_config:   The resolved table config to test
        severity:       The error type to emit on test failure
                        Defaults to AMLAITestSeverity.ERROR
        test_id:        A unique identifier for the test
        column:         The column being tested
    """

    def _test(self, *, connection: BaseBackend):
        try:
            self.table[self.column]
        except IbisTypeError as e:
            raise DataTestFailure("Missing Required Column") from e


class ColumnTypeTest(AbstractColumnTest):
    """Test if the column type matches the expected column type.

    Verification is a straight comparison The verification has a few notable caveats:
        * It is not possible to specify non-nullable embedded values
          in bigquery. Comparisons do not take the underlying nullability of
          embedded values
        * A timestamp in bigquery is always associated with a UTC timezone. In
          other databases, this is not the case. The offset is ignored. TODO:
          Review this if supporting other databases for production purposes

    Args:
        table_config:   The resolved table config to test
        severity:       The error type to emit on test failure
                        Defaults to AMLAITestSeverity.ERROR
        test_id:        A unique identifier for the test
        column:         The column being tested
    """

    class _FieldComparisonInterrupt(Exception):
        """Utility class used during recursive field comparison"""

        pass

    def _test(self, *, connection: BaseBackend):
        # connection is not used because [self.column] is a direct table
        # reference
        actual_table = self.table
        actual_type = actual_table.schema()[self.column]
        schema_data_type = self.table_config.schema[self.column]

        try:
            # We compare stripped schema types with actual types
            # to find only essential fields
            extra_fields = self._check_field_types(
                schema_data_type, actual_type, self.column
            )
            if len(extra_fields) > 0:
                warnings.warn(
                    message=DataTestWarning(
                        f"Additional fields found in struct"
                        f" Full path to the extra fields were: {extra_fields}"
                    )
                )
            return
        except ColumnTypeTest._FieldComparisonInterrupt:
            # An issue with the datatypes was encounter
            pass
        raise DataTestFailure(
            f"Column type mismatch: expected {schema_data_type},"
            f" found {actual_type}.",
        )

    @classmethod
    def _check_field_types(
        cls, expected_type: DataType, actual_type: DataType, path="", level=0
    ):
        """Attempt to determine if the schema mismatch is because of an extra
        field in a struct, including embedded structs
        and arrays. If the fields are too dissimilar, raises
        FieldComparisonInterrupt().
        """
        level += 1
        if expected_type.name != actual_type.name:
            raise ColumnTypeTest._FieldComparisonInterrupt()
        if expected_type.nullable != actual_type.nullable:

            if not actual_type.nullable:
                warnings.warn(
                    message=DataTestWarning(
                        "Schema is stricter than required: expected "
                        f"{expected_type} found {actual_type}"
                    )
                )
            if not expected_type.nullable:
                raise ColumnTypeTest._FieldComparisonInterrupt()
        extra_fields = []
        if expected_type.is_struct():
            expected_type = cast(Struct, expected_type)
            for name, actual_dtype in actual_type.items():
                expected_dtype = expected_type.get(name)
                if (
                    expected_dtype is None
                ):  # actual struct field does not exist on the expected field
                    extra_fields.append(f"{path}.{name}")
                else:
                    fields = cls._check_field_types(
                        expected_type=expected_dtype,
                        actual_type=actual_dtype,
                        path=f"{path}.{name}",
                        level=level,
                    )
                    extra_fields += fields
            # Also need to check all the fields in the expected type are present,
            # and the type
            for name, expected_dtype in expected_type.items():
                actual_dtype = actual_type.get(name)
                if actual_dtype is None:  # actual struct field does not exist
                    if expected_dtype.nullable:
                        # Ignore nullable fields, they
                        # are not required
                        continue
                    raise ColumnTypeTest._FieldComparisonInterrupt()
                # This is the name of the type, not the name of the field
                if expected_dtype.name != actual_dtype.name:
                    raise ColumnTypeTest._FieldComparisonInterrupt()

        if expected_type.is_array():
            expected_type = cast(Array, expected_type)
            extra_fields.append(
                cls._check_field_types(
                    expected_type=expected_type.value_type,
                    actual_type=actual_type.value_type,
                    path=f"{path}.",
                    level=level,
                )
            )
        # Otherwise, not a container so we don't need to check recursively
        return extra_fields


class ColumnValuesTest(AbstractColumnTest):
    """Verify the column is only ever set to one of the allowed values

    Args:
        table_config:   The resolved table config to test
        severity:       The error type to emit on test failure
                        Defaults to AMLAITestSeverity.ERROR
        test_id:        A unique identifier for the test
        column:         The column being tested
        allowed_values: List of permitted column values
    """

    def __init__(
        self,
        *,
        allowed_values: List[Any],
        table_config: ResolvedTableConfig,
        column: str,
        test_id: Optional[str] = None,
    ) -> None:
        super().__init__(table_config=table_config, column=column, test_id=test_id)
        self.allowed_values = allowed_values

    def _test(self, *, connection: BaseBackend):
        table, field = resolve_field(self.table, self.column)

        expr = table.filter(field.notin(self.allowed_values)).select(field=field)

        result = connection.execute(expr.count())

        if result > 0:
            valid_values = " ".join(self.allowed_values)
            raise DataTestFailure(
                f"{result} rows found with invalid values."
                f"Valid values are: {valid_values}.",
                expr=expr,
            )


class FieldNeverWhitespaceOnlyTest(AbstractColumnTest):
    """Verify the column is never only whitespace.

    Args:
        table_config:   The resolved table config to test
        severity:       The error type to emit on test failure
                        Defaults to AMLAITestSeverity.ERROR
        test_id:        A unique identifier for the test
        column:         The column under test which contains event ids
    """

    def _test(self, *, connection: BaseBackend):
        table, field = resolve_field(self.table, self.column)

        predicates = [field.strip() == "", *self.filter_null_parent_fields()]

        expr = table.filter(predicates)

        count_blank = connection.execute(expr.count())

        if count_blank > 0:
            raise DataTestFailure(
                f"{count_blank} rows found with whitespace-only " f"values",
                expr=expr,
            )


class FieldNeverNullTest(AbstractColumnTest):
    """Verify the field is never only null

    Args:
        table_config:   The resolved table config to test
        severity:       The error type to emit on test failure
                        Defaults to AMLAITestSeverity.ERROR
        test_id:        A unique identifier for the test
        column:         The column under test which contains event ids
    """

    def _test(self, *, connection: BaseBackend):
        table, field = resolve_field(self.table, self.column)

        predicates = [field.isnull(), *self.filter_null_parent_fields()]
        expr = table.filter(predicates)

        count_null = connection.execute(expr.count())

        if count_null > 0:
            raise DataTestFailure(
                f"{count_null} rows found with null values",
                expr=expr,
            )


class NullIfTest(AbstractColumnTest):
    """Verify that a column is always null if a provided expression
    is true

    Args:
        table_config:   The resolved table config to test
        severity:       The error type to emit on test failure
                        Defaults to AMLAITestSeverity.ERROR
        test_id:        A unique identifier for the test
        expression:     The functor returning an expression to test
        column:         The column under test which contains event ids
    """

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        column: str,
        expression: Expr,
        severity: AMLAITestSeverity = AMLAITestSeverity.ERROR,
        test_id: Optional[str] = None,
    ) -> None:
        super().__init__(
            table_config=table_config, column=column, test_id=test_id, severity=severity
        )
        self.expression = expression

    def _test(self, *, connection: BaseBackend):
        expr = self.table.filter(self.expression).filter(
            self.table[self.column].notnull()
        )
        result = connection.execute(expr.count())
        if result > 0:
            raise DataTestFailure(
                f"{result} rows which should have null column values",
                expr=expr,
            )


class CountMatchingRows(AbstractColumnTest):
    """Count the number of rows which match the provided expression
    in the provided column.

    At least one of max_number, min_number, max_proportion must always be set.
    If multiple tests fail, only one will be raised

    Args:
        table_config:   The resolved table config to test
        severity:       The error type to emit on test failure
                        Defaults to AMLAITestSeverity.ERROR
        test_id:        A unique identifier for the test
        expression:     The functor returning an expression to test
        column:         The column under test which contains event ids
        max_number:     The maximum absolute number of matching rows. Inclusive.
        min_number:     The minimum absolute number of matching rows. Inclusive.
        max_proportion: The maximum proportion of matching rows over all rows
        min_proportion: The minimum proportion of matching rows over all rows
        explanation:    A human readable explanation of the criteria
    """

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        column: str,
        expression: Callable[[Expr], Expr],
        severity: AMLAITestSeverity = AMLAITestSeverity.ERROR,
        test_id: Optional[str] = None,
        max_number: Optional[int] = None,
        min_number: Optional[int] = None,
        max_proportion: Optional[float] = None,
        min_proportion: Optional[int] = None,
        explanation: Optional[str] = None,
    ) -> None:
        super().__init__(
            table_config=table_config, column=column, severity=severity, test_id=test_id
        )
        self.expression = expression
        self.max_number = max_number
        self.min_number = min_number
        self.max_proportion = max_proportion
        self.min_proportion = min_proportion
        self.explanation = explanation

    def _test(self, *, connection: BaseBackend):
        table = self.get_latest_rows(self.table)
        expr = table.agg(
            total_rows=table.count(), matching_rows=table.count(where=self.expression)
        ).mutate(proportion=_.matching_rows / _.total_rows)
        result = connection.execute(expr).iloc[0]
        value = int(result["matching_rows"])
        proportion = result["proportion"]
        criteria_explained = (
            "criteria" if not self.explanation else f"criteria: {self.explanation}"
        )
        if self.min_number and (value < self.min_number):
            raise DataTestFailure(
                f"{value:d} rows met {criteria_explained}. "
                f"Expected at least {self.min_number:d}.",
                expr=expr,
            )
        if self.max_number and (value > self.max_number):
            raise DataTestFailure(
                f"{value:d} rows met {criteria_explained}. "
                f"Expected at most {self.max_number:d}.",
                expr=expr,
            )
        if self.max_proportion and (proportion > self.max_proportion):
            raise DataTestFailure(
                f"A high proportion ({proportion:.0%}) of rows met "
                f" {criteria_explained}. Expected at most ({self.max_proportion:.0%})",
                expr=expr,
            )
        if self.min_proportion and (proportion < self.min_proportion):
            raise DataTestFailure(
                f"A low proportion ({proportion:.0%}) of rows met "
                f" {criteria_explained}. Expected at least ({self.min_proportion:.0%})",
                expr=expr,
            )


class EventOrder(AbstractColumnTest):
    """Verify all events with an id are in a particular order

    Duplicates are ignored, provided they are also in the correct order

    Args:
        table_config:   The resolved table config to test
        severity:       The error type to emit on test failure
                        Defaults to AMLAITestSeverity.ERROR
        test_id:        A unique identifier for the test
        column:         The column under test which contains event ids
        time_column:    The timestamp column describing when event occurred
        events:         An ordered list of events. The order is tested.
    """

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        column: str,
        time_column: str,
        events: list[str],
        group_by: list[str],
        severity: AMLAITestSeverity = AMLAITestSeverity.ERROR,
        test_id: Optional[str] = None,
    ) -> None:
        super().__init__(
            table_config=table_config, column=column, severity=severity, test_id=test_id
        )
        self.time_column = time_column
        self.column = column
        self.events = events
        self.group_by = group_by

    @property
    def id(self):
        return self.column

    def _test(self, *, connection: BaseBackend):
        # Allow callable to be passed in for expressions which cannot be generated at
        # runtime
        field = self.table[self.column]
        time_column = self.table[self.time_column]
        kwargs = {}
        for e in self.events:
            kwargs[f"{e}_min"] = time_column.min(where=field == e)
            kwargs[f"{e}_max"] = time_column.max(where=field == e)

        expr = self.table.group_by(self.group_by).agg(**kwargs)

        comparisons = []
        # Look for cases where the maximum of the earlier order is greater than
        # the first of the minimum events. The combinations are without
        # replacement so will build comparisons from left to right
        for first, compare in itertools.combinations(self.events, 2):
            comparisons.append(expr[f"{first}_max"] > expr[f"{compare}_min"])

        expr = expr.filter(functools.reduce(lambda x, y: x | y, comparisons))
        result = connection.execute(expr.count())
        if result > 0:
            raise DataTestFailure(
                f"{result} did not fulfil the required event order. "
                f"Expected events to always be in the order {self.events}",
                expr=expr,
            )


class AcceptedRangeTest(AbstractColumnTest):
    """Test a column value lies inside a particular range

    Args:
        table_config: The resolved table config to test
        severity:   The error type to emit on test failure
                    Defaults to AMLAITestSeverity.ERROR
        test_id:    A unique identifier for the test. Useful when used via
                    @pytest.parameter as it allows us to uniquely identify the
                    test
        column:     The column under test which contains event ids
        min_value:  The minimum allowance value of the column (inclusive)
        max_value:  The maximum allowable value of the column (inclusive)
    """

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        column: str,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None,
        severity: AMLAITestSeverity = AMLAITestSeverity.ERROR,
        test_id: Optional[str] = None,
    ) -> None:
        super().__init__(
            table_config=table_config, column=column, severity=severity, test_id=test_id
        )
        self.min_value = min_value
        self.max_value = max_value

    def _test(self, *, connection: BaseBackend):
        table, field = resolve_field(self.table, self.column)

        min_pred = field < self.min_value if self.min_value is not None else False
        max_pred = field > self.max_value if self.max_value is not None else False

        predicates = [min_pred | max_pred]
        expr = table.filter(predicates)

        result = connection.execute(expr.count())
        if result > 0:
            raise DataTestFailure(
                f"{result} rows were outside"
                f" of inclusive range {self.min_value} - {self.max_value}",
                expr=expr,
            )


class ReferentialIntegrityTest(AbstractTableTest):
    """_summary_

    max_proportion: allows a certain proportion of the number of missing keys.
    Defaults to 0

    Args:
        AbstractTableTest: _description_
    """

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        to_table_config: ResolvedTableConfig,
        max_proportion: Optional[float] = None,
        keys: list[str],
        severity=AMLAITestSeverity.ERROR,
        test_id: Optional[str] = None,
    ) -> None:
        super().__init__(table_config=table_config, severity=severity, test_id=test_id)
        self.to_table_config = to_table_config
        self.to_table = to_table_config.table
        self.keys = keys
        self.max_proportion = max_proportion

    def _test(self, *, connection: BaseBackend):
        # The superclass does not skip the test if the to_table is optional,
        # which it may be. If it is, skip the test.
        self.check_table_exists(
            connection=connection, table_config=self.to_table_config
        )
        expr = self.table.select(*[self.keys]).anti_join(self.to_table, self.keys)

        key_columns = {" ".join(self.keys)}

        if self.max_proportion:
            # Join on the total distinct count of keys
            total_key_cnt = self.table[self.keys].nunique().name("total_key_cnt")
            subexpr = expr.agg(missing_key_count=_[self.keys].count()).select(
                proportion=_.missing_key_count / total_key_cnt
            )
            result = connection.execute(subexpr).iloc[0]["proportion"]
            if result > 0:
                msg = (
                    f"More than {result:.0%} of keys {self.keys} in table "
                    f"{self.table.get_name()} were not in "
                    f"{self.to_table.get_name()}. "
                    f"Key column(s) were {key_columns}"
                    ""
                )
                raise DataTestFailure(msg, expr=expr)

        result = connection.execute(expr.count())
        if result > 0:
            msg = (
                f"{result} keys found in table {self.table.get_name()} "
                f"which were not in {self.to_table.get_name()}. "
                f"Key column(s) were {key_columns}"
            )
            raise DataTestFailure(msg, expr=expr)
        return


class TemporalProfileTest(AbstractColumnTest):
    """_summary_

    max_proportion: allows a certain proportion of the number of missing keys.
    Defaults to 0

    Args:
        AbstractTableTest: _description_
    """

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        column: str,
        period: Literal["MONTH"],
        threshold: float,
        test_id: Optional[str] = None,
        severity: AMLAITestSeverity = AMLAITestSeverity.ERROR,
    ) -> None:
        super().__init__(
            table_config=table_config, severity=severity, test_id=test_id, column=column
        )
        if period == "MONTH":
            self.strf_string = "%Y-%m"
        else:
            raise ValueError("Unsupported period provided")
        self.threshold = threshold
        self.period = period

    def _test(self, *, connection: BaseBackend):
        # The superclass does not skip the test if the to_table is optional,
        # which it may be. If it is, skip the test.
        table, column = resolve_field(table=self.table, column=self.column)
        expr = table.group_by(mnth=column.strftime(self.strf_string)).agg(cnt=_.count())
        expr = expr.mutate(proportion=_.cnt / _.cnt.mean().over())

        expr = expr.filter(_.proportion < self.threshold)

        result = connection.execute(expr.count())
        if result > 0:
            msg = (
                f"{result} {self.period.lower()}s had a volume of less than "
                f"{self.threshold:.0%} of the average volume for all "
                f"{self.period.lower()}s"
            )
            raise DataTestFailure(msg, expr=expr)


class TemporalReferentialIntegrityTest(AbstractTableTest):
    """Validate that tables are referentially integral over times

    A temporal validation involves validating the specified table against
    entries in a to_table. For example, a customer event table might be
    validated against a customer table. Events should not happen to a customer
    before or after they existed.

    There are two types of table to validated


    Args:
        table_config            : The table to be validated. Columns in this
                                    dataset will be validated against the
                                    to_table to check the columns are not out of
                                    scope with the range of corresponding dates
        to_table_config         : The table to validate against. The dates in
                                    this table are used as a source of truth for
                                    the range of dates and times the events
                                    could have existed over, and are not
                                    directly checked against the table_config.
        validate_datetime_column: The column to validate. Defaults to
                                    "validity_start_time"
        key                     : The unique identifier to validate across in
                                    both tables. For example, if account_id, the
                                    table will have the range of values in
                                    validate_datetime_column validated against
                                    the range of dates presented by the first
                                    and last validity_start_time values of
                                    to_table
        tolerance               : An order of magnitude to validate against. For
                                    example, if 'day', inconsistencies
                                    of less than a day will be ignored and will
                                    not result in a test failure
    """

    MAX_DATETIME_VALUE = datetime.datetime(9995, 1, 1, tzinfo=datetime.timezone.utc)

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        to_table_config: ResolvedTableConfig,
        key: str,
        validate_datetime_column: Optional[str] = None,
        # BQ doesn't support values longer than a day
        tolerance: Optional[
            Literal[
                "day",
                "hour",
                "minute",
                "second",
                "millisecond",
                "microsecond",
                "nanosecond",
            ]
        ] = None,
        severity: AMLAITestSeverity = AMLAITestSeverity.ERROR,
        test_id: Optional[str] = None,
    ) -> None:

        super().__init__(table_config=table_config, severity=severity, test_id=test_id)
        self.to_table = to_table_config.table
        self.validate_datetime_column = validate_datetime_column
        self.to_table_config = to_table_config
        self.tolerance = tolerance
        self.key = key

    def get_entity_state_windows(self, table_config: ResolvedTableConfig):
        # For a table with flipping is_entity_deleted:
        #
        # | party_id | validity_start_time | is_entity_deleted |
        # |    1     |      00:00:00       |       False       |
        # |    1     |      00:30:00       |       False       | <--- no flip
        # |    1     |      01:00:00       |       True        |
        # |    1     |      02:00:00       |       False       |
        #
        # |party_id|window_id| window_start_time|window_end_time|is_entity_deleted
        # |   1    |    0    |      00:00:00    |     null      |     False
        table = table_config.table

        # Select potentially overlapping keys between these two entity tables
        keys = set([self.key, *table_config.entity_keys])

        if table_config.table_type == TableType.EVENT:
            return table.select(first_date=_.event_time, last_date=_.event_time, *keys)

        w = ibis.window(group_by=keys, order_by="validity_start_time")

        # is_entity_deleted is a nullable field, so we assume if it is null, we
        # mean False
        cte0 = ibis.coalesce(table.is_entity_deleted, False)
        # First, find the changes in entity_deleted switching in order to
        # determine the rows which lead to the table switching. do this by
        # finding the previous values of "is_entity_deleted" and determining if.

        cte1 = table.select(
            *keys,
            "validity_start_time",
            is_entity_deleted=cte0,
            previous_entity_deleted=cte0.lag().over(w),
            next_row_validity_start_time=_.validity_start_time.lead().over(w),
            previous_row_validity_start_time=_.validity_start_time.lag().over(w),
        )

        # Handle the entity being immediately deleted by assuming it only
        # existed for a fraction of a second. This isn't valid data, but it
        # should be picked up by the entity mutation tests
        cte2 = cte1.mutate(
            previous_row_validity_start_time=ibis.ifelse(
                (_.previous_row_validity_start_time.isnull()) & (_.is_entity_deleted),
                _.validity_start_time,
                _.previous_row_validity_start_time,
            )
        )

        # Only return useful rows, not ones where the entities deletion state
        # didn't change
        cte3 = cte2.filter(
            (_.previous_row_validity_start_time == ibis.literal(None))  # first row
            | (_.next_row_validity_start_time == ibis.literal(None))  # last row
            | (_.is_entity_deleted != _.previous_entity_deleted)  # state flips
        ).group_by(keys)

        if table_config.table_type == TableType.CLOSED_ENDED_ENTITY:
            # For closed ended entities, e.g. parties, we need to assume the
            # entity persists until it is deleted. This means that the maximum
            # validity date time
            res = (
                cte3
                # At the moment, we only pay attention to the first/last dates,
                # not where there are multiple flips if the only row and the row
                # isn't yet deleted, we need to make the validity end time far
                # into the future
                .agg(
                    # null handling not required as validity_start_time is a
                    # non-nullable field
                    first_date=_.validity_start_time.min(),
                    last_date=ibis.ifelse(
                        condition=_.next_row_validity_start_time.isnull()
                        & ~_.is_entity_deleted,
                        true_expr=TemporalReferentialIntegrityTest.MAX_DATETIME_VALUE,
                        false_expr=_.validity_start_time,
                    ).max(),
                )
            )
        else:
            # The entity's validity is counted only as of the last datetime provided
            res = (
                cte3
                # At the moment, we only pay attention to the first/last dates,
                # not where there are multiple flips
                .agg(
                    first_date=_.validity_start_time.min(),
                    last_date=_.validity_start_time.max(),
                )
            )
        # There may be multiple group_by keys, which doesn't correspond
        # to the referential integrity test testing a single key. This is because
        # we need to handle the case where there are multiple entity keys,
        # e.g. a party_link table where we are validating the account_id
        return res.group_by(self.key).agg(
            first_date=_.first_date.min(), last_date=_.last_date.max()
        )

    def _test(self, *, connection: BaseBackend):
        # Table 1
        # |party_id|window_id|window_start_time|window_end_time|is_entity_deleted
        # |   1    |    0    |     00:00:00    |     01:00:00  |     False
        # |   1    |    1    |     01:00:00    |     02:00:00  |     True
        # |   1    |    2    |     02:00:00    |       null    |     False
        #
        # Table 2
        # |party_id|window_id|window_start_time|window_end_time|is_entity_deleted
        # |   1    |    0    |     00:00:00    |     01:00:00  |     False
        # |   1    |    1    |     01:00:00    |     02:00:00  |     True
        # |   1    |    2    |     02:00:00    |       null    |     False
        # Get the first and last validity periods of the entities in both tables
        if self.validate_datetime_column:
            # If a different column is specified for validation,
            # then obtain the latest row and then validate it
            latest_rows = self.get_latest_rows(
                self.table_config.table, self.table_config
            )
            tbl = latest_rows.group_by(self.key).aggregate(
                first_date=_[self.validate_datetime_column].min(),
                last_date=_[self.validate_datetime_column].max(),
            )
        else:
            tbl = self.get_entity_state_windows(self.table_config)
        totbl = self.get_entity_state_windows(self.to_table_config)
        # First, associate keys by joining
        # We want to find items where the
        # tbl=party_id ---
        # totbl=party_account_id_link table ---
        # Want to find cases where:
        # a) windows do not overlap
        # ----
        # |
        # | tbl
        # |
        # ----
        #      ---- first_date
        #      |
        #      | totbl
        #      |
        #      ---- last_date
        # b) windows do overlap, but the verification table has the entity deleted
        #    period to the totbl
        # ---- first_date
        # |               ---- first_date
        # | tbl           |
        # |               | totbl
        # ---- last_date  |
        #                 ---- last_date
        # c) windows do overlap
        #                 ---- first_date
        # ---- first_date |
        # |               | totbl
        # | tbl           ---- last_date
        # |
        # ---- last_date

        # Avoid overlapping tablenames
        tbl = tbl.alias("table")
        totbl = totbl.alias("validation_table")

        # Find elements which do not overlap
        first_date_with_tolerance = (
            totbl.first_date.sub(ibis.interval(1, self.tolerance))
            if self.tolerance
            else totbl.first_date
        )
        last_date_with_tolerance = (
            totbl.last_date.add(ibis.interval(1, self.tolerance))
            if self.tolerance
            else totbl.last_date
        )

        expr = (
            tbl.join(
                right=totbl,
                predicates=(
                    (tbl[self.key] == totbl[self.key])
                    # If this item is before the first date on the base table
                    & (
                        ~tbl.first_date.between(
                            lower=first_date_with_tolerance,
                            upper=last_date_with_tolerance,
                        )
                        |
                        # If this item is after the last date on the base table
                        ~tbl.last_date.between(
                            lower=first_date_with_tolerance,
                            upper=last_date_with_tolerance,
                        )
                    )
                ),
            )
            .mutate(
                last_date=ibis.ifelse(
                    condition=_.last_date == self.MAX_DATETIME_VALUE,
                    true_expr=None,
                    false_expr=_.last_date,
                ),
                last_date_right=ibis.ifelse(
                    condition=_.last_date_right == self.MAX_DATETIME_VALUE,
                    true_expr=None,
                    false_expr=_.last_date_right,
                ),
            )
            .rename(
                {
                    f"first_date_{self.table_config.name}": "first_date",
                    f"last_date_{self.table_config.name}": "last_date",
                    f"first_date_{self.to_table_config.name}": "first_date_right",
                    f"last_date_{self.to_table_config.name}": "last_date_right",
                }
            )
        )

        result = connection.execute(expr=expr.count())
        if result > 0:
            msg = (
                f"{result} values of {self.key} found in the table which were "
                f"either not in {self.to_table_config.name}, or had inconsistent "
                "time periods, where validity_start_time and is_entity_deleted "
                f"columns in {self.table_config.name} did not correspond to the time "
                f"periods defined by validity_start_time and is_entity_deleted "
                f"columns in {self.to_table_config.name}"
            )
            if self.validate_datetime_column:
                msg = (
                    f"{result} values of {self.key} found in the table which were "
                    f"either not in {self.to_table_config.name}, or had inconsistent "
                    f"time periods, where the {self.validate_datetime_column} "
                    f"column in {self.table_config.name} did not correspond to the "
                    f"time periods defined by validity_start_time and "
                    f"is_entity_deleted columns in {self.to_table_config.name}"
                )
            raise DataTestFailure(msg, expr=expr)
        return


class ConsistentIDsPerColumn(AbstractColumnTest):
    """Verify the number of IDs is consistent for each column"""

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        column: str,
        id_to_verify: str,
        severity: AMLAITestSeverity = AMLAITestSeverity.ERROR,
        test_id: Optional[str] = None,
    ) -> None:
        """_summary_

        Args:
            table_config: _description_
            column: _description_
            id_to_verify: _description_
            severity: _description_. Defaults to AMLAITestSeverity.ERROR.
            test_id: _description_. Defaults to None.
        """
        super().__init__(
            table_config=table_config, column=column, severity=severity, test_id=test_id
        )
        self.id_to_verify = id_to_verify

    @property
    def id(self):
        return self.column

    def _test(self, *, connection: BaseBackend):
        # Allow callable to be passed in for expressions which cannot be generated at
        # runtime
        table = self.get_latest_rows(self.table)

        expr = (
            table.group_by(by=self.column)
            .order_by(self.id_to_verify)
            .agg(
                # We cannot group by or count distinct by columns in bq
                ids=_[self.id_to_verify].collect()
            )
            .mutate(ids=_.ids.sort().join("|"))
        )

        result = connection.execute(expr["ids"].nunique())
        if result > 1:
            raise DataTestFailure(
                f"Inconsistent {self.id_to_verify} across column. "
                f"Expected all {self.column} to have the same same set of IDs",
                expr=expr,
            )
