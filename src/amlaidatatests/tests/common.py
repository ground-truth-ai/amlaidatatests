import datetime
import itertools
import warnings
from functools import reduce
from typing import Any, Callable, List, Literal, Optional, cast, override

import ibis
from ibis import BaseBackend, Expr, _
from ibis.common.exceptions import IbisTypeError
from ibis.expr.datatypes import Array, DataType, Struct, Timestamp

from amlaidatatests.base import (
    AbstractColumnTest,
    AbstractTableTest,
    resolve_field,
    resolve_field_to_level,
)
from amlaidatatests.exceptions import AMLAITestSeverity, FailTest, WarnTest
from amlaidatatests.schema.base import ResolvedTableConfig, TableType


class TableExcessColumnsTest(AbstractTableTest):
    """Verify there are no excess columns on the table

    Args:
        AbstractTableTest: _description_
    """

    def __init__(self, table_config: ResolvedTableConfig, test_id: str) -> None:
        super().__init__(table_config, test_id=test_id)

    def _test(self, *, connection: BaseBackend):
        actual_columns = set(connection.table(name=self.table.get_name()).columns)
        schema_columns = set(self.table_config.table.columns)
        excess_columns = actual_columns.difference(schema_columns)
        if len(excess_columns) > 0:
            raise WarnTest(
                f"{len(excess_columns)} unexpected columns found in table"
                f" {self.table.get_name()}"
            )
        # Schema table


class TableCountTest(AbstractTableTest):
    """_summary_

    Args:
        AbstractTableTest: _description_
    """

    def __init__(
        self,
        table_config: ResolvedTableConfig,
        max_rows: int,
        severity: AMLAITestSeverity = AMLAITestSeverity.ERROR,
        test_id: Optional[str] = None,
    ) -> None:
        """_summary_

        Args:
            table_config: _description_
            max_rows_factor: _description_
            severity: _description_. Defaults to AMLAITestSeverity.ERROR.
        """
        self.max_rows = max_rows
        super().__init__(table_config, severity, test_id=test_id)

    def _test(self, *, connection: BaseBackend):
        count = connection.execute(self.table.count())
        if count == 0:
            raise FailTest(f"Table {self.table.get_name()} is empty")
        if count > self.max_rows:
            raise FailTest(
                f"Table {self.table.get_name()} has more rows than seems feasible: "
                f"{count} vs maximum {self.max_rows}. To stop this error triggering, review "
                "the data provided or increase the scale setting"
            )
        if count > (self.max_rows) * 0.9:
            raise WarnTest(
                f"Table {self.table.get_name()} is close to the feasibility ceiling: "
                f"{count} vs maximum {self.max_rows}. To stop this error triggering, review "
                "the data provided or increase the scale setting"
            )


class PrimaryKeyColumnsTest(AbstractTableTest):
    """_summary_

    Args:
        AbstractTableTest: _description_
    """

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        unique_combination_of_columns: List[str],
        test_id: Optional[str] = None
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
            raise FailTest(f"Found {n_total - n_pairs} duplicate values")


class ColumnCardinalityTest(AbstractColumnTest):
    """Check for the number of values of column, optionally
    grouped by group_by.

    For example:
        if column = party_id, group_by = None
            - Check for the number of distinct party_id
        if column = party_id, group_by = ["account_id"]
            - Check for the number of distinct party_ids
                for each account


    Args:
        AbstractTableTest: _description_
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
    ) -> None:
        super().__init__(table_config=table_config, column=column, severity=severity)
        self.max_number = max_number
        self.min_number = min_number
        self.test_id = test_id
        self.group_by = group_by
        self.where = where
        self.having = having

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
                f"column {self.full_column_path} has an "
                f"unexpectedly {direction} number of distinct values "
                f"for each {self.group_by} "
                if self.group_by
                else ""
            )
            raise FailTest(
                message=message,
                expr=expr,
            )


class CountFrequencyValues(AbstractColumnTest):
    """Check for the proportion or number of rows containing any
    particular value in column

    Args:
        AbstractTableTest: _description_
    """

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        column: str,
        test_id: Optional[str] = None,
        proportion: Optional[float] = None,
        max_number: Optional[int] = None,
        where: Optional[Callable[[Expr], Expr]] = None,
        having: Optional[Callable[[Expr], Expr]] = None,
        severity: AMLAITestSeverity = AMLAITestSeverity.WARN,
        group_by: Optional[list[str]] = None,
    ) -> None:
        """_summary_

        Args:
            table_config: _description_
            entity_ids: _description_
            warn: _description_. Defaults to 500.
            error: _description_. Defaults to 1000.

        Raises:
            ValueError: _description_
        """
        super().__init__(
            table_config=table_config, column=column, severity=severity, test_id=test_id
        )
        if (proportion) and any([max_number]):
            raise ValueError("Only proportion or number must be set, not both")
        if not any([proportion, max_number]):
            raise ValueError("One of proportion and number must be set")
        if (proportion) and (proportion < 0 or proportion > 1):
            raise ValueError("Proportion must be between 0 and 1")
        self.proportion = proportion
        self.max_number = max_number
        self.where = where
        self.having = having
        self.group_by = group_by if group_by else []

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
        table, column = resolve_field(table=table, column=self.column)

        grp_columns = []
        for grp in self.group_by:
            _, col = resolve_field(table=table, column=grp)
            grp_columns.append(col)

        expr = table.group_by([column, *grp_columns]).agg(value_cnt=table.count())
        expr = expr.mutate(
            proportion=expr.value_cnt / expr.value_cnt.sum().over(group_by=grp_columns)
        )

        boolean_expr = None
        if self.proportion:
            boolean_expr |= expr.proportion >= self.proportion
        if self.max_number:  # if self.number: - checked during init
            boolean_expr |= expr.value_cnt > self.max_number

        expr = expr.filter(boolean_expr)

        if self.having is not None:
            expr = expr.filter(self.having)

        results = connection.execute(expr.count())

        if results > 0:
            raise FailTest(
                message=f"{results} values of {self.full_column_path} "
                "appeared unusually frequently",
                expr=expr,
            )


class VerifyTypedValuePresence(AbstractColumnTest):
    """Checks for the proportion or number of rows containing any
    particular value in column relative to group_by. This is mainly used
    for verifying the presence of a value in a column across a table.

    For example:
    group_by = account_id, min_number = 1, column = type, value = CARD,
    max_proportion = 0.1
    will:
        * Count the number of account_id which have a type = CARD (value_cnt)
        * Count the proportion of account_id which have
        * Error if the value_cnt is not at least 1
        * Error if max_proportion of account_id having a type = CARD is greater
            than 10%.

    Args:
        table_config: _description_
        column: _description_
        value: _description_
        group_by: list of column_ids to group by
        test_id: _description_. Defaults to None.
        min_number: _description_. Defaults to None.
        max_number: _description_. Defaults to None.
        max_proportion: _description_. Defaults to None.
        min_proportion: _description_. Defaults to None.
        where: _description_. Defaults to None.
        severity: _description_. Defaults to AMLAITestSeverity.WARN.
    """

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        column: str,
        value: str,
        group_by: list[str],
        test_id: Optional[str] = None,
        min_number: Optional[int] = None,
        max_number: Optional[int] = None,
        max_proportion: Optional[float] = None,
        min_proportion: Optional[float] = None,
        where: Optional[Callable[[Expr], Expr]] = None,
        severity: AMLAITestSeverity = AMLAITestSeverity.WARN,
    ) -> None:
        super().__init__(table_config=table_config, column=column, severity=severity)
        self.min_number = min_number
        self.max_number = max_number
        self.max_proportion = max_proportion
        self.min_proportion = min_proportion
        self.test_id = test_id
        self.value = value
        self.group_by = group_by
        self.where = where

    def _test(self, *, connection: BaseBackend) -> None:
        table = self.table
        if self.table_config.table_type in (
            TableType.CLOSED_ENDED_ENTITY,
            TableType.OPEN_ENDED_ENTITY,
        ):
            # For these tables, we need to identify the latest row
            table = self.get_latest_rows(table)
        table, column = resolve_field(table=table, column=self.column)

        where_group_kwargs = {"where": self.where(_)} if self.where else {}

        expr = (
            # Concatenate the group by so we can count unique combinations
            table.mutate(
                concat=reduce(lambda x, y: x + y, [_[i] for i in self.group_by], "")
            )
            .agg(
                value_cnt=_["concat"].nunique(column == self.value),
                group_count=_["concat"].nunique(**where_group_kwargs),
            )
            .mutate(proportion=_.value_cnt / _.group_count)
        )
        results = connection.execute(expr).iloc[0]
        value_cnt = int(results["value_cnt"])
        proportion = results["proportion"]

        if self.min_number and results["value_cnt"] < self.min_number:
            msg = "Only {value_cnt:d} rows"
            if self.min_number == 1:
                msg = f"No rows "
            raise FailTest(
                message=f"{msg} found "
                f"with a value of {self.value} in {self.full_column_path}. "
                f"Expected at least {self.min_number}",
                expr=expr,
            )
        if self.max_number and results["value_cnt"] > self.max_number:
            raise FailTest(
                message=f"Too many ({value_cnt:d}) rows {self.group_by} found "
                f"with a value of {self.value} in {self.full_column_path}. "
                f"Expected at most {self.max_number}",
                expr=expr,
            )
        if self.max_proportion and results["proportion"] >= self.max_proportion:
            raise FailTest(
                message=f"{proportion:.0%} of {self.group_by} "
                f"had values of {self.value} in {self.full_column_path}. "
                f"Expected at most {self.max_proportion:.0%}",
                expr=expr,
            )
        if self.min_proportion and results["proportion"] <= self.min_proportion:
            proportion = results["proportion"]
            raise FailTest(
                message=f"Only {proportion:.0%} of {self.group_by} "
                f"had values of {self.value} in {self.full_column_path}. "
                f"Expected at least {self.min_proportion:.0%}",
                expr=expr,
            )


class ConsecutiveEntityDeletionsTest(AbstractTableTest):
    """_summary_

    Args:
        AbstractTableTest: _description_
    """

    def __init__(
        self, *, table_config: ResolvedTableConfig, entity_ids: List[str]
    ) -> None:
        """_summary_

        Args:
            table_config: _description_
            entity_ids: _description_
        """
        super().__init__(table_config=table_config, severity=AMLAITestSeverity.WARN)
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
            raise FailTest(
                f"{results} rows found with consecutive entity deletions. Entities"
                " should generally only be deleted once.",
                expr=expr,
            )


class OrphanDeletionsTest(AbstractTableTest):
    """_summary_

    Args:
        AbstractTableTest: _description_
    """

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        entity_ids: List[str],
        severity: AMLAITestSeverity = AMLAITestSeverity.WARN,
    ) -> None:
        """_summary_

        Args:
            table_config: _description_
            entity_ids: _description_
            severity: _description_. Defaults to AMLAITestSeverity.WARN.
        """
        super().__init__(table_config=table_config, severity=severity)
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
            raise FailTest(
                f"{results} rows found with orphaned entity deletions. These rows had"
                " no previously values where is_entity_deleted = True",
                expr=expr.select(*self.entity_ids),
            )


class ColumnPresenceTest(AbstractColumnTest):

    def _test(self, *, connection: BaseBackend):
        try:
            self.table[self.column]
        except IbisTypeError as e:
            raise FailTest(f"Missing Required Column: {self.full_column_path}") from e


class FieldComparisonInterrupt(Exception):
    """_summary_

    Args:
        Exception: _description_
    """

    pass


class ColumnTypeTest(AbstractColumnTest):
    """_summary_

    Args:
        AbstractColumnTest: _description_
    """

    def _test(self, *, connection: BaseBackend):
        """_summary_

        Args:
            table (Table): _description_
            schema (Schema): _description_

        Raises:
            FailTest: _description_
        """
        actual_table = self.table
        actual_type = actual_table.schema()[self.column]
        schema_data_type = self.table_config.schema[self.column]

        # Check the child types
        if self._strip_type_for_comparison(
            actual_type
        ) != self._strip_type_for_comparison(schema_data_type):
            # First, check if the difference might just be due to excess fields
            # in structs
            try:
                extra_fields = self._find_extra_struct_fields(
                    schema_data_type, actual_type, self.column
                )
                # If no expcetion
                warnings.warn(
                    message=WarnTest(
                        f"Additional fields found in struct in {self.full_column_path}."
                        f" Full path to the extra fields were: {extra_fields}"
                    )
                )
                return
            except FieldComparisonInterrupt:
                pass
            if schema_data_type.nullable and not actual_type.nullable:
                warnings.warn(
                    message=WarnTest(
                        "Schema is stricter than required: expected column"
                        f" {self.full_column_path} to be {schema_data_type}, "
                        f"found {actual_type}"
                    )
                )
                return
            raise FailTest(
                f"Expected column {self.full_column_path} to be {schema_data_type},"
                f" found {actual_type}",
            )

    @classmethod
    def _find_extra_struct_fields(
        cls, expected_type: DataType, actual_type: DataType, path="", level=0
    ):
        """Attempt to determine if the schema mismatch is because of an extra
        field in a struct, including embedded structs
        and arrays. If the fields are too dissimilar, raises
        FieldComparisonInterrupt().
        """
        level += 1
        if level == 1 and (expected_type.nullable != actual_type.nullable):
            # Don't get in the way of nullability checks for the top level fields
            # so assume not nullable
            raise FieldComparisonInterrupt()
        if expected_type.name != actual_type.name:
            raise FieldComparisonInterrupt()
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
                    fields = cls._find_extra_struct_fields(
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
                if (
                    actual_dtype is None
                ):  # actual struct field does not exist on the expected field
                    raise FieldComparisonInterrupt()
                if expected_dtype.name != actual_dtype.name:
                    raise FieldComparisonInterrupt()

        if expected_type.is_array():
            expected_type = cast(Array, expected_type)
            extra_fields.append(
                cls._find_extra_struct_fields(
                    expected_type=expected_type.value_type,
                    actual_type=actual_type.value_type,
                    path=f"{path}.",
                    level=level,
                )
            )
        # Otherwise, not a container so we don't need to check recursively
        return extra_fields

    @classmethod
    def _strip_type_for_comparison(cls, a: DataType, level=0) -> Struct:
        """Recursively strip nulls for type comparison"""
        level = level + 1
        # We can't check lower level nullable columns because
        # it's not possible to specify these fields as non-nullable
        nullable = a.nullable if level == 1 else True
        dct = {}
        if a.is_struct():
            a = cast(Struct, a)
            for n, dtype in a.items():
                dct[n] = cls._strip_type_for_comparison(dtype, level)
            # Now normalize the keys so the order is consistent for comparison
            dct = dict(sorted(dct.items()))
            return Struct(nullable=nullable, fields=dct)
        if a.is_array():
            value = cls._strip_type_for_comparison(a.value_type, level)
            return Array(nullable=nullable, value_type=value)
        if a.is_timestamp():
            a = cast(Timestamp, a)
            # In bigquery, the timestamp datatype is always in UTC
            # so this shouldn't happen, but it could happen in another
            # database
            if a.timezone and a.timezone != "UTC":
                warnings.warn(
                    f"Timezone of column {a.name} is not UTC. This "
                    "could cause problems with bigquery, since the timezone "
                    "is always UTC"
                )
            # Scale varies by database
            return a.copy(nullable=nullable, scale=None, timezone=None)
        return a.copy(nullable=nullable)


class ColumnValuesTest(AbstractColumnTest):

    def __init__(
        self,
        *,
        values: List[Any],
        table_config: ResolvedTableConfig,
        column: str,
        test_id: Optional[str] = None,
    ) -> None:
        super().__init__(table_config=table_config, column=column, test_id=test_id)
        self.values = values

    @override
    def _test(self, *, connection: BaseBackend):
        table, field = resolve_field(self.table, self.column)

        expr = table.filter(field.notin(self.values)).select(field=field)

        result = connection.execute(expr.count())

        if result > 0:
            valid_values = " ".join(self.values)
            raise FailTest(
                f"{result} rows found with invalid values in {self.full_column_path}"
                f" Valid values are: {valid_values}.",
                expr=expr,
            )


class FieldNeverWhitespaceOnlyTest(AbstractColumnTest):

    def filter_null_parent_fields(self):
        # If subfields exist (struct or array), we need to compare the nullness of
        # the parent
        # as well - it doesn't make any sense to check the parent if the child
        # is also null
        predicates = []
        if self.column.count(".") >= 1:
            _, parent_field = resolve_field_to_level(self.table, self.column, -1)
            # We want to check for cases only where field is null but its parent isn't
            predicates += [parent_field.notnull()]
        return predicates

    def _test(self, *, connection: BaseBackend):
        """_summary_

        Args:
            table (Table): _description_
            schema (Schema): _description_

        Raises:
            FailTest: _description_
        """
        table, field = resolve_field(self.table, self.column)

        predicates = [field.strip() == "", *self.filter_null_parent_fields()]

        expr = table.filter(predicates)

        count_blank = connection.execute(expr.count())

        if count_blank == 0:
            return
        raise FailTest(
            f"{count_blank} rows found with whitespace-only "
            f"values of {self.full_column_path} in table {self.table.get_name()}",
            expr=expr,
        )


class FieldNeverNullTest(FieldNeverWhitespaceOnlyTest):

    def _test(self, *, connection: BaseBackend):
        """_summary_

        Args:
            table (Table): _description_
            schema (Schema): _description_

        Raises:
            FailTest: _description_
        """
        table, field = resolve_field(self.table, self.column)

        predicates = [field.isnull(), *self.filter_null_parent_fields()]
        expr = table.filter(predicates)

        count_null = connection.execute(expr.count())

        if count_null == 0:
            return
        raise FailTest(
            f"{count_null} rows found with null values of "
            f"{self.full_column_path} in table {self.table.get_name()}",
            expr=expr,
        )


class DatetimeFieldNeverJan1970Test(FieldNeverWhitespaceOnlyTest):

    def _test(self, *, connection: BaseBackend):
        """_summary_

        Args:
            table (Table): _description_
            schema (Schema): _description_

        Raises:
            FailTest: _description_
        """
        table, field = resolve_field(self.table, self.column)

        predicates = [field.epoch_seconds() == 0, *self.filter_null_parent_fields()]
        expr = table.filter(predicates)

        count_null = connection.execute(expr.count())

        if count_null == 0:
            return
        raise FailTest(
            f"{count_null} rows found with date on 1970-01-01 in "
            f"{self.full_column_path}. This value is often nullable",
            expr=expr,
        )


class NullIfTest(AbstractColumnTest):

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
            raise FailTest(
                f"{result} rows not fulfilling criteria in {self.full_column_path}",
                expr=expr,
            )


class CountMatchingRows(AbstractColumnTest):

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        column: str,
        expression: Callable[[Expr], Expr],
        severity: AMLAITestSeverity = AMLAITestSeverity.ERROR,
        test_id: Optional[str] = None,
        max_rows: Optional[int] = None,
        min_number: Optional[int] = None,
        max_proportion: Optional[float] = None,
        min_proportion: Optional[int] = 0,
    ) -> None:
        super().__init__(
            table_config=table_config, column=column, severity=severity, test_id=test_id
        )
        self.expression = expression
        self.max_rows = max_rows
        self.min_rows = min_number
        self.max_proportion = max_proportion
        self.max_proportion = min_proportion

    def _test(self, *, connection: BaseBackend):
        table = self.get_latest_rows(self.table)
        expr = table.agg(
            total_rows=table.count(), matching_rows=table.count(where=self.expression)
        ).mutate(proportion=_.matching_rows / _.total_rows)
        result = connection.execute(expr).iloc[0]
        value = int(result["matching_rows"])
        proportion = result["proportion"]
        if self.min_rows and (value < self.min_rows):
            raise FailTest(
                f"{value:d} rows met specified criteria "
                f"in {self.full_column_path}. Expected at least "
                f"{self.min_rows:d}.",
                expr=expr,
            )
        if self.max_rows and (value > self.max_rows):
            raise FailTest(
                f"{value:d} rows met specified criteria "
                f"in {self.full_column_path}. Expected at most "
                f"{self.max_rows:d}.",
                expr=expr,
            )
        if self.max_proportion and (proportion > self.max_proportion):
            raise FailTest(
                f"A high proportion ({proportion:.0%}) of rows met "
                "specified criteria "
                f" in {self.full_column_path}",
                expr=expr,
            )


class EventOrder(AbstractColumnTest):
    """_summary_

    Args:
        table_config: _description_
        column: _description_
        time_column: _description_
        events: _description_
        severity: _description_. Defaults to AMLAITestSeverity.ERROR.
        test_id: _description_. Defaults to None.
    """

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        column: str,
        time_column: str,
        events: list[str],
        severity: AMLAITestSeverity = AMLAITestSeverity.ERROR,
        test_id: Optional[str] = None,
    ) -> None:
        super().__init__(
            table_config=table_config, column=column, severity=severity, test_id=test_id
        )
        self.time_column = time_column
        self.column = column
        self.events = events
        self.group_by = ["risk_case_id", "party_id"]

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
            comparisons.append(expr[f"{first}_max"] >= expr[f"{compare}_min"])

        expr = expr.filter(itertools.accumulate(comparisons, lambda x, y: x | y))
        result = connection.execute(expr.count())
        if result > 0:
            raise FailTest(
                f"{result} did not fulfil the required event order "
                "in {self.full_column_path}",
                expr=expr,
            )


class AcceptedRangeTest(AbstractColumnTest):

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        column: str,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None,
    ) -> None:
        super().__init__(table_config=table_config, column=column)
        self.min = min_value
        self.max = max_value

    def _test(self, *, connection: BaseBackend):
        table, field = resolve_field(self.table, self.column)

        min_pred = field < self.min if self.min is not None else False
        max_pred = field > self.max if self.max is not None else False

        predicates = [min_pred | max_pred]
        expr = table.filter(predicates)

        result = connection.execute(expr.count())
        if result > 0:
            raise FailTest(
                f"{result} rows in column {self.full_column_path} in table {self.table}"
                f" were outside of inclusive range {self.min} - {self.max}",
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
                    f"Key column(s) were {" ".join(self.keys)}"
                    ""
                )
                raise FailTest(msg, expr=expr)

        result = connection.execute(expr.count())
        if result > 0:
            msg = (
                f"{result} keys found in table {self.table.get_name()} "
                f"which were not in {self.to_table.get_name()}. "
                f"Key column(s) were {" ".join(self.keys)}"
            )
            raise FailTest(msg, expr=expr)
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
            raise FailTest(msg, expr=expr)


class TemporalReferentialIntegrityTest(AbstractTableTest):
    """_summary_


    Args:
        AbstractTableTest (_type_): _description_
    """

    MAX_DATETIME_VALUE = datetime.datetime(9995, 1, 1, tzinfo=datetime.timezone.utc)

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        to_table_config: ResolvedTableConfig,
        key: str,
        # BQ doesn't support values beyond day
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

        keys = set([self.key])  # , *table_config.entity_keys])

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
            return (
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
                        & _.is_entity_deleted.negate(),
                        true_expr=TemporalReferentialIntegrityTest.MAX_DATETIME_VALUE,
                        false_expr=_.validity_start_time,
                    ).max(),
                )
            )
        else:
            # The entity's validity is counted only as of the last datetime provided
            return (
                cte3
                # At the moment, we only pay attention to the first/last dates,
                # not where there are multiple flips
                .agg(
                    first_date=_.validity_start_time.min(),
                    last_date=_.validity_start_time.max(),
                )
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
        tbl = self.get_entity_state_windows(self.table_config)
        totbl = self.get_entity_state_windows(self.to_table_config)
        # First, associate keys by joining
        # self.table.mutate(row_number=ibis.row_number().over(group_by=_.party_id, ))
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

        expr = tbl.join(
            right=totbl,
            predicates=(
                (tbl[self.key] == totbl[self.key])
                # If this item is before the first date on the base table
                & (
                    tbl.first_date.between(
                        lower=first_date_with_tolerance, upper=last_date_with_tolerance
                    ).negate()
                    |
                    # If this item is after the last date on the base table
                    tbl.last_date.between(
                        lower=first_date_with_tolerance, upper=last_date_with_tolerance
                    ).negate()
                )
            ),
        ).mutate(
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

        result = connection.execute(expr=expr.count())
        if result > 0:
            msg = (
                f"{result} keys found in table {self.table.get_name()} which were "
                f"either not in {self.to_table.get_name()}, or had inconsistent "
                "time periods, where validity_start_time and is_entity_deleted "
                f"keys in {self.table.get_name()} did not correspond to the time "
                f"periods for the same entity in {self.to_table.get_name()}"
            )
            raise FailTest(msg, expr=expr)
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
        super().__init__(table_config=table_config, column=column, severity=severity)
        self.id_to_verify = id_to_verify
        self.test_id = test_id

    @property
    def id(self):
        return self.column

    def _test(self, *, connection: BaseBackend):
        # Allow callable to be passed in for expressions which cannot be generated at
        # runtime
        table = self.get_latest_rows(self.table)

        expr = (
            table.group_by(by=self.column)
            .agg(ids=_[self.id_to_verify].collect())
            .group_by(1)
            .agg(ids=_.nunique())
        )

        result = connection.execute(expr.count())
        if result > 1:
            raise FailTest(
                f"Inconsistent {self.id_to_verify} across {self.full_column_path}. "
                "Expected all {self.column} to have the same same set of IDs",
                expr=expr,
            )
