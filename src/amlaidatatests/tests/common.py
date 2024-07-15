import datetime
import warnings
from typing import Any, List, Literal, Optional, cast

import ibis
from ibis import BaseBackend, Expr, _
from ibis.common.exceptions import IbisTypeError
from ibis.expr.datatypes import Array, DataType, Struct, Timestamp

from amlaidatatests.base import (
    AbstractColumnTest,
    AbstractTableTest,
    AMLAITestSeverity,
    FailTest,
    WarnTest,
    check_table_exists,
    resolve_field,
    resolve_field_to_level,
)
from amlaidatatests.config import ConfigSingleton
from amlaidatatests.schema.base import ResolvedTableConfig


class TableSchemaTest(AbstractTableTest):
    """_summary_

    Args:
        AbstractTableTest: _description_
    """

    def __init__(self, table_config: ResolvedTableConfig) -> None:
        """_summary_

        Args:
            table_config: _description_
        """
        super().__init__(table_config)

    def _test(self, *, connection: BaseBackend):
        actual_columns = set(connection.table(name=self.table.get_name()).columns)
        schema_columns = set(self.table.columns)
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
        max_rows_factor: int,
        severity: AMLAITestSeverity = AMLAITestSeverity.ERROR,
    ) -> None:
        """_summary_

        Args:
            table_config: _description_
            max_rows_factor: _description_
            severity: _description_. Defaults to AMLAITestSeverity.ERROR.
        """
        self.max_rows_factor = max_rows_factor
        self.scale = ConfigSingleton.get().scale
        super().__init__(table_config, severity)

    def _test(self, *, connection: BaseBackend):
        count = connection.execute(self.table.count())

        max_rows = self.max_rows_factor * self.scale

        if count == 0:
            raise FailTest(f"Table {self.table.get_name()} is empty")
        if count > max_rows:
            raise FailTest(
                f"Table {self.table.get_name()} has more rows than seems feasible:"
                f" {count} vs maximum {max_rows}. To stop this error triggering, review"
                " the data provided or increase the scale setting"
            )
        if count > (max_rows) * 0.9:
            raise WarnTest(
                f"Table {self.table.get_name()} is close to the feasibility ceiling:"
                f" {count} vs maximum {max_rows}. To stop this error triggering, review"
                " the data provided or increase the scale setting"
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
    ) -> None:
        """_summary_

        Args:
            table_config: _description_
            unique_combination_of_columns: _description_
        """
        super().__init__(table_config=table_config)
        # Check columns provided are in table
        for col in unique_combination_of_columns:
            resolve_field(self.table, col)
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


class CountValidityStartTimeChangesTest(AbstractTableTest):
    """_summary_

    Args:
        AbstractTableTest: _description_
    """

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        entity_ids: List[str],
        warn=500,
        error=1000,
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
        super().__init__(table_config=table_config)
        self.warn = warn
        self.error = error
        self.entity_ids = entity_ids
        if warn > error:
            raise ValueError(f"Warn value: {warn} cannot be greater than Error value: {error}")

    def _test(self, *, connection: BaseBackend) -> None:
        # References to validity_start_time are unnecessary, but they do ensure that the column is present on the table
        counted = self.table.group_by(self.entity_ids).agg(
            count_per_pk=self.table.count(),
            min_validity_start_time=_.validity_start_time.min(),
            max_validity_start_time=_.validity_start_time.max(),
        )
        warn_number = counted.filter(_.count_per_pk > self.warn)
        error_number = counted.filter(_.count_per_pk > self.error)

        result = connection.execute(
            self.table.select(
                warn_number=warn_number.count(), error_number=error_number.count()
            )
        )
        if len(result.index) == 0:
            raise FailTest("No rows in table")

        no_errors = result["error_number"][0]
        no_warnings = result["warn_number"][0]

        if result["error_number"][0] > 0:
            raise FailTest(
                f"{no_errors} entities found with more than"
                f" {self.error} validity_start_time changes in table {self.table}",
                expr=error_number,
            )
        if result["warn_number"][0] > 0:
            raise WarnTest(
                f"{no_warnings} entities found with more than"
                f" {self.warn} validity_start_time changes in table {self.table}",
                expr=warn_number,
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
        # is_entity_deleted is a nullable field, so we assume if it is null, we mean False
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
            self.get_bound_table(connection)[self.column]
        except IbisTypeError as e:
            raise FailTest(f"Missing Required Column: {self.column}") from e


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
        actual_table = self.get_bound_table(connection)
        actual_type = actual_table.schema()[self.column]
        schema_data_type = self.table.schema()[self.column]

        # Check the child types
        if self._strip_type_for_comparison(
            actual_type
        ) != self._strip_type_for_comparison(schema_data_type):
            # First, check if the difference might just be due to excess fields in structs
            try:
                extra_fields = self._find_extra_struct_fields(
                    schema_data_type, actual_type, self.column
                )
                # If no expcetion
                warnings.warn(
                    message=WarnTest(
                        f"Additional fields found in structs in {self.column}. Full"
                        f" path to the extra fields were: {extra_fields}"
                    )
                )
                return
            except FieldComparisonInterrupt:
                pass
            if schema_data_type.nullable and not actual_type.nullable:
                warnings.warn(
                    message=WarnTest(
                        "Schema is stricter than required: expected column"
                        f" {self.column} to be {schema_data_type}, found {actual_type}"
                    )
                )
                return
            raise FailTest(
                f"Expected column {self.column} to be {schema_data_type}, found"
                f" {actual_type}",
            )

    @classmethod
    def _find_extra_struct_fields(
        cls, expected_type: DataType, actual_type: DataType, path="", level=0
    ):
        """Attempt to determine if the schema mismatch is because of an extra field in a struct, including embedded structs
        and arrays. If the fields are too dissimilar, raises FieldComparisonInterrupt().
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
                warnings.warn(f"""Timezone of column {a.name} is not UTC. This
                              could cause problems with bigquery, since the timezone
                              is always UTC""")
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
        validate: bool = True,
    ) -> None:
        super().__init__(table_config=table_config, column=column, validate=validate)
        self.values = values

    def _test(self, *, connection: BaseBackend):
        """_summary_

        Args:
            table (Table): _description_
            schema (Schema): _description_

        Raises:
            FailTest: _description_
        """
        table, field = resolve_field(self.table, self.column)

        expr = table.filter(field.notin(self.values)).select(field=field)

        result = connection.execute(expr.count())

        if result > 0:
            raise FailTest(
                f"{result} rows found with invalid values. Valid values are:"
                f" {' '.join(self.values)}.",
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
            f"{count_blank} rows found with whitespace-only values of {self.column} in"
            f" table {self.table.get_name()}",
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
            f"{count_null} rows found with null values of {self.column} in table"
            f" {self.table.get_name()}",
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
            f"{count_null} rows found with date on 1970-01-01 {self.column} in table"
            f" {self.table.get_name()}. This value is often nullable",
            expr=expr,
        )


class NullIfTest(AbstractColumnTest):

    def __init__(
        self, *, table_config: ResolvedTableConfig, column: str, expression: Expr
    ) -> None:
        super().__init__(table_config=table_config, column=column)
        self.expression = expression

    @property
    def id(self):
        return self.expression.get_name()

    def _test(self, *, connection: BaseBackend):
        expr = self.table.filter(self.expression).filter(
            self.table[self.column].notnull()
        )
        result = connection.execute(expr.count())
        if result > 0:
            raise FailTest(
                f"{result} rows not fulfilling criteria {ibis.to_sql(self.expression)}",
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
        validate: bool = True,
    ) -> None:
        super().__init__(table_config=table_config, column=column, validate=validate)
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
                f"{result} rows in column {self.column} in table {self.table} were"
                f" outside of inclusive range {self.min} - {self.max}",
                expr=expr,
            )


class ReferentialIntegrityTest(AbstractTableTest):

    def __init__(
        self,
        *,
        table_config: ResolvedTableConfig,
        to_table_config: ResolvedTableConfig,
        keys: list[str],
        severity=AMLAITestSeverity.ERROR,
    ) -> None:
        super().__init__(table_config=table_config, severity=severity)
        self.to_table_config = to_table_config
        self.to_table = to_table_config.table
        self.keys = keys

    def _test(self, *, connection: BaseBackend):
        # The superclass does not skip the test if the to_table is optional,
        # which it may be. If it is, skip the test.
        check_table_exists(connection=connection, table_config=self.to_table_config)
        expr = self.table.select(*[self.keys]).anti_join(self.to_table, self.keys)
        result = connection.execute(expr.count())
        if result > 0:
            msg = f"""{result} keys found in table {self.table.get_name()} which were not in {self.to_table.get_name()}.
                           Key column(s) were {" ".join(self.keys)}"""
            raise FailTest(msg, expr=expr)
        return


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
        tolerance: Optional[
            Literal[
                "year",
                "quarter",
                "month",
                "week",
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
    ) -> None:

        super().__init__(table_config=table_config, severity=severity)
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
        # | party_id | window_id  | window_start_time | window_end_time   | is_entity_deleted
        # |    1     |     0      |      00:00:00     |      null         |      False
        table = table_config.table
        w = ibis.window(group_by=self.key, order_by="validity_start_time")
        # is_entity_deleted is a nullable field, so we assume if it is null, we mean False
        cte0 = ibis.coalesce(table.is_entity_deleted, False)
        # First, find the changes in entity_deleted switching in order to determine the rows which lead to the table switching.
        # do this by finding the previous values of "is_entity_deleted" and determining if.

        cte1 = table.select(
            self.key,
            "validity_start_time",
            is_entity_deleted=cte0,
            previous_entity_deleted=cte0.lag().over(w),
            next_row_validity_start_time=_.validity_start_time.lead().over(w),
            previous_row_validity_start_time=_.validity_start_time.lag().over(w),
        )

        # Handle the entity being immediately deleted by assuming it only existed for a
        # fraction of a second. This isn't valid data, but it should be picked up by the entity
        # mutation tests
        cte2 = cte1.mutate(
            previous_row_validity_start_time=ibis.ifelse(
                (_.previous_row_validity_start_time.isnull()) & (_.is_entity_deleted),
                _.validity_start_time,
                _.previous_row_validity_start_time,
            )
        )

        # Only return useful rows, not ones where the entities deletion state didn't change
        cte3 = cte2.filter(
            (_.previous_row_validity_start_time == None)  # first row
            | (_.next_row_validity_start_time == None)  # last row
            | (_.is_entity_deleted != _.previous_entity_deleted)  # state flips
        ).group_by(self.key)

        if table_config.is_open_ended_entity:
            # For open ended entities, e.g. parties, we need to assume the entity persists until it is deleted.
            # This means that the maximum validity date time
            return (
                cte3
                # At the moment, we only pay attention to the first/last dates, not where there are multiple flips
                .agg(
                    first_date=_.validity_start_time.min(),  # null handling not required as validity_start_time is a non-nullable field
                    last_date=ibis.ifelse(
                        condition=_.next_row_validity_start_time.isnull()
                        & _.is_entity_deleted.negate(),
                        true_expr=TemporalReferentialIntegrityTest.MAX_DATETIME_VALUE,
                        false_expr=_.validity_start_time,
                    ).max(),  # if the only row and the row isn't yet deleted, we need to make the validity end time far into the future
                )
            )
        else:
            # The entity's validity is counted only as of the last datetime provided
            return (
                cte3
                # At the moment, we only pay attention to the first/last dates, not where there are multiple flips
                .agg(
                    first_date=_.validity_start_time.min(),
                    last_date=_.validity_start_time.max(),
                )
            )

    def _test(self, *, connection: BaseBackend):
        # Table 1
        # | party_id | window_id  | window_start_time | window_end_time   | is_entity_deleted
        # |    1     |     0      |      00:00:00     |      01:00:00     |      False
        # |    1     |     1      |      01:00:00     |      02:00:00     |      True
        # |    1     |     2      |      02:00:00     |        null       |      False
        #
        # Table 2
        # | party_id | window_id  | window_start_time | window_end_time   | is_entity_deleted
        # |    1     |     0      |      00:00:00     |      01:00:00     |      False
        # |    1     |     1      |      01:00:00     |      02:00:00     |      True
        # |    1     |     2      |      02:00:00     |        null       |      False
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

        # Anti-join, only return rows which aren't in both.
        # This does have the side effect of not being able to differentiate between
        # missing keys and problems with the date alignment
        # join_check = tbl.anti_join(
        #     right=totbl,
        #     predicates=(
        #         (tbl[self.key] == totbl[self.key])
        #         # If this item is before the first date on the base table
        #         & tbl.first_date.between(
        #             lower=first_date_with_tolerance, upper=last_date_with_tolerance
        #         )
        #         &
        #         # If this item is after the last date on the base table
        #         tbl.last_date.between(
        #             lower=first_date_with_tolerance, upper=last_date_with_tolerance
        #         )
        #     ),
        # )

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
            msg = f"""{result} keys found in table {self.table.get_name()} which were either not in {self.to_table.get_name()},
                        or had inconsistent time periods, where validity_start_time and is_entity deleted keys in {self.table.get_name()}
                         did not correspond to the time periods for the same entity in {self.to_table.get_name()}
                           """
            raise FailTest(msg, expr=expr)
        return
