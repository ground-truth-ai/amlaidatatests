

from typing import Any, List, Optional, cast
from amlaidatatests.config import ConfigSingleton
from amlaidatatests.tests.base import AbstractColumnTest, AbstractTableTest, FailTest, TestSeverity, WarnTest, resolve_field, resolve_field_to_level
from ibis import BaseBackend, Table
import ibis
from ibis.expr.datatypes import Struct, Array, Timestamp, DataType
from ibis import Expr, _
import warnings
from ibis.common.exceptions import IbisTypeError
import warnings


class TestTableSchema(AbstractTableTest):

    def __init__(self, table: Table) -> None:
        super().__init__(table)

    def _test(self, *, connection: BaseBackend):
        actual_columns = set(connection.table(name=self.table.get_name()).columns)
        schema_columns = set(self.table.columns)
        excess_columns = actual_columns.difference(schema_columns)
        if len(excess_columns) > 0:
            raise WarnTest(f"{len(excess_columns)} unexpected columns found in table {self.table.get_name()}")
        # Schema table


class TestTableCount(AbstractTableTest):

    def __init__(self, table: Table, max_rows_factor: int, severity: TestSeverity = TestSeverity.ERROR) -> None:
        self.max_rows_factor = max_rows_factor
        self.scale = ConfigSingleton.get().scale
        super().__init__(table, severity)

    def _test(self, *, connection: BaseBackend):
        count = connection.execute(self.table.count())
        
        max_rows = self.max_rows_factor * self.scale

        if count == 0:
            raise FailTest(f"Table {self.table.get_name()} is empty")
        if count > max_rows:
            raise FailTest(f"Table {self.table.get_name()} has more rows than seems feasible: {count} vs maximum {max_rows}. "
                           "To stop this error triggering, review the data provided or increase the scale setting")
        if count > (max_rows) * 0.9:
            raise WarnTest(f"Table {self.table.get_name()} is close to the feasibility ceiling: {count} vs maximum {max_rows}. "
                           "To stop this error triggering, review the data provided or increase the scale setting")

class TestUniqueCombinationOfColumns(AbstractTableTest):

    def __init__(self, *, table: Table, unique_combination_of_columns: List[str]) -> None:
        super().__init__(table)
        # Check columns provided are in table
        for col in unique_combination_of_columns:
            resolve_field(table, col)
        self.unique_combination_of_columns = unique_combination_of_columns

    def _test(self, *, connection: BaseBackend) -> None:
        # TODO: Make into a single call rather than two
        expr = self.table[self.unique_combination_of_columns].agg(unique_rows=_.nunique(),
                                                                              count=_.count())
        result = connection.execute(expr).loc[0]
        n_pairs = result["unique_rows"]
        n_total = result["count"]
        if n_pairs != n_total:
            raise FailTest(f"Found {n_total - n_pairs} duplicate values")

class TestCountValidityStartTimeChanges(AbstractTableTest):

    def __init__(self, *, table: Table, primary_keys: List[str], warn=500, error=1000) -> None:
        super().__init__(table=table)
        self.warn  = warn
        self.error = error
        self.primary_keys = primary_keys
        if warn > error:
            raise Exception("")

    def _test(self, *, connection: BaseBackend) -> None:
        # References to validity_start_time are unnecessary, but they do ensure that the column is present on the table
        counted = self.table.group_by(self.primary_keys).agg(count_per_pk=self.table.count(), 
                                                             min_validity_start_time=_.validity_start_time.min(),
                                                             max_validity_start_time=_.validity_start_time.max())
        warn_number = counted.count(_.count_per_pk > self.warn)
        error_number = counted.count(_.count_per_pk > self.error)

        result = connection.execute(self.table.select(warn_number=warn_number, error_number=error_number))
        if len(result.index) == 0:
            raise Exception("No rows in table")
        
        no_errors = result["error_number"][0]
        no_warnings = result["warn_number"][0]
        
        if result["error_number"][0] > 0:
            raise FailTest(f"{no_errors} entities found with more than {self.error} validity_start_time changes in table {self.table}")
        if result["warn_number"][0] > 0:
            raise WarnTest(f"{no_warnings} entities found with more than {self.warn} validity_start_time changes in table {self.table}")
        
class TestConsecutiveEntityDeletions(AbstractTableTest):

    def __init__(self, *, table: Table, primary_keys: List[str]) -> None:
        super().__init__(table=table, severity=TestSeverity.WARN)
        self.primary_keys = primary_keys

    def _test(self, *, connection: BaseBackend) -> None:
        counted = self.table.filter(_.is_entity_deleted).group_by(self.primary_keys).agg(count_per_pk=_.count())
        expr = counted.count(_.count_per_pk > 0)
        results = connection.execute(expr)
        if results > 0:
            raise FailTest(f"{results} rows found with consecutive entity deletions. Entities should generally only be deleted once.",
                           expr=expr)


class TestColumnPresence(AbstractColumnTest):

    def _test(self, *, connection: BaseBackend):
        try:
            self.get_bound_table(connection)[self.column]
        except IbisTypeError as e:
            raise FailTest(f"Missing Required Column: {self.column}") from e

class FieldComparisonInterrupt(Exception):
    pass

class TestColumnType(AbstractColumnTest):
    
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
        if self._strip_type_for_comparison(actual_type) != self._strip_type_for_comparison(schema_data_type):
            # First, check if the difference might just be due to excess fields in structs
            try:
                extra_fields = self._find_extra_struct_fields(schema_data_type, actual_type, self.column)
                # If no expcetion
                warnings.warn(message=WarnTest(f"Additional fields found in structs in {self.column}. Full path to the extra fields were: {extra_fields}"))
                return
            except FieldComparisonInterrupt as e:
                pass
            if schema_data_type.nullable and not actual_type.nullable:
                warnings.warn(message=WarnTest(f"Schema is stricter than required: expected column {self.column} to be {schema_data_type}, found {actual_type}"))
                return
            raise FailTest(f"Expected column {self.column} to be {schema_data_type}, found {actual_type}")
    
    @classmethod
    def _find_extra_struct_fields(cls, expected_type: DataType, actual_type: DataType, path = '', level = 0):
        """ Attempt to determine if the schema mismatch is because of an extra field in a struct, including embedded structs 
        and arrays. If the fields are too dissimilar, raises FieldComparisonInterrupt(). """
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
                if expected_dtype is None: # actual struct field does not exist on the expected field
                    extra_fields.append(f'{path}.{name}')
                else:
                    fields = cls._find_extra_struct_fields(expected_type=expected_dtype, actual_type=actual_dtype, path=f'{path}.{name}', level=level)
                    extra_fields += fields
            # Also need to check all the fields in the expected type are present,
            # and the type
            for name, expected_dtype in expected_type.items():
                actual_dtype = actual_type.get(name)
                if actual_dtype is None: # actual struct field does not exist on the expected field
                    raise FieldComparisonInterrupt()
                if expected_dtype.name != actual_dtype.name:
                    raise FieldComparisonInterrupt()

        if expected_type.is_array():
            expected_type = cast(Array, expected_type)
            extra_fields.append(cls._find_extra_struct_fields(expected_type=expected_type.value_type, actual_type=actual_type.value_type, path=f'{path}.', level=level))
        # Otherwise, not a container so we don't need to check recursively
        return extra_fields

    @classmethod
    def _strip_type_for_comparison(cls, a: DataType, level = 0) -> Struct:
        """ Recursively strip nulls for type comparison """
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
            if a.timezone and a.timezone != 'UTC':
                warnings.warn(f"""Timezone of column {a.name} is not UTC. This
                              could cause problems with bigquery, since the timezone
                              is always UTC""")
            # Scale varies by database
            return a.copy(nullable=nullable, scale=None, timezone=None)
        return a.copy(nullable=nullable)
        

class TestColumnValues(AbstractColumnTest):

    def __init__(self, *, values: List[Any], table: Table, column: str, validate: bool = True) -> None:
        super().__init__(table=table, column=column, validate=validate)
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
        
        tbl = table.filter(field.notin(self.values)).select(field=field)

        result = connection.execute(ibis.join(
            # Limit the number of valid values collected to 100
            left=tbl.limit(100).agg(invalid_fields=_.field.collect()),
            right=tbl.agg(count=_.count()))
        )

        cnt = result["count"].loc[0]
                                    
        if cnt > 0:
            row = result.iloc[0]
            valid_values_message = f"Value(s) found were {result["invalid_fields"].loc[0]}"
            raise FailTest(f"{row["count"]} rows found with invalid values. Valid values are: {' '.join(self.values)}.")

class TestFieldNeverWhitespaceOnly(AbstractColumnTest):

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

        predicates = [field.strip() == '', *self.filter_null_parent_fields()]
        
        count_blank = connection.execute(table.filter(predicates).count())

        if count_blank == 0:
            return
        raise FailTest(f"{count_blank} rows found with whitespace-only values of {self.column} in table {self.table.get_name()}")
    

class TestFieldNeverNull(TestFieldNeverWhitespaceOnly):

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
        
        count_null = connection.execute(table.filter(predicates).count())

        if count_null == 0:
            return
        raise FailTest(f"{count_null} rows found with null values of {self.column} in table {self.table.get_name()}")

class TestDatetimeFieldNeverJan1970(TestFieldNeverWhitespaceOnly):

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
        
        count_null = connection.execute(table.filter(predicates).count())

        if count_null == 0:
            return
        raise FailTest(f"{count_null} rows found with date on 1970-01-01 {self.column} in table {self.table.get_name()}. This value is often nullable")



class TestNullIf(AbstractColumnTest):

    def __init__(self, *, table: Table, column: str, expression: Expr) -> None:
        super().__init__(table=table, column=column)
        self.expression = expression

    @property
    def id(self):
        return self.expression.get_name()

    def _test(self, *, connection: BaseBackend):
        result = connection.execute(self.table.filter(self.expression).count(self.table[self.column].notnull()))
        if result > 0:
            raise FailTest(f"{result} rows not fulfilling criteria {ibis.to_sql(self.expression)}")



class TestAcceptedRange(AbstractColumnTest):

    def __init__(self, *, table: Table, column: str, min: Optional[int] = None, max: Optional[int] = None,
                 validate: bool = True) -> None:
        super().__init__(table=table, column=column, validate=validate)
        self.min = min
        self.max = max


    def _test(self, *, connection: BaseBackend):
        table, field = resolve_field(self.table, self.column)

        min_pred = field < self.min if self.min is not None else False
        max_pred = field > self.max if self.max is not None else False

        predicates = [min_pred | max_pred]

        result = connection.execute(table.filter(predicates).count())
        if result > 0:
            raise FailTest(f"{result} rows in column {self.column} in table {self.table} were outside of inclusive range {self.min} - {self.max}")

class TestReferentialIntegrity(AbstractTableTest):

    def __init__(self, *, table: Table, to_table: Table, keys: list[str], severity = TestSeverity.ERROR) -> None:
        super().__init__(table=table, severity=severity)
        self.to_table = to_table
        if isinstance(keys, list):
            self.keys = keys
        else:
            # TODO: Validate provided keys
            raise Exception("Expecting a list of keys present in both tables")

    def _test(self, *, connection: BaseBackend):        
        result = connection.execute(self.table.select(*[self.keys]).anti_join(self.to_table, self.keys).count())
        if result > 0:
            msg = f"""{result} keys found in table {self.table.get_name()} which were not in {self.to_table.get_name()}. 
                           Key column(s) were {" ".join(self.keys)}"""
            raise FailTest(msg)
        return