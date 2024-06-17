

from typing import Any, List, Optional, Union, cast
from amlaidatatests import connection
from amlaidatatests.tests.base import AbstractColumnTest, AbstractMultiTableTest, AbstractTableTest, FailTest, resolve_field, resolve_field_to_level
from ibis import BaseBackend, Table
from ibis.expr.datatypes import Struct, Array, Timestamp, DataType
from ibis import Expr, Schema, _
import warnings
from ibis.common.exceptions import IbisTypeError
from ibis import literal
from ibis.expr.operations.arrays import Array as NewArray

    
class TestUniqueCombinationOfColumns(AbstractTableTest):

    def __init__(self, *, table: Table, unique_combination_of_columns: List[str]) -> None:
        super().__init__(table)
        self.unique_combination_of_columns = unique_combination_of_columns

    def test(self, *, connection: BaseBackend) -> None:
        n_pairs = connection.execute(self.table[self.unique_combination_of_columns].nunique())
        n_total = connection.execute(self.table.count())
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

    def test(self, *, connection: BaseBackend) -> None:
        counted = self.table.group_by(self.primary_keys).agg(count_per_pk=self.table.count())
        warn_number = counted.count(_.count_per_pk > self.warn)
        error_number = counted.count(_.count_per_pk > self.error)
        result = connection.execute(self.table.select(warn_number=warn_number, error_number=error_number))
        if len(result.index) == 0:
            raise Exception("No rows in table")
        
        no_errors = result["error_number"][0]
        no_warnings = result["warn_number"][0]
            
        
        if result["error_number"][0] > 0:
            raise Exception(f"{no_errors} entities found with more than {self.error} validation changes in table {self.table}")
        if result["warn_number"][0] > 0:
            warnings.warn(f"{no_warnings} entities found with more than {self.warn} validation changes in table {self.table}")
        


class TestColumnPresence(AbstractColumnTest):

    def test(self, *, connection: BaseBackend):
        try:
            self.get_bound_table(connection)[self.column]
        except IbisTypeError as e:
            raise FailTest(f"Missing Required Column: {self.column}") from e

class TestColumnType(AbstractColumnTest):
    
    def test(self, *, connection: BaseBackend):
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

        self._compare(actual_type, schema_data_type)

        if self._strip_type_for_comparison(actual_type) != self._strip_type_for_comparison(schema_data_type):
            friendly_schema_type = f"nullable {schema_data_type}" if schema_data_type.nullable else f"not-nullable {schema_data_type}"
            friendly_actual_type = f"nullable {actual_type}" if actual_type.nullable else f"not-nullable {actual_type}"

            raise FailTest(f"Expected column {self.column} to be {friendly_schema_type}, found {friendly_actual_type}")
    
    def _compare(self, a: Struct, b: Struct):
        pass
        

    def _strip_type_for_comparison(self, a: DataType, level = 0) -> Struct:
        """ Recursively strip nulls for type comparison """
        level = level + 1
        # We can't check lower level nullable columns because
        # it's not possible to specify columns
        nullable = a.nullable if level == 1 else True
        dct = {}
        if a.is_struct():
            a = cast(Struct, a)
            for n, dtype in a.items():
                dct[n] = self._strip_type_for_comparison(dtype, level)
            # Now normalize the keys so the order is consistent for comparison
            dct = dict(sorted(dct.items()))
            return Struct(nullable=nullable, fields=dct)
        if a.is_array():
            value = self._strip_type_for_comparison(a.value_type, level)
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
    
    def test(self, *, connection: BaseBackend):
        """_summary_

        Args:
            table (Table): _description_
            schema (Schema): _description_

        Raises:
            FailTest: _description_
        """
        table, field = resolve_field(self.table, self.column)
        
        assert connection.execute(table.filter(field.notin(self.values)).count()) == 0

class TestFieldNeverNull(AbstractColumnTest):

    def test(self, *, connection: BaseBackend):
        """_summary_

        Args:
            table (Table): _description_
            schema (Schema): _description_

        Raises:
            FailTest: _description_
        """
        table, field = resolve_field(self.table, self.column)

        predicates = [field.isnull()]
        
        # If subfields exist (struct or array), we need to compare the nullness of 
        # the parent
        # as well - it doesn't make any sense to check the parent if the child
        # is also null
        if self.column.count(".") >= 1:
            _, parent_field = resolve_field_to_level(self.table, self.column, -1)
            # We want to check if the field is null but its parent isn't
            predicates += [parent_field.notnull()]
        
        assert connection.execute(table.filter(predicates).count()) == 0
        
class TestNullIf(AbstractColumnTest):

    def __init__(self, *, table: Table, column: str, expression: Expr) -> None:
        super().__init__(table, column)
        self.expression = expression

    def test(self, *, connection: BaseBackend):
        assert connection.execute(self.table.filter(self.expression).count(self.table[self.column].notnull())) == 0


class TestAcceptedRange(AbstractColumnTest):

    def __init__(self, *, table: Table, column: str, min: Optional[int] = None, max: Optional[int] = None,
                 validate: bool = True) -> None:
        super().__init__(table, column=column, validate=validate)
        self.min = min
        self.max = max


    def test(self, *, connection: BaseBackend):
        table, field = resolve_field(self.table, self.column)

        min_pred = [field < self.min] if min is not None else []
        max_pred = [field > self.max] if max is not None else []

        predicates = [*min_pred, *max_pred]

        assert connection.execute(table.filter(predicates).count()) == 0

class TestReferentialIntegrity(AbstractTableTest):

    def __init__(self, *, table: Table, to_table: Table, keys: list[str]) -> None:
        super().__init__(table=table)
        self.to_table = to_table
        if isinstance(keys, list):
            self.keys = keys
        # elif isinstance(keys, dict):
        #     # Validate the length of keys are always the same
        #     self.keys = keys
        #     length = None
        #     for k, v in self.keys.items():
        #         if not isinstance(v, list) or not isinstance(k, str):
        #             raise Exception("Expecting a dictionary of string table ids to keys in those tables")
        #         if length is None:
        #             length = len(v)
        #             continue
        #         if length != len(v):
        #             raise Exception("Mismatched key lengths in key dictionary passed")
        else:
            raise Exception("Expecting a dictionary of string table ids to keys in those tables")

    def test(self, *, connection: BaseBackend):        
        result = connection.execute(self.table.select(*[self.keys]).anti_join(self.to_table, self.keys).count())
        if result > 0:
            raise FailTest(f"""{result} keys found in table {self.table.get_name()} which were not in {self.to_table.get_name()}. 
                           Key column(s) was {" ".join(self.keys)}""")
        return True