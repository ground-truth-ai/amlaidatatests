

from typing import Any, List, Optional, cast
from amlaidatatests import connection
from amlaidatatests.tests.base import AbstractColumnTest, AbstractTableTest, FailTest, resolve_field, resolve_field_to_level
from ibis import Table
from ibis.expr.datatypes import Struct, Array, Timestamp, DataType
from ibis import Expr, Schema, _
import warnings
from ibis.common.exceptions import IbisTypeError

    
class TestUniqueCombinationOfColumns(AbstractTableTest):

    def __init__(self, *, schema: Schema, unique_combination_of_columns: List[str]) -> None:
        super().__init__(schema)
        self.unique_combination_of_columns = unique_combination_of_columns

    def test(self, *, table: Table) -> None:
        n_pairs = table[self.unique_combination_of_columns].nunique().execute()
        n_total = table.count().execute()
        if n_pairs != n_total:
            raise FailTest(f"Found {n_total - n_pairs} duplicate values")

class TestCountValidityStartTimeChanges(AbstractTableTest):

    def __init__(self, *, schema: Schema, primary_keys: List[str], warn=500, error=1000) -> None:
        super().__init__(schema)
        self.warn  = warn
        self.error = error
        self.primary_keys = primary_keys
        if warn > error:
            raise Exception("")

    def test(self, *, table: Table) -> None:
        counted = table.group_by(self.primary_keys).agg(count_per_pk=table.count())
        warn_number = counted.count(_.count_per_pk > self.warn)
        error_number = counted.count(_.count_per_pk > self.error)
        result = table.select(warn_number=warn_number, error_number=error_number).execute()
        if len(result.index) == 0:
            raise Exception("No rows in table")
        
        no_errors = result["error_number"][0]
        no_warnings = result["warn_number"][0]
            
        
        if result["error_number"][0] > 0:
            raise Exception(f"{no_errors} entities found with more than {self.error} validation changes in table {table}")
        if result["warn_number"][0] > 0:
            warnings.warn(f"{no_warnings} entities found with more than {self.warn} validation changes in table {table}")
        


class TestColumnPresence(AbstractColumnTest):

    def test(self, *, column: str, table: Table):
        try:
            table.schema()[column]
        except KeyError as e:
            raise FailTest(f"Missing Required Column: {column}") from e

class TestColumnType(AbstractColumnTest):
    
    def test(self, *, column: str, table: Table):
        """_summary_

        Args:
            table (Table): _description_
            schema (Schema): _description_

        Raises:
            FailTest: _description_
        """
        actual_type = table.schema()[column]
        schema_data_type = self.schema[column]

        self._compare(actual_type, schema_data_type)

        if self._strip_type_for_comparison(actual_type) != self._strip_type_for_comparison(schema_data_type):
            friendly_schema_type = f"nullable {schema_data_type}" if schema_data_type.nullable else f"not-nullable {schema_data_type}"
            friendly_actual_type = f"nullable {actual_type}" if actual_type.nullable else f"not-nullable {actual_type}"

            raise FailTest(f"Expected column {column} to be {friendly_schema_type}, found {friendly_actual_type}")
    
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

    def __init__(self, *, values: List[Any], schema: Schema) -> None:
        super().__init__(schema)
        self.values = values
    
    def test(self, *, table: Table, column: str):
        """_summary_

        Args:
            table (Table): _description_
            schema (Schema): _description_

        Raises:
            FailTest: _description_
        """
        table, field = resolve_field(table, column)
        
        assert table.filter(field.notin(self.values)).count().execute() == 0

class TestFieldNeverNull(AbstractColumnTest):

    def test(self, *, table: Table, column: str):
        """_summary_

        Args:
            table (Table): _description_
            schema (Schema): _description_

        Raises:
            FailTest: _description_
        """
        table, field = resolve_field(table, column)

        predicates = [field.isnull()]
        
        # If subfields exist (struct or array), we need to compare the nullness of 
        # the parent
        # as well - it doesn't make any sense to check the parent if the child
        # is also null
        if column.count(".") >= 1:
            _, parent_field = resolve_field_to_level(table, column, -1)
            # We want to check if the field is null but its parent isn't
            predicates += [parent_field.notnull()]
        
        assert table.filter(predicates).count().execute() == 0
        
class TestNullIf(AbstractColumnTest):

    def __init__(self, *, schema: Schema, expression: Expr) -> None:
        super().__init__(schema)
        self.expression = expression

    def test(self, *, table: Table, column: str):
        assert table.filter(self.expression).count(table[column].notnull()).execute() == 0


class TestAcceptedRange(AbstractColumnTest):

    def __init__(self, *, schema: Schema, min: Optional[int] = None, max: Optional[int] = None) -> None:
        super().__init__(schema)
        self.min = min
        self.max = max


    def test(self, *, table: Table, column: str):
        table, field = resolve_field(table, column)

        min_pred = [field < self.min] if min is not None else []
        max_pred = [field > self.max] if max is not None else []

        predicates = [*min_pred, *max_pred]

        assert table.filter(predicates).count().execute() == 0