

from typing import List, Optional
from amlaidatatests.tests.base import AbstractBaseTest, AbstractColumnTest, AbstractTableTest, FailTest
from ibis import Table
from ibis.expr.datatypes import DataType, Struct, Array
import pytest
from ibis import Expr, Schema, _
from ...connection import connection
import warnings


def resolve_field(table: Table, column: str) -> Expr:
    # Given a path x.y.z, resolve the field object
    # on the table
    field = table
    for i, p in enumerate(column.split(".")):
        # The first field is a table. If the table
        # has a field called table, this loo
        if i > 0 and field.type().is_array():
            field = field.map(lambda f: getattr(f, p))
        else:
            field = getattr(field, p)
    return field

class TestUniqueCombinationOfColumns(AbstractTableTest):

    def __init__(self, *, unique_combination_of_columns: List[str]) -> None:
        super().__init__()
        self.unique_combination_of_columns = unique_combination_of_columns

    def test(self, *, table: Table) -> None:
        n_pairs = table[self.unique_combination_of_columns].nunique().execute()
        n_total = table.count().execute()
        if n_pairs != n_total:
            raise FailTest(f"Found {n_total - n_pairs} duplicate values")

class TestCountValidityStartTimeChanges(AbstractTableTest):

    def __init__(self, *, primary_keys: List[str], warn=500, error=1000) -> None:
        super().__init__()
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
        table.schema()[column]

class TestColumnType(AbstractColumnTest):

    def __init__(self, schema: Schema) -> None:
        super().__init__()
        self.schema = schema
    
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

        if self._strip_type_for_comparison(actual_type) != self._strip_type_for_comparison(schema_data_type):
            friendly_schema_type = f"nullable {schema_data_type}" if schema_data_type.nullable else f"not-nullable {schema_data_type}"
            friendly_actual_type = f"nullable {actual_type}" if actual_type.nullable else f"not-nullable {actual_type}"

            raise FailTest(f"Expected column {column} to be {friendly_schema_type}, found {friendly_actual_type}")
    
    def _strip_type_for_comparison(self, a: Struct, level = 0) -> Struct:
        level = level + 1
        nullable = a.nullable if level == 1 else True
        dct = {}
        if a.is_struct():
            for n, dtype in a.items():
                dct[n] = self._strip_type_for_comparison(dtype, level)
            return Struct(nullable=nullable, fields=dct)
        if a.is_array():
            value = self._strip_type_for_comparison(a.value_type, level)
            return Array(nullable=nullable, value_type=value)
        if a.is_timestamp():
            # Scale varies by database
            return a.copy(nullable=nullable, scale=None)
        return a.copy(nullable=nullable)
        

class TestColumnValues(AbstractColumnTest):

    def __init__(self, values: List[any]) -> None:
        super().__init__()
        self.values = values
    
    def test(self, *, table: Table, column: str):
        """_summary_

        Args:
            table (Table): _description_
            schema (Schema): _description_

        Raises:
            FailTest: _description_
        """
        field = resolve_field(table, column)
        
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
        field = resolve_field(table, column)
        
        field = table
        for p in column.split("."):
            field = getattr(field, p)
        assert table.filter(field.isnull()).count().execute() == 0
        
class TestNullIf(AbstractColumnTest):

    def __init__(self, expression: Expr) -> None:
        super().__init__()
        self.expression = expression

    def test(self, *, table: Table, column: str):
        assert table.filter(self.expression).count().execute() == 0


@pytest.fixture
def test_schema_entities():
    
    def _test_schema_entities(*, table: Table, column: str, entity: Struct):
        """_summary_

        Args:
            table (Table): _description_
            schema (Schema): _description_

        Raises:
            FailTest: _description_
        """
        assert table.filter(expression).count().execute() == 0

    return _test_schema_entities


class TestAcceptedRange(AbstractColumnTest):

    def __init__(self, min: Optional[int] = None, max: Optional[int] = None) -> None:
        super().__init__()
        self.min = min
        self.max = max


    def test(self, *, table: Table, column: str):
        field = resolve_field(table, column)

        min_pred = [field < self.min] if min is not None else []
        max_pred = [field > self.max] if max is not None else []

        predicates = [*min_pred, *max_pred]

        assert table.filter(predicates).count().execute() == 0


#     return _test_column_types
# @pytest.fixture
# def test_column_types():
#     test_issues = []
#     def _test_column_types(*, table: Table, schema: Schema):
#         """_summary_

#         Args:
#             table (Table): _description_
#             schema (Schema): _description_

#         Raises:
#             FailTest: _description_
#         """
#         for field_name, field_type in table.schema().items():
#             try:
#                 validation_field = schema[field_name]
#                 assert field_type == validation_field
#             except AssertionError:
#                 test_issues.append(f"Column {field_name} is the wrong type. Was {field_type}, expected {validation_field}")

#         if len(test_issues) > 0:
#             raise FailTest("\n".join(test_issues))

#     return _test_column_types