

from typing import List, Optional
from amlaidatatests.tests.base import FailTest
from ibis import Table
from ibis.expr.datatypes import DataType, Struct
import pytest
from ibis import Expr


def resolve_field(table: Table, column: str) -> Expr:
    # Given a path x.y.z, resolve the field object
    # on the table
    field = table
    for i, p in enumerate(column.split(".")):
        # The first field is a table. If the table
        # has a field called table, this loo
        if i > 0 and field.type().is_array():
            field.map(lambda f: getattr(f, p))
        else:
            field = getattr(field, p)
    return field

@pytest.fixture
def unique_combination_of_columns():
    def _unique_combination_of_columns(*, table: Table, unique_combination_of_columns: List[str]):
        n_pairs = table[unique_combination_of_columns].nunique().execute()
        n_total = table.count().execute()
        if n_pairs != n_total:
            raise FailTest(f"Found {n_total - n_pairs} duplicate values")
    
    return _unique_combination_of_columns

@pytest.fixture
def test_column_presence():
    def _test_column_presence(*, schema_column: str, table: Table):
        """_summary_

        Args:
            table (Table): _description_
            schema (Schema): _description_

        Raises:
            FailTest: _description_
        """
        table.schema()[schema_column]

    return _test_column_presence

@pytest.fixture
def test_column_type():

    def _strip_type_for_comparison(a: Struct, level = 0) -> Struct:
        level = level + 1
        nullable = a.nullable if level == 1 else True
        dct = {}
        if a.is_struct():
            for n, dtype in a.items():
                dct[n] = _strip_type_for_comparison(dtype, level)
            return Struct(dct)
        if a.is_timestamp():
            # Scale varies by database
            return a.copy(nullable=nullable, scale=None)
        return a.copy(nullable=nullable)
        

    def _test_column_type(*, schema_name: str, schema_data_type: DataType, table: Table):
        """_summary_

        Args:
            table (Table): _description_
            schema (Schema): _description_

        Raises:
            FailTest: _description_
        """
        actual_type = table.schema()[schema_name]

        if _strip_type_for_comparison(actual_type) != schema_data_type:
            friendly_schema_type = f"nullable {schema_data_type}" if schema_data_type.nullable else f"not-nullable {schema_data_type}"
            friendly_actual_type = f"nullable {actual_type}" if actual_type.nullable else f"not-nullable {actual_type}"

            raise FailTest(f"Expected column {schema_name} to be {friendly_schema_type}, found {friendly_actual_type}")
            

    return _test_column_type

@pytest.fixture
def test_column_values(values: Optional[List[any]]):
    
    def _test_column_values(*, table: Table, column: str, values: List[any] = values):
        """_summary_

        Args:
            table (Table): _description_
            schema (Schema): _description_

        Raises:
            FailTest: _description_
        """
        field = resolve_field(table, column)
        
        assert table.filter(field.notin(values)).count().execute() == 0

    return _test_column_values

@pytest.fixture
def test_non_nullable_fields():
    
    def _test_non_nullable_fields(*, table: Table, path: str):
        """_summary_

        Args:
            table (Table): _description_
            schema (Schema): _description_

        Raises:
            FailTest: _description_
        """
        field = table
        for p in path.split("."):
            field = getattr(field, p)
        assert table.filter(field.isnull()).count().execute() == 0

    return _test_non_nullable_fields

def test_null_if():
    
    def _test_null_if(*, table: Table, expression: Expr, column: str):
        """_summary_

        Args:
            table (Table): _description_
            schema (Schema): _description_

        Raises:
            FailTest: _description_
        """
        assert table.filter(expression).count().execute() == 0

    return _test_null_if

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


def accepted_range(min: Optional[int] = None, max: Optional[int] = None):

    def _accepted_range(*, table: Table, column: str):
        field = resolve_field(table, column)

        min_pred = [field < min] if min is not None else []
        max_pred = [field > max] if max is not None else []

        predicates = [*min_pred, *max_pred]

        assert table.filter(predicates).count().execute() == 0

    return _accepted_range

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