from typing import Union
from amlaidatatests.io import get_valid_currency_codes
from amlaidatatests.schema.v1.common import CurrencyValue, ValueEntity
from amlaidatatests.tests.base import AbstractColumnTest, TestSeverity, resolve_field
from amlaidatatests.tests.common import TestAcceptedRange, TestColumnValues, TestConsecutiveEntityDeletions, TestCountValidityStartTimeChanges, TestFieldNeverWhitespaceOnly, TestFieldNeverNull, TestTableCount, TestTableSchema
from ibis import Schema, Table
from ibis.expr.datatypes import Array, DataType, Struct


ENTITIES = {'CurrencyValue': CurrencyValue(),
            'ValueEntity': ValueEntity()}


def get_entity_tests(table: Table, entity_name: str) -> list[AbstractColumnTest]:
    """_summary_

    Args:
        table (Table): _description_
        entity_name (str): _description_

    Raises:
        Exception: _description_

    Returns:
        list[AbstractColumnTest]: _description_
    """
    if entity_name == "CurrencyValue":
        return [TestAcceptedRange(table=table, column="nanos" , min=0, max=1e9, validate=False),
                TestAcceptedRange(table=table, column="units", min=0, max=None, validate=False),
                TestColumnValues(table=table, column="currency_code" , values=get_valid_currency_codes(), validate=False)]
    raise Exception(f"Unknown Entity {entity_name}")


def get_entity_mutation_tests(table: Table, primary_keys: list[str]) -> list[AbstractColumnTest]:
    """_summary_

    Args:
        table (Table): _description_
        primary_keys (list[str]): _description_

    Returns:
        list[AbstractColumnTest]: _description_
    """
    return [TestCountValidityStartTimeChanges(table=table, warn=500, error=1000, primary_keys=primary_keys),
            TestConsecutiveEntityDeletions(table=table, primary_keys=primary_keys)]


def entity_columns(schema: Union[Schema, Array, Struct, DataType],
                  entity_types = ENTITIES.keys(),
                  path: list[str] = []):
    """_summary_

    Args:
        schema (Union[Schema, Array, Struct, DataType]): _description_
        entity_types (_type_, optional): _description_. Defaults to ENTITIES.keys().
        path (list[str], optional): _description_. Defaults to [].

    Returns:
        _type_: _description_
    """
    fields = {}
    for n, dtype in schema.items():

        if isinstance(dtype, Struct):
            subfields = entity_columns(schema=dtype,
                                      entity_types=entity_types,
                                      path=path.copy() + [n])
            fields.update(subfields)

        if isinstance(dtype, Array):
            subfields = entity_columns(schema=dtype.value_type,
                                      entity_types=entity_types,
                                       path=path.copy() + [n])
            fields.update(subfields)

        for name, entity_obj in ENTITIES.items():
            if name not in entity_types:
                continue
            if dtype == entity_obj:
                fields[".".join(path + [n])] = name


    return fields



def non_nullable_fields(schema: Union[Schema, Array, Struct, DataType], path: list[str] = []):# -> list | dict:
    """_summary_

    Args:
        schema (Union[Schema, Array, Struct, DataType]): _description_
        path (list[str], optional): _description_. Defaults to [].

    Returns:
        _type_: _description_
    """
    if not isinstance(schema, (Schema, Array, Struct)):
        return True

    fields = []
    for n, dtype in schema.items():
        if isinstance(dtype, Struct):
            subfields = non_nullable_fields(dtype, path=path.copy() + [n])
            fields += subfields

        if isinstance(dtype, Array):
            subfields = non_nullable_fields(dtype.value_type, path=path.copy() + [n])
            fields += subfields
        # Also include the object itself, even if it's a struct or array.
        # This is because the Array or Struct could also be nullable.
        if not dtype.nullable:
            fields += [".".join(path + [n])]

    return fields

def non_nullable_field_tests(table: Table):
    """ Depending on field type, generate a list of tests 
    which depend on """
    fields = non_nullable_fields(schema=table.schema())
    print(fields)
    tests = []
    for f in fields:
        _, _field = resolve_field(table=table, column=f)
        field_type = _field.type()
        if field_type.is_string():
            tests.append(TestFieldNeverWhitespaceOnly(table=table, column=f))

        tests.append(TestFieldNeverNull(table=table, column=f))
    return tests

def get_generic_table_tests(table: Table,  max_rows_factor: int, severity: TestSeverity = TestSeverity.WARN):
    """ Depending on field type, generate a list of tests 
    which depend on """
    return [TestTableSchema(table), TestTableCount(table, severity=severity, max_rows_factor=max_rows_factor)]