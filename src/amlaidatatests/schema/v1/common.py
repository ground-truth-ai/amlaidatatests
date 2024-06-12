from typing import Callable, Union
from amlaidatatests.io import get_valid_currency_codes
from ibis.expr.datatypes import Int64, String, Struct, DataType, Array
from ibis import Schema
from amlaidatatests.tests.fixtures.common_tests import TestAcceptedRange, TestColumnValues, TestCountValidityStartTimeChanges
from dataclasses import dataclass


def CurrencyValue():
    return Struct(
            nullable=False,
            fields={
                'units': Int64(nullable=False),
                'nanos': Int64(nullable=False)
            }
        )

def ValueEntity():
    return Struct(
            nullable=False,
            fields={
                'units': Int64(nullable=False),
                'nanos': Int64(nullable=False),
                'currency_code': String()
            }
        )

@dataclass
class EntityTest():
    test_name: str
    test_field: str
    test: Callable

ENTITIES = {'CurrencyValue': CurrencyValue(),
            'ValueEntity': ValueEntity()}

def get_entity_tests(entity_name: str) -> list[EntityTest]:
    if entity_name == "CurrencyValue":
        return [EntityTest(test_name="nanos_range", 
                           test_field="nanos",
                           test=TestAcceptedRange(min=0, max=1e9)),
                EntityTest(test_name="units_range", 
                           test_field="units",
                           test=TestAcceptedRange(min=0, max=None)),
                EntityTest(test_name="currency_code", 
                           test_field="currency_code",
                           test=TestColumnValues(values=get_valid_currency_codes()))
                ]
    raise Exception(f"Unknown Entity {entity_name}")

def get_entity_mutation_tests(primary_keys: list[str]) -> list[EntityTest]:
    return [EntityTest(test_name="validity_start_time_changes",
                       test_field="validity_start_time",
                       test=TestCountValidityStartTimeChanges(warn=500, error=1000, primary_keys=primary_keys))
            ]


def entity_columns(schema: Union[Schema, Array, Struct, DataType], 
                  entity_types = ENTITIES.keys(), 
                  path: list[str] = []):   
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



