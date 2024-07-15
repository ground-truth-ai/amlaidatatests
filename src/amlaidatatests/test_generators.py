from typing import List, Union

from ibis import Schema
from ibis.expr.datatypes import Array, DataType, Struct

from amlaidatatests.base import AbstractBaseTest, AbstractColumnTest, AMLAITestSeverity, resolve_field
from amlaidatatests.io import get_valid_currency_codes
from amlaidatatests.schema.base import ResolvedTableConfig
from amlaidatatests.schema.v1.common import CurrencyValue, ValueEntity
from amlaidatatests.tests.common import (
    AcceptedRangeTest,
    ColumnValuesTest,
    ConsecutiveEntityDeletionsTest,
    CountValidityStartTimeChangesTest,
    FieldNeverNullTest,
    FieldNeverWhitespaceOnlyTest,
    OrphanDeletionsTest,
    TableCountTest,
    TableSchemaTest,
)

ENTITIES = {"CurrencyValue": CurrencyValue(), "ValueEntity": ValueEntity()}


def get_entity_tests(
    table_config: ResolvedTableConfig, entity_name: str
) -> list[AbstractColumnTest]:
    """_summary_

    Args:
        table: _description_
        entity_name: _description_

    Raises:
        _description_

    Returns:
        _description_
    """
    if entity_name == "CurrencyValue":
        return [
            AcceptedRangeTest(
                table_config=table_config,
                column="nanos",
                min_value=0,
                max_value=1e9,
                validate=False,
            ),
            AcceptedRangeTest(
                table_config=table_config,
                column="units",
                min_value=0,
                max_value=None,
                validate=False,
            ),
            ColumnValuesTest(
                table_config=table_config,
                column="currency_code",
                values=get_valid_currency_codes(),
                validate=False,
            ),
        ]
    raise ValueError(f"Unknown Entity {entity_name}")


def get_entity_mutation_tests(
    table_config: ResolvedTableConfig, entity_ids: List[str]
) -> list[AbstractColumnTest]:
    """_summary_

    Args:
        table: _description_
        primary_keys: _description_

    Returns:
        _description_
    """
    return [
        CountValidityStartTimeChangesTest(
            table_config=table_config, warn=500, error=1000, entity_ids=entity_ids
        ),
        ConsecutiveEntityDeletionsTest(
            table_config=table_config, entity_ids=entity_ids
        ),
        OrphanDeletionsTest(table_config=table_config, entity_ids=entity_ids),
    ]


def entity_columns(
    schema: Union[Schema, Array, Struct, DataType],
    entity_types=ENTITIES.keys(),
    path: list[str] = [],
):
    """_summary_

    Args:
        schema: _description_
        entity_types: _description_. Defaults to ENTITIES.keys().
        path: _description_. Defaults to [].

    Returns:
        _description_
    """
    fields = {}
    for n, dtype in schema.items():

        if isinstance(dtype, Struct):
            subfields = entity_columns(
                schema=dtype, entity_types=entity_types, path=path.copy() + [n]
            )
            fields.update(subfields)

        if isinstance(dtype, Array):
            subfields = entity_columns(
                schema=dtype.value_type,
                entity_types=entity_types,
                path=path.copy() + [n],
            )
            fields.update(subfields)

        for name, entity_obj in ENTITIES.items():
            if name not in entity_types:
                continue
            if dtype == entity_obj:
                fields[".".join(path + [n])] = name

    return fields


def non_nullable_fields(
    schema: Union[Schema, Array, Struct, DataType], path: list[str] = []
):  # -> list | dict:
    """_summary_

    Args:
        schema: _description_
        path: _description_. Defaults to [].

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


def non_nullable_field_tests(table_config: ResolvedTableConfig) -> list[AbstractBaseTest]:
    """_summary_

    Args:
        table_config: _description_

    Returns:
        _description_
    """
    fields = non_nullable_fields(schema=table_config.table.schema())
    tests = []
    for f in fields:
        _, _field = resolve_field(table=table_config.table, column=f)
        field_type = _field.type()
        if field_type.is_string():
            tests.append(
                FieldNeverWhitespaceOnlyTest(table_config=table_config, column=f)
            )

        tests.append(FieldNeverNullTest(table_config=table_config, column=f))
    return tests


def get_generic_table_tests(
    table_config: ResolvedTableConfig,
    max_rows_factor: int,
    severity: AMLAITestSeverity = AMLAITestSeverity.ERROR,
) -> list[AbstractBaseTest]:
    """Generate tests which are completely generic and agnostic to the underlying table

    Args:
        table_config: _description_
        max_rows_factor: _description_
        severity: _description_. Defaults to AMLAITestSeverity.ERROR.

    Returns:
        _description_
    """
    return [
        TableSchemaTest(table_config),
        TableCountTest(
            table_config, severity=severity, max_rows_factor=max_rows_factor
        ),
    ]
