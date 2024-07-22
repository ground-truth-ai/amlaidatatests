import functools
from typing import Optional, Union

from ibis import Schema, literal, Table
import ibis
from ibis.expr.datatypes import Array, DataType, Struct, Timestamp

from amlaidatatests.base import AbstractBaseTest, AbstractColumnTest
from amlaidatatests.config import cfg
from amlaidatatests.exceptions import AMLAITestSeverity
from amlaidatatests.io import get_valid_currency_codes
from amlaidatatests.schema.base import ResolvedTableConfig, TableType
from amlaidatatests.schema.v1.common import CurrencyValue, ValueEntity
from amlaidatatests.tests.common import (
    AcceptedRangeTest,
    ColumnCardinalityTest,
    ColumnValuesTest,
    ConsecutiveEntityDeletionsTest,
    CountFrequencyValues,
    CountMatchingRows,
    FieldNeverNullTest,
    OrphanDeletionsTest,
    TableCountTest,
    TableExcessColumnsTest,
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
            ),
            AcceptedRangeTest(
                table_config=table_config,
                column="units",
                min_value=0,
                max_value=None,
            ),
            ColumnValuesTest(
                table_config=table_config,
                column="currency_code",
                values=get_valid_currency_codes(),
            ),
        ]
    raise ValueError(f"Unknown Entity {entity_name}")


def get_entity_mutation_tests(
    table_config: ResolvedTableConfig,
) -> list[AbstractColumnTest]:
    """Retrieve all generic tests for detection of entity mutation.

    Args:
        table_config: Configuration for
        entity_ids: _description_

    Returns:
        _description_
    """
    return [
        ColumnCardinalityTest(
            column="validity_start_time",
            table_config=table_config,
            max_number=500,
            group_by=table_config.entity_keys,
            severity=AMLAITestSeverity.WARN,
            test_id="P057",
        ),
        ColumnCardinalityTest(
            column="validity_start_time",
            table_config=table_config,
            max_number=1000,
            group_by=table_config.entity_keys,
            severity=AMLAITestSeverity.WARN,
            test_id="P058",
        ),
        ConsecutiveEntityDeletionsTest(
            table_config=table_config, entity_ids=table_config.entity_keys
        ),
        OrphanDeletionsTest(
            table_config=table_config, entity_ids=table_config.entity_keys
        ),
    ]


def get_entities(
    item: Union[Schema, Array, Struct, DataType],
    entity_types=ENTITIES.keys(),
    path: Optional[list[str]] = None,
):
    """Get the entity types in the schema

    Args:
        item: The [ibis.Schema] or [ibis.common.collections.MapSet] which
              corresponds to this item.
        entity_types: The entities to locate. Defaults to ENTITIES.keys().
        path: The path relative to the parent object of this object. Used to
              specify any parent item above the one being called. For example,
              if . Defaults to [].

    Returns:
        A list of the entities paths
    """
    # top level path
    if path is None:
        path = []

    fields = {}
    for n, dtype in item.items():

        if isinstance(dtype, Struct):
            subfields = get_entities(
                item=dtype, entity_types=entity_types, path=path.copy() + [n]
            )
            fields.update(subfields)

        if isinstance(dtype, Array):
            subfields = get_entities(
                item=dtype.value_type,
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


def get_non_nullable_fields(
    item: Union[Schema, Array, Struct, DataType], path: Optional[list[str]] = None
) -> list[str]:  # -> list | dict:
    """Return a list of all the nullable fields in the schema, specified
    as dot delimited paths.

    Includes fields embedded within Structs or Arrays whose parents might not be
    nullable, which are return as "paths" by recursively calling this function.

    Args:
        item: The [ibis.Schema] or [ibis.common.collections.MapSet] which
              corresponds to this item.
        path: The path relative to the parent object of this object. Used to
              specify any parent item above the one being called. Defaults to [].

    Returns:
        A list of paths to the non-nullable fields in the container, e.g.
        for a struct {'a': {'b': {'c': value}}} would return a.b.c
    """
    # top level path
    if path is None:
        path = []

    # base case
    if not isinstance(item, (Schema, Array, Struct)):
        return []

    fields = []
    for n, dtype in item.items():
        if isinstance(dtype, Struct):
            subfields = get_non_nullable_fields(dtype, path=path.copy() + [n])
            fields += subfields

        if isinstance(dtype, Array):
            subfields = get_non_nullable_fields(
                dtype.value_type, path=path.copy() + [n]
            )
            fields += subfields
        # Also include the object itself, even if it's a struct or array.
        # This is because the Array or Struct could also be nullable.
        if not dtype.nullable:
            fields += [".".join(path + [n])]

    return fields


def get_timestamp_fields(
    item: Union[Schema, Array, Struct, DataType], path: Optional[list[str]] = None
):  # -> list | dict:
    """Return a list of all the timestamp fields in the schema, specified
    as dot delimited paths.

    Includes fields embedded within Structs or Arrays.

    Args:
        item: The [ibis.Schema] or [ibis.common.collections.MapSet] which
              corresponds to this item.
        path: The path relative to the parent object of this object. Used to
              specify any parent item above the one being called. Defaults to [].

    Returns:
        A list of paths to the timestamp fields in the container, e.g.
        for a struct {'a': {'b': {'c': timestamp_value}}} would return a.b.c
    """
    # top level path
    if path is None:
        path = []

    # base case
    if not isinstance(item, (Schema, Array, Struct)):
        return True

    fields = []
    for n, dtype in item.items():
        if isinstance(dtype, Struct):
            subfields = get_timestamp_fields(dtype, path=path.copy() + [n])
            fields += subfields

        if isinstance(dtype, Array):
            subfields = get_timestamp_fields(dtype.value_type, path=path.copy() + [n])
            fields += subfields
        # Also include the object itself, even if it's a struct or array.
        # This is because the Array or Struct could also be nullable.
        if isinstance(dtype, Timestamp):
            fields += [".".join(path + [n])]

    return fields


def non_nullable_field_tests(
    table_config: ResolvedTableConfig,
) -> list[AbstractBaseTest]:
    """For each non_nullable field in the table_config,
    return all relevant non-nullable field tests

    Args:
        table_config: The table config to retrieve non-nullable tests

    Returns:
        A list of non-nullable field tests
    """
    fields = get_non_nullable_fields(item=table_config.table.schema())
    tests = []
    for f in fields:
        tests.append(FieldNeverNullTest(table_config=table_config, column=f))
    return tests


def find_consistent_timestamp_offset(field: str, table: Table) -> ibis.Expr:
    """Find rows in field in table which are offset from midnight by a round amount.

    This can be an indicator that a timezone conversion to timestamp (which is
    an absolute value) has been applied incorrectly.

    Args:
        field: The name of the field to check
        table: The table

    Returns:
        Ibis expression
    """
    # Look for timestamps which are consistently offset from midnight. This
    # can be an indicator of values assigned to the wrong day

    return (table[field].strftime("%M:%S").isin(["00:00", "00:30"])) & (
        table[field].strftime("%H:%M:%S") != "00:00:00"
    )


def timestamp_field_tests(
    table_config: ResolvedTableConfig,
) -> list[AbstractBaseTest]:
    """For each non_nullable field in the table_config,
    return all relevant non-nullable field tests

    Args:
        table_config: The table config to retrieve non-nullable tests

    Returns:
        A list of non-nullable field tests
    """
    fields = get_timestamp_fields(item=table_config.table.schema())
    tests = []

    for f in fields:
        expr = functools.partial(find_consistent_timestamp_offset, f)

        test = CountMatchingRows(
            table_config=table_config,
            column=f,
            max_proportion=0.10,
            expression=expr,
            test_id="P052",
        )
        tests.append(test)
    return tests


def get_generic_table_tests(
    table_config: ResolvedTableConfig, expected_max_rows: float
) -> list[AbstractBaseTest]:
    """Generate tests which are completely generic and agnostic to the
    underlying table

    Args:
        table_config: The table config to retrieve generic tests
        expected_max_rows: The expected maximum number of rows. Can be modified
                           by changing the scale configuration option

    Returns:
        A list of tests to run on the table
    """
    config = cfg()
    tests = [
        TableExcessColumnsTest(table_config, test_id="F001"),
        TableCountTest(
            table_config, max_rows=expected_max_rows * config.scale, test_id="T001"
        ),
    ]
    if table_config.table_type in (
        TableType.CLOSED_ENDED_ENTITY,
        TableType.OPEN_ENDED_ENTITY,
    ):
        tests += [
            CountMatchingRows(
                table_config=table_config,
                column="validity_start_time",
                expression=lambda t: t.validity_start_time.date()
                > cfg().interval_end_date,
                test_id="DT001",
            ),
            CountFrequencyValues(
                table_config=table_config,
                column="is_entity_deleted",
                having=lambda c: c.is_entity_deleted == literal(True),
                proportion=0.4,
                severity=AMLAITestSeverity.WARN,
                test_id="P050",
            ),
            CountFrequencyValues(
                table_config=table_config,
                column="validity_start_time",
                proportion=0.01,
                severity=AMLAITestSeverity.WARN,
                test_id="P001",
            ),
        ]
    return tests
