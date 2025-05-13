import datetime
import importlib
from dataclasses import asdict
from string import Template
from typing import List, Optional

import ibis
from ibis import _

from amlaidatatests.config import ConfigSingleton
from amlaidatatests.schema.base import (
    BaseSchemaConfiguration,
    ResolvedTableConfig,
    TableConfig,
    TableType,
)


def get_amlai_schema(version: str) -> BaseSchemaConfiguration:
    try:
        module = importlib.import_module(f"amlaidatatests.schema.{version}.tables")
        schema_configuration: BaseSchemaConfiguration = module.SchemaConfiguration()
        return schema_configuration

    except ModuleNotFoundError as e:
        raise ValueError(f"Schema version {version} not found") from e


def get_table_name(name: str) -> str:
    """Get the fully resolved table name for the provided string

    Args:
        name: A table name corresponding to a table in the configured schema

    Returns:
        A fully qualified table name
    """
    config_singleton = ConfigSingleton.get()
    name_template = Template(config_singleton.table_name_template)
    if config_singleton.id is None:
        return name
    else:
        return name_template.substitute({"id": config_singleton.id, "table": name})


def get_table_config(name: str) -> TableConfig:
    cfg = ConfigSingleton.get()
    version = cfg.schema_version

    return get_amlai_schema(version)[name]


def resolve_table_config(
    name: str, schema: Optional[ibis.Schema] = None
) -> ResolvedTableConfig:
    """Gets the unbound [ResolvedTableConfig] config for [name], which should
    be a table in the schema version

    Args:
        name: an unqualified reference to a table in the schema. Should not
              include any suffixes or prefixes.

    Returns:
        [ResolvedTableConfig] object
    """
    table_config = get_table_config(name)
    cfg = ConfigSingleton.get()
    name = get_table_name(name)
    # Concert from TableConfig to ResolvedTableConfig
    # does not have argument name
    dct = asdict(table_config)
    del dct["schema"]
    resolved_table_config = ResolvedTableConfig(
        table=ibis.table(
            schema=schema if schema else table_config.schema,
            name=name,
            database=cfg.database,
        ),
        **dct,
    )
    return resolved_table_config


def get_entity_state_windows(
    table_config: ResolvedTableConfig, key: List[str] | None = None
):
    """Generate a table indicating the time periods an entity
    was valid between.

    Limitation: currently does not handle time periods between
                time periods.

    Args:
        table_config: _description_
        key: Optional additional keys to aggregate to which are not table keys.
                For example, we might want to group by account_id
                on a transaction table. Setting this value will produce a group
                by to the transaction level.

    Returns:
        _description_
    """
    MAX_DATETIME_VALUE = datetime.datetime(9995, 1, 1, tzinfo=datetime.timezone.utc)
    # For a table with flipping is_entity_deleted:
    #
    # | party_id | validity_start_time | is_entity_deleted |
    # |    1     |      00:00:00       |       False       |
    # |    1     |      00:30:00       |       False       | <--- no flip
    # |    1     |      01:00:00       |       True        |
    # |    1     |      02:00:00       |       False       |
    #
    # |party_id|window_id| window_start_time|window_end_time|is_entity_deleted
    # |   1    |    0    |      00:00:00    |     null      |     False
    table = table_config.table

    key = [] if not key else key

    # Select potentially overlapping keys between these two entity tables
    keys = set([*key, *table_config.entity_keys])

    if table_config.table_type == TableType.EVENT:
        return table.select(first_date=_.event_time, last_date=_.event_time, *keys)

    w = ibis.window(group_by=keys, order_by="validity_start_time")

    # is_entity_deleted is a nullable field, so we assume if it is null, we
    # mean False
    cte0 = ibis.coalesce(table.is_entity_deleted, False)
    # First, find the changes in entity_deleted switching in order to
    # determine the rows which lead to the table switching. do this by
    # finding the previous values of "is_entity_deleted" and determining if.

    cte1 = table.select(
        *keys,
        "validity_start_time",
        is_entity_deleted=cte0,
        previous_entity_deleted=cte0.lag().over(w),
        next_row_validity_start_time=_.validity_start_time.lead().over(w),
        previous_row_validity_start_time=_.validity_start_time.lag().over(w),
    )

    # Handle the entity being immediately deleted by assuming it only
    # existed for a fraction of a second. This isn't valid data, but it
    # should be picked up by the entity mutation tests
    cte2 = cte1.mutate(
        previous_row_validity_start_time=ibis.ifelse(
            (_.previous_row_validity_start_time.isnull()) & (_.is_entity_deleted),
            _.validity_start_time,
            _.previous_row_validity_start_time,
        )
    )

    # Only return useful rows, not ones where the entities deletion state
    # didn't change
    cte3 = cte2.filter(
        (_.previous_row_validity_start_time == ibis.literal(None))  # first row
        | (_.next_row_validity_start_time == ibis.literal(None))  # last row
        | (_.is_entity_deleted != _.previous_entity_deleted)  # state flips
    ).group_by(keys)

    if table_config.table_type == TableType.CLOSED_ENDED_ENTITY:
        # For closed ended entities, e.g. parties, we need to assume the
        # entity persists until it is deleted. This means that the maximum
        # validity date time
        res = (
            cte3
            # At the moment, we only pay attention to the first/last dates,
            # not where there are multiple flips if the only row and the row
            # isn't yet deleted, we need to make the validity end time far
            # into the future
            .agg(
                # null handling not required as validity_start_time is a
                # non-nullable field
                first_date=_.validity_start_time.min(),
                last_date=ibis.ifelse(
                    condition=_.next_row_validity_start_time.isnull()
                    & ~_.is_entity_deleted,
                    true_expr=MAX_DATETIME_VALUE,
                    false_expr=_.validity_start_time,
                ).max(),
            )
        )
    else:
        # The entity's validity is counted only as of the last datetime provided
        res = (
            cte3
            # At the moment, we only pay attention to the first/last dates,
            # not where there are multiple flips
            .agg(
                first_date=_.validity_start_time.min(),
                last_date=_.validity_start_time.max(),
            )
        )
    # There may be multiple group_by keys, which doesn't correspond
    # to the referential integrity test testing a single key. This is because
    # we need to handle the case where there are multiple entity keys,
    # e.g. a party_link table where we are validating the account_id
    return res.group_by(key if key else keys).agg(
        first_date=_.first_date.min(), last_date=_.last_date.max()
    )
