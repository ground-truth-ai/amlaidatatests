import importlib
from dataclasses import asdict
from string import Template

import ibis

from amlaidatatests.config import ConfigSingleton
from amlaidatatests.schema.base import (
    BaseSchemaConfiguration,
    ResolvedTableConfig,
    TableConfig,
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


def resolve_table_config(name: str) -> ResolvedTableConfig:
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
        table=ibis.table(schema=table_config.schema, name=name, database=cfg.database),
        **dct,
    )
    return resolved_table_config
