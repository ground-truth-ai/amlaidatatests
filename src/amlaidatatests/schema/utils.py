import importlib
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

    except ModuleNotFoundError:
        raise ValueError(f"Schema version {version} not found")


def get_table_name(name: str):
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
    """Get the unbound ibis table reference for the unqualified table name specified"""
    table_config = get_table_config(name)
    cfg = ConfigSingleton.get()
    name = get_table_name(name)
    resolved_table_config = ResolvedTableConfig(
        table=ibis.table(schema=table_config.schema, name=name, database=cfg.database),
        optional=table_config.optional,
        is_open_ended_entity=table_config.is_open_ended_entity,
    )
    return resolved_table_config


if __name__ == "__main__":
    get_amlai_schema()
