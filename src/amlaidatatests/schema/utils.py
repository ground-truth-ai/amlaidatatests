import importlib
from amlaidatatests.config import ConfigSingleton
from amlaidatatests.schema.base import BaseSchemaConfiguration
import ibis
from string import Template 

def get_schema_version_config() -> BaseSchemaConfiguration:
    # Interpolate the version config here
    cfg = ConfigSingleton.get()
    version = cfg.schema_version
    try:
        my_module = importlib.import_module(f'amlaidatatests.schema.{version}.tables')
    except ModuleNotFoundError:
        raise Exception(f"Schema version {version} not found")
    schema_configuration: BaseSchemaConfiguration = my_module.SchemaConfiguration()
    return schema_configuration


def get_table_name(name: str):
    config_singleton = ConfigSingleton.get()
    name_template = Template(config_singleton.table_template)
    if config_singleton.id is None:
        return name
    else:
        return name_template.substitute({
            'id': config_singleton.id,
            'table': name
        })

def get_table_schema(name: str) -> ibis.Schema:
    return get_schema_version_config().get_table_schema(name).schema

def get_table(name: str) -> ibis.Table:
    s = get_table_schema(name)
    return ibis.table(schema=s, name=get_table_name(name))


if __name__ == "__main__":
    get_schema_version_config()