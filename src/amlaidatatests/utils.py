"""Misc utils for amlaidatatests"""

from amlaidatatests.config import ConfigSingleton
from amlaidatatests.connection import connection_factory
from amlaidatatests.schema.base import ResolvedTableConfig
from amlaidatatests.schema.utils import get_amlai_schema, get_table_name


def create_empty_schema_tables():

    cfg = ConfigSingleton.get()
    version = cfg.schema_version
    schema = get_amlai_schema(version)

    connection = connection_factory()

    for table in schema.TABLES:
        connection.create_table(name=get_table_name(table.name), schema=table.schema)


def get_columns(table_config: ResolvedTableConfig):
    """Retrieve all columns for the provided table_config

    Args:
        table_config: Table config to retrieve columns for

    Returns:
        An iterable of column fields
    """
    return table_config.table.schema().fields.keys()


if __name__ == "__main__":
    pass
