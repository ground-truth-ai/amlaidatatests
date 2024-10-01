#!/usr/bin/env python

from amlaidatatests.cli import build_parser
from amlaidatatests.config import ConfigSingleton
from amlaidatatests.connection import connection_factory
from amlaidatatests.schema.utils import get_amlai_schema, get_table_name


def create_skeleton(args):
    """Create an empty schema which is to specification"""
    cfg = ConfigSingleton.get()
    version = cfg.schema_version
    schema = get_amlai_schema(version)
    connection = connection_factory()
    for table in schema.TABLES:
        connection.create_table(name=get_table_name(table.name), schema=table.schema)


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    create_skeleton(args)
