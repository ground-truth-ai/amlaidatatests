""" Utility CLI for amlaidatatests """

import argparse
import os
import sys

import pytest

from amlaidatatests.config import ConfigSingleton, init_parser_options_from_config
from amlaidatatests.connection import connection_factory
from amlaidatatests.schema.utils import get_amlai_schema, get_table_name

dir_path = os.path.dirname(os.path.realpath(__file__))


def create_skeleton(args):
    cfg = ConfigSingleton.get()
    version = cfg.schema_version
    schema = get_amlai_schema(version)
    connection = connection_factory()
    for table in schema.TABLES:
        connection.create_table(name=get_table_name(table.name), schema=table.schema)


def run_tests(extras):
    # Pass the entire set of arguments through to pytest
    args = [f"{dir_path}/tests", *sys.argv[1:]]
    print(args)
    pytest.main(args=args)


def entry_point():
    """Configure an argparse based entry point for amlaidatatests

    This is configured somewhat differently from the pytest dataset, which is
    somewhat clunky to configure"""
    parser = argparse.ArgumentParser()
    parser = init_parser_options_from_config(parser)

    parser.set_defaults(func=run_tests)

    args, extra = parser.parse_known_args()

    args.func(extra)


if __name__ == "__main__":
    entry_point()
