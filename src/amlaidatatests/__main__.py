import argparse
import os
import sys

import pytest

from amlaidatatests.config import ConfigSingleton, init_config
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


def run_tests(args):
    pytest.main(args=[f"{dir_path}/tests", *sys.argv[1:]])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser = init_config(parser)
    subparsers = parser.add_subparsers(required=True)

    skeleton = subparsers.add_parser("skeleton")
    tests = subparsers.add_parser("tests")

    tests.add_argument("args", nargs="+")

    tests.set_defaults(func=run_tests)
    skeleton.set_defaults(func=create_skeleton)

    args = parser.parse_args()
    args.func(args)
