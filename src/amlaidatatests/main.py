import os
import sys
from amlaidatatests.config import ConfigSingleton, init_config
from amlaidatatests.connection import connection_factory
from amlaidatatests.schema.utils import get_schema_version_config, get_table_name
import argparse

import pytest

dir_path = os.path.dirname(os.path.realpath(__file__))


def create_skeleton(args):
    cfg = ConfigSingleton.get()
    version = cfg.schema_version
    schema = get_schema_version_config(version)
    connection = connection_factory()
    for table in schema.TABLES:
        connection.create_table(name=get_table_name(table.name), schema=table.schema)


def run_tests(args):
    print(dir_path)
    print()
    pytest.main(args=[f"{dir_path}/tests", *sys.argv[1:]])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser = init_config(parser)
    subparsers = parser.add_subparsers(required=True)

    skeleton = subparsers.add_parser("skeleton")
    skeleton.set_defaults(func=create_skeleton)

    args = parser.parse_args()
    args.func(args)

    # # Do not add help to prevent argparse from capturing any help requests
    # # the -h is present to prevent
    # parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    # parser.add_argument('--conf')

    # args, _ = parser.parse_known_args()
    # file_conf: dict = OmegaConf.load(args.conf) if args.conf else {}

    # parser = argparse.ArgumentParser(description=__doc__, add_help=True)

    # parser.add_argument('--id',
    #                     help="ID of the run which is used to uniquely associated the tables with one another")
    # parser.add_argument('-u',
    #                     '--connection-string',
    #                     help="Ibis connection string for the database to connect to", required=file_conf.get("connection_string") is None)
    # parser.add_argument('--conf', help="Load configuration from a yaml config file")

    # parser.set_defaults(**dict(file_conf))

    # config_file = args.conf

    # cfg = parse_config(parser=parser, config_file=config_file)
    # # These keys are not part of the structured config passed to pytest:
    # # don't
    # ignore_keys = ["conf"]
    # for k in ignore_keys:
    #     del cfg[k]

    # # Merge in config to Omegaconf,
    # conf = OmegaConf.structured(DatatestConfig)
    # conf = OmegaConf.merge(conf, cfg)
    # config_singleton = ConfigSingleton()
    # config_singleton.set_config(conf)

    # retcode = pytest.main(args=[f'{dir_path}/tests', *sys.argv])
