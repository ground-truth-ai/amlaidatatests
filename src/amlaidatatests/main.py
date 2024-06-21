import os
from amlaidatatests.config import ConfigSingleton, DatatestConfig
from omegaconf import OmegaConf
import pytest
import argparse
from omegacli import parse_config
import json
import sys

dir_path = os.path.dirname(os.path.realpath(__file__))

tables = ["party", "account_party_link", "risk_case_event", "transaction"]

    # for t in tables:
    #     tbl_path = p.joinpath(f"{t}.parquet")
    #     table_name = f"{t}_{SUFFIX}"
    #     print("Loading from ", tbl_path, " into ", table_name)
    #     #table_out = ibis.read_parquet(tbl_path, )
    #     tmp_table = connection.read_parquet(tbl_path)
    #     connection.create_table(name=f"{t}_{SUFFIX}", obj=tmp_table.select(tmp_table.columns), overwrite=True)
    #     #connection.create_table(name=f"{t}_{SUFFIX}", obj=table_out, overwrite=True, schema=party_schema)


if __name__ == "__main__":
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

    retcode = pytest.main(args=[f'{dir_path}/tests', *sys.argv])