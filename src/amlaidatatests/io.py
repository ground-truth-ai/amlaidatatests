import pathlib
from .connection import connection
import pandas as pd

tables = ["party", "account_party_link", "risk_case_event", "transaction"]

SUFFIX = "1234"

def load_directory(p: pathlib.Path | str):
    if isinstance(p, str):
        p = pathlib.Path(p)
    for t in tables:
        tbl_path = p.joinpath(f"{t}.parquet")
        table_name = f"{t}_{SUFFIX}"
        print("Loading from ", tbl_path, " into ", table_name)
        #table_out = ibis.read_parquet(tbl_path, )
        tmp_table = connection.read_parquet(tbl_path)
        connection.create_table(name=f"{t}_{SUFFIX}", obj=tmp_table.select(tmp_table.columns), overwrite=True)
        #connection.create_table(name=f"{t}_{SUFFIX}", obj=table_out, overwrite=True, schema=party_schema)

def get_valid_region_codes():# -> list:
    return pd.read_csv("/home/matt/amlaidatatests/seeds/country_codes.csv", na_values=[])["code"].to_list()

def get_valid_currency_codes():# -> list:
    return pd.read_csv("/home/matt/amlaidatatests/seeds/currency_codes.csv", na_values=[])["code"].to_list()