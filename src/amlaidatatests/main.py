import os
from amlaidatatests.connection import connection
from amlaidatatests.io import load_from_bigquery_to_copy
from amlaidatatests.schema.v1.common import entity_columns, non_nullable_fields
import pytest
from amlaidatatests.schema.v1 import party_schema

dir_path = os.path.dirname(os.path.realpath(__file__))

tables = ["party", "account_party_link", "risk_case_event", "transaction"]

SUFFIX = "1234"

def create_empty_tables():
    connection.create_table(name=f"party_{SUFFIX}", overwrite=True, schema=party_schema)

    # for t in tables:
    #     tbl_path = p.joinpath(f"{t}.parquet")
    #     table_name = f"{t}_{SUFFIX}"
    #     print("Loading from ", tbl_path, " into ", table_name)
    #     #table_out = ibis.read_parquet(tbl_path, )
    #     tmp_table = connection.read_parquet(tbl_path)
    #     connection.create_table(name=f"{t}_{SUFFIX}", obj=tmp_table.select(tmp_table.columns), overwrite=True)
    #     #connection.create_table(name=f"{t}_{SUFFIX}", obj=table_out, overwrite=True, schema=party_schema)


if __name__ == "__main__":
    # load_from_bigquery_to_copy()
    retcode = pytest.main(args=[f'{dir_path}/tests/test_party_table.py'])
