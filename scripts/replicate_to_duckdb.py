#!/usr/bin/env python


import importlib.resources
import pathlib

import google.auth
import ibis
import pandas as pd
from ibis import literal

from amlaidatatests.cli import build_parser
from amlaidatatests.connection import connection_factory
from amlaidatatests.schema.utils import get_table_name

credentials, project_id = google.auth.default()


def load_directory(source_connection, target_connection, tables: list[str]):
    for t in tables:
        t_bq = source_connection.table(t + "_1234")
        target_table_name = get_table_name(t)
        print("Loading into", target_table_name)

        # t = connection.table(f"{t}_{SUFFIX}")
        temp_table = target_connection.create_table(
            f"{t}_temp", obj=t_bq.to_pandas(), temp=True
        )
        target_table = target_connection.create_table(
            target_table_name, schema=t_bq.schema()
        )

        target_schema = target_table.schema()
        temp_schema = temp_table.schema()

        missing_columns = []

        for n, dtype in target_schema.items():
            if n not in temp_schema:
                missing_columns.append((n, dtype))

        # Add any columns which are missing in the
        all_temp_table = temp_table.mutate(
            **{n: literal(None).cast(dtype) for n, dtype in missing_columns}
        )

        all_temp_table_2 = all_temp_table.select(
            **{n: getattr(all_temp_table, n) for n in target_schema.keys()}
        )

        target_connection.insert(
            target_table_name, obj=all_temp_table_2, overwrite=True
        )
        # connection.create_table(name=f"{t}_{SUFFIX}", obj=table_out, overwrite=True, schema=party_schema)


if __name__ == "__main__":

    parser = build_parser()
    args = parser.parse_args()
    target_connection = connection_factory()

    # INPUT_DATASET_CONNECTION = ibis.bigquery.connect(
    #     project_id="gtai-amlai-sandbox",
    #     dataset_id="my_bq_input_dataset",
    #     credentials=credentials,
    # )

    # load_directory(
    #     bigquery_connection=INPUT_DATASET_CONNECTION,
    #     tables=["transaction", "party"],
    #     connection=connection,
    # )

    load_directory(
        source_connection=ibis.connect("duckdb://duckdb.ddb"),
        tables=[
            "party",
            "account_party_link",
            "risk_case_event",
            "party_supplementary_data",
            "transaction",
        ],
        target_connection=target_connection,
    )
