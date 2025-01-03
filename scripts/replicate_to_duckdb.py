#!/usr/bin/env python


import argparse
import importlib.resources
import pathlib
from string import Template

import google.auth
import ibis
import pandas as pd
from ibis import literal

credentials, project_id = google.auth.default()


def get_table_name(name: str, suffix: str) -> str:
    """Get the fully resolved table name for the provided string

    Args:
        name: A table name corresponding to a table in the configured schema

    Returns:
        A fully qualified table name
    """
    name_template = Template("${table}_${id}")
    return name_template.substitute({"id": suffix, "table": name})


def load_directory(
    source_connection,
    target_connection,
    tables: list[str],
    source_suffix: str,
    target_suffix: str,
):
    for t in tables:
        t_bq = source_connection.table(get_table_name(t, source_suffix))
        target_table_name = get_table_name(t, target_suffix)
        print("Loading into", target_table_name)

        # create a temporary staging table
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


if __name__ == "__main__":
    # parser = argparse.ArgumentParser()

    # parser.add_argument("source_connection_string")
    # parser.add_argument("target_connection_string")

    # args = parser.parse_args()

    source_connection = ibis.connect(
        "bigquery://utopian-pact-429518-p8/aml_ai_input_dataset?location=US"
    )
    target_connection = ibis.connect("duckdb://duckdb2.ddb")

    load_directory(
        source_connection=source_connection,
        source_suffix="v2",
        target_suffix="1234",
        tables=[
            "party",
            "account_party_link",
            "risk_case_event",
            "party_supplementary_data",
            "transaction",
        ],
        target_connection=target_connection,
    )
