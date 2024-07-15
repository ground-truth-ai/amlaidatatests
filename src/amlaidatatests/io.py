import importlib.resources

import pandas as pd
from ibis import literal

from amlaidatatests.schema.utils import get_table_name

from .connection import connection_factory
import google.auth
import ibis


# TODO: Configure connections properly
def bigquery_connection_factory():
    credentials, project_id = google.auth.default()
    connection = ibis.bigquery.connect(
        project_id=project_id, dataset_id="my_bq_input_dataset", credentials=credentials
    )

    return connection

tables = [
    "party_supplementary_data"
]  # ["party", "account_party_link", "risk_case_event", "transaction"]


def load_from_bigquery_to_empty_table():
    for t in tables:
        table_name = get_table_name(t)
        bigquery_connection = bigquery_connection_factory()
        t_bq = bigquery_connection.table(t)

        print("Loading into", table_name)

        # t = connection.table(f"{t}_{SUFFIX}")
        connection = connection_factory()
        temp_table = connection.create_table(
            f"{t}_temp", obj=t_bq.to_pandas(), temp=True
        )
        target_table = connection.table(get_table_name(t))

        target_schema = target_table.schema()
        temp_schema = temp_table.schema()

        missing_columns = []

        for n, dtype in target_schema.items():
            if n not in temp_schema:
                missing_columns.append((n, dtype))

        all_temp_table = temp_table.mutate(
            **{n: literal(None).cast(dtype) for n, dtype in missing_columns}
        )

        all_temp_table_2 = all_temp_table.select(
            **{n: getattr(all_temp_table, n) for n in target_schema.keys()}
        )

        connection.insert(get_table_name(t), obj=all_temp_table_2, overwrite=True)
        # connection.create_table(name=f"{t}_{SUFFIX}", obj=table_out, overwrite=True, schema=party_schema)


def load_from_bigquery_to_copy():
    for t in tables:
        connection = connection_factory()
        table_name = get_table_name(t)
        bigquery_connection = bigquery_connection_factory()
        t_bq = bigquery_connection.table(t)

        print("Loading into", table_name)

        # t = connection.table(f"{t}_{SUFFIX}")
        temp_table = connection.create_table(
            get_table_name(t), obj=t_bq.to_pandas(), overwrite=True
        )


def get_valid_region_codes():
    template_res = importlib.resources.files("amlaidatatests.seeds").joinpath(
        "country_codes.csv"
    )
    with importlib.resources.as_file(template_res) as template_file:
        return pd.read_csv(template_file, na_values=[], keep_default_na=False)[
            "code"
        ].to_list()


def get_valid_currency_codes():
    template_res = importlib.resources.files("amlaidatatests.seeds").joinpath(
        "currency_codes.csv"
    )
    with importlib.resources.as_file(template_res) as template_file:
        return pd.read_csv(template_file, na_values=[], keep_default_na=False)[
            "code"
        ].to_list()


# def copy_prefix() ->
