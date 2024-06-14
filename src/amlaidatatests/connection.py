import ibis
import google.auth

# TODO: Suffix needs to be configurable (probably via Omegaconf)
SUFFIX = "1234"

def connection_factory():
    connection = ibis.connect("duckdb://duckdb.ddb")
    return connection

# TODO: Configure connections properly
def bigquery_connection_factory():
    credentials, project_id = google.auth.default()
    connection = ibis.bigquery.connect(
        project_id="gtai-amlai-sandbox",
        dataset_id="my_bq_input_dataset",
        credentials=credentials
    )

    return connection

