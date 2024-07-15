import google.auth
import ibis


# TODO: Configure connections properly
def bigquery_connection_factory():
    credentials, project_id = google.auth.default()
    connection = ibis.bigquery.connect(
        project_id=project_id, dataset_id="my_bq_input_dataset", credentials=credentials
    )

    return connection
