"""
Provides a centralized connection factory for creating ibis backend connections.

This module is responsible for parsing connection strings, handling different
database backends (e.g., BigQuery, DuckDB), and applying global configurations
such as `dry_run` mode or custom client properties like user-agents for
BigQuery API calls.
"""

import importlib
import re
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import ibis

from amlaidatatests import __version__
from amlaidatatests.config import ConfigSingleton
from amlaidatatests.schema.utils import get_table_name
from amlaidatatests.schema.v1.tables import SchemaConfiguration


# Replicated from ibis pyspark connector
class Options(ibis.config.Config):
    """PySpark options.

    Attributes
    ----------
    treat_nan_as_null : bool
        Treat NaNs in floating point expressions as NULL.

    """

    treat_nan_as_null: bool = False


def _verify_required_packages(scheme):
    """Verifies that optional backend dependencies are installed and configured.

    This function checks the connection scheme and raises an ImportError if the
    corresponding Python package for the ibis backend is not found. It also
    handles any backend-specific configuration needed before creating a
    connection.

    Args:
        scheme (str): The ibis connection scheme (e.g., 'duckdb', 'pyspark').

    Raises:
        ImportError: If the required package for the specified scheme is not
            installed.

    """
    # TODO: Consider if the package is actually required for a dryrun.
    # For pyspark, it is
    if scheme == "duckdb" and not importlib.util.find_spec("duckdb"):
        raise ImportError(
            "duckdb is not installed. To use duckdb, run "
            "`pip install amlaidatatests[duckdb]`"
        )
    if scheme == "pyspark":
        if not importlib.util.find_spec("pyspark"):
            raise ImportError(
                "pyspark is not installed. To use pyspark, run "
                "`pip install ibis-framework[pyspark]`"
            )
        ibis.options.pyspark = Options()
        ibis.options.pyspark.treat_nan_as_null = True


def connection_factory(default: Optional[str] = None):
    config = ConfigSingleton.get()

    is_real_execution = not config.dry_run

    connection_string = config.get("connection_string", default)
    # Workaround https://github.com/ibis-project/ibis/issues/9456,
    # which means that connection details aren't properly parsed out
    parsed_url = urlparse(connection_string)
    kwargs = dict(parse_qsl(parsed_url.query))

    # Extract job labels from query string
    label_keys = [k for k in kwargs.keys() if re.match(r"^labels\.(.*)$", k)]
    labels = dict(
        [
            (re.match(r"^labels\.(.*)$", k)[1], v)
            for k, v in kwargs.items()
            if k in label_keys
        ]
    )
    for k in label_keys:
        kwargs.pop(k)

    parsed_url = parsed_url._replace(query=urlencode(kwargs, True))
    connection_string = urlunparse(parsed_url)

    _verify_required_packages(scheme=parsed_url.scheme)

    if config.dry_run:
        # For dry runs, create a duckdb database instead
        # for the purpose of getting the tests and various
        # checks to run cleanly
        ibis.set_backend("duckdb")
        connection = ibis.connect("duckdb://")
        for t in SchemaConfiguration.TABLES:
            try:
                from duckdb.duckdb import CatalogException  # type: ignore # noqa: E402
            except ImportError as e:
                raise ImportError("Duckdb is required for a dry run") from e
            try:
                connection.create_database(config.database)
            # We need a bare exception here. We're trying to
            # avoid making duckdb a compulsory dependency, but
            # we cannot catch an optional dependency
            except CatalogException:
                # The database may already exist
                pass

            connection.create_table(
                name=get_table_name(t.name),
                schema=t.schema,
                database=config.database,
                overwrite=True,
            )
        return connection

    if parsed_url.scheme == "bigquery":
        import google.auth  # noqa: E402
        from google.api_core import client_info  # noqa: E402
        from google.cloud import bigquery  # noqa: E402

        info = client_info.ClientInfo(user_agent=f"gtai-amlaidatatests/{__version__}")

        if is_real_execution:
            credentials, _ = google.auth.default()
            kwargs["credentials"] = credentials

        query_job_config = bigquery.job.QueryJobConfig(labels=labels)

        bq_client = bigquery.Client(
            project=parsed_url.hostname,
            default_query_job_config=query_job_config,
            client_info=info,
        )

        connection = ibis.bigquery.connect(
            dataset_id=parsed_url.path.lstrip("/"), client=bq_client, **kwargs
        )
    else:
        connection = ibis.connect(connection_string, **kwargs)

    # We also need to set the ibis backend to avoid always passing around the connection
    # object. This allows ibis.to_sql to successfully generate sql in the right language
    ibis.set_backend(connection)

    return connection
