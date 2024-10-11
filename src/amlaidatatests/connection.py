import importlib
from typing import Optional
from urllib.parse import parse_qsl, urlparse

import ibis

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
    result = urlparse(connection_string)
    kwargs = dict(parse_qsl(result.query))

    _verify_required_packages(scheme=result.scheme)

    if config.dry_run:
        # For dry runs, create a duckdb database instead
        # for the purpose of getting the tests and various
        # checks to run cleanly
        ibis.set_backend("duckdb")
        connection = ibis.connect("duckdb://")
        for t in SchemaConfiguration.TABLES:
            try:
                from duckdb.duckdb import CatalogException  # type: ignore
            except ImportError as e:
                raise ImportError("Duckdb is required for a dry run") from e
            try:
                connection.create_database(config.database)
            # We need a bare exception here. We're trying to
            # avoid making duckdb a compulsory dependency, but
            # we cannot catch an optional dependency
            except CatalogException:
                # The database may already exists
                pass

            connection.create_table(
                name=get_table_name(t.name),
                schema=t.schema,
                database=config.database,
                overwrite=True,
            )
        return connection

    # Workaround the ibis library depending on pydata. TODO: Look into this
    # in more detail
    if result.scheme == "bigquery":
        import google.auth

        if is_real_execution:
            credentials, _ = google.auth.default()
            kwargs["credentials"] = credentials

    connection = ibis.connect(connection_string, **kwargs)
    # We also need to set the ibis backend to avoid always passing around the connection
    # object. This allows ibis.to_sql to successfully generate sql in the right language
    ibis.set_backend(connection)

    return connection
