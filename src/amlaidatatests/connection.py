from typing import Optional
from urllib.parse import parse_qsl, urlparse

import ibis

from amlaidatatests.config import ConfigSingleton


def connection_factory(default: Optional[str] = None):
    config = ConfigSingleton.get()
    connection_string = config.get("connection_string", default)
    # Workaround https://github.com/ibis-project/ibis/issues/9456,
    # which means that connection details aren't properly parsed out
    result = urlparse(connection_string)
    kwargs = dict(parse_qsl(result.query))
    # Workaround the ibis library depending on pydata. TODO: Look into this
    # in more detail
    if result.scheme == "bigquery":
        import google.auth

        credentials, _ = google.auth.default()
        kwargs["credentials"] = credentials

    connection = ibis.connect(connection_string, **kwargs)
    # We also need to set the ibis backend to avoid always passing around the connection
    # object. This allows ibis.to_sql to successfully generate sql in the right language
    ibis.set_backend(connection)

    return connection
