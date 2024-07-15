from typing import Optional
from urllib.parse import parse_qsl, urlparse

import ibis

from amlaidatatests.config import ConfigSingleton


def connection_factory(default: Optional[str] = None):
    config = ConfigSingleton.get()
    connection_string = config.get("connection_string", default)
    # Workaround https://github.com/ibis-project/ibis/issues/9456,
    # which means that
    result = urlparse(connection_string)
    kwargs = dict(parse_qsl(result.query))

    connection = ibis.connect(connection_string, **kwargs)
    # We also need to set the backend to avoid always passing around the connection
    # object.
    ibis.set_backend(connection_string)

    return connection
