from typing import Optional
from amlaidatatests.config import ConfigSingleton
import ibis
from urllib.parse import parse_qsl, urlparse


def connection_factory(default: Optional[str] = None):
    config = ConfigSingleton.get()
    connection_string = config.get("connection_string", default)
    # Workaround https://github.com/ibis-project/ibis/issues/9456,
    # which means that 
    result = urlparse(connection_string)
    kwargs = dict(parse_qsl(result.query))

    connection = ibis.connect(connection_string, **kwargs)
    return connection