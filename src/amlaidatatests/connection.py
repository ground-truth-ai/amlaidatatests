from amlaidatatests.config import ConfigSingleton
import ibis

def connection_factory():
    config = ConfigSingleton.get()
    connection = ibis.connect(config.connection_string)
    return connection