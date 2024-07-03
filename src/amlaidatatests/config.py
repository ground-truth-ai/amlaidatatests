from dataclasses import dataclass
from typing import Any, Optional
from omegaconf import OmegaConf
from .singleton import Singleton
from urllib.parse import parse_qsl, urlparse

def infer_database(connection_str: str):
    """ Infer the database from the provided connection string """
    parsed_url = urlparse(connection_str)
    kwargs = dict(parse_qsl(parsed_url.query))

    if parsed_url.scheme == "bigquery":
        # "/my_bq_input_dataset" -> "my_bq_input_dataset"
        return parsed_url.path[1:]

OmegaConf.register_new_resolver("infer_database", infer_database)

@dataclass(kw_only=True)
class DatatestConfig:
    id: Optional[str] = None
    """ Unique identifier for """
    
    connection_string: str

    schema_version: str = 'v1'
    table_name_template: str = '\\${table}_\\${id}'
    """ Template for building table path. Defaults to <table>
    if id is not set, otherwise <table>_<id>"""

    database: Optional[str] = "${infer_database:${connection_string}}"
    """ For bigquery, the dataset being used """




class ConfigSingleton(metaclass=Singleton):
    def __init__(self) -> None:
        self.cfg: Optional[DatatestConfig] = None

    def set_config(self, cfg: DatatestConfig) -> None:
        assert cfg is not None
            
        self.cfg = cfg

    @staticmethod
    def get() -> DatatestConfig:
        instance = ConfigSingleton.instance()
        if instance.cfg is None:
            raise ValueError("Singleton was not set")
        return instance.cfg  # type: ignore

    @staticmethod
    def initialized() -> bool:
        instance = ConfigSingleton.instance()
        return instance.cfg is not None

    @staticmethod
    def instance(*args: Any, **kwargs: Any) -> "ConfigSingleton":
        return Singleton.instance(ConfigSingleton, *args, **kwargs)  # type: ignore

    @staticmethod
    def clear() -> None:
        instance = ConfigSingleton.instance()
        instance.cfg = None

if __name__ == "__main__":
    conf = OmegaConf.structured(DatatestConfig)
    conf = OmegaConf.merge(conf, OmegaConf.from_cli())
    config_singleton = ConfigSingleton()
    config_singleton.set_config(conf)