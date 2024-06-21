from dataclasses import MISSING, dataclass
from typing import Any, Optional, cast
from omegaconf import OmegaConf
from .singleton import Singleton


@dataclass(kw_only=True)
class DatatestConfig:
    id: Optional[str] = None
    """ Unique identifier for """
    
    connection_string: str

    schema_version: str = 'v1'
    table_template: str = '\\${table}_\\${id}'
    """ Template for building table path. Defaults to <table>
    if id is not set, otherwise <table>_<id>"""


class ConfigSingleton(metaclass=Singleton):
    def __init__(self) -> None:
        self.cfg: Optional[DatatestConfig] = None

    def set_config(self, cfg: DatatestConfig) -> None:
        assert cfg is not None
        self.cfg = cast(DatatestConfig, cfg)

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