import argparse
from dataclasses import dataclass, fields
from typing import Any, Optional
from urllib.parse import parse_qsl, urlparse

from omegaconf import OmegaConf

from .singleton import Singleton


def infer_database(connection_str: str) -> str | None:
    """Infer the database from the provided connection string

    For example if the connection string provided is:
    bigquery://my-project/my_dataset?location=us-central1, this function
    will return my_dataset

    Args:
        connection_str: An ibis connection string

    Returns:
        The inferred database, or None
    """
    parsed_url = urlparse(connection_str)

    if parsed_url.scheme == "bigquery":
        # "/my_bq_input_dataset" -> "my_bq_input_dataset"
        return parsed_url.path[1:]
    if parsed_url.scheme == "duckdb":
        return None
    raise ValueError(f"Unsupported database: {parsed_url.scheme}")


OmegaConf.register_new_resolver("infer_database", infer_database)


@dataclass(kw_only=True)
class DatatestConfig:
    id: Optional[str] = None
    """ Unique identifier for a set of associated tables"""

    connection_string: str
    """ The ibis connection string """

    schema_version: str = "v1"
    table_name_template: str = "\\${table}_\\${id}"
    """ Template for building table path. Defaults to <table>
    if id is not set, otherwise <table>_<id>"""

    database: Optional[str] = "${infer_database:${connection_string}}"
    """ For bigquery, the dataset being used """

    scale: float = 1.0


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


STRUCTURED_CONFIG = OmegaConf.structured(DatatestConfig)


class IngestConfigAction(argparse.Action):
    def __init__(
        self,
        option_strings,
        dest,
        default=None,
        required=False,
        help=None,
    ):

        super().__init__(
            option_strings=option_strings,
            dest=dest,
            default=default,
            required=required,
            help=help,
        )

    def __call__(self, parser, namespace, values, option_string=None):
        current_conf = ConfigSingleton.get()
        if option_string == "--conf":
            conf = OmegaConf.load(values)
            conf = OmegaConf.merge(STRUCTURED_CONFIG, current_conf, conf)
        else:
            # TODO: We're not handling nested configuration here
            conf_for_param = {option_string.replace("--", ""): values}
            conf = OmegaConf.merge(STRUCTURED_CONFIG, current_conf, conf_for_param)
        ConfigSingleton().set_config(conf)

    def format_usage(self) -> str:
        return " | ".join(self.option_strings)


def init_config(parser, defaults={}):
    ConfigSingleton().set_config(STRUCTURED_CONFIG)
    if isinstance(parser, argparse.ArgumentParser):
        parser.addoption = parser.add_argument
    parser.addoption(f"--conf", action=IngestConfigAction)
    for field in fields(DatatestConfig):
        parser.addoption(
            f"--{field.name}",
            action=IngestConfigAction,
            default=defaults.get(field.name) or field.default,
        )
    return parser


if __name__ == "__main__":
    conf = OmegaConf.structured(DatatestConfig)
    conf = OmegaConf.merge(conf, OmegaConf.from_cli())
    config_singleton = ConfigSingleton()
    config_singleton.set_config(conf)
