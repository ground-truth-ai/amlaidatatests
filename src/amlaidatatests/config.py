import argparse
import datetime
from dataclasses import dataclass, field, fields
from typing import Any, Optional
from urllib.parse import urlparse

from omegaconf import OmegaConf
import pytest

from .singleton import Singleton


def cfg() -> "DatatestConfig":
    """Convenience function for retrieving the config from the ConfigSingleton
    in a short function.

    The config must have been initialized

    Returns:
        The DatatestConfig
    """
    return ConfigSingleton.get()


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
    """ The version of the AML AI schema """

    table_name_template: str = "\\${table}_\\${id}"
    """ Template for building table path. Defaults to <table>
    if id is not set, otherwise <table>_<id>"""

    database: Optional[str] = "${infer_database:${connection_string}}"
    """ For bigquery, the dataset being used """

    scale: float = 1.0
    """ Scale changes to modify profiling tests based on absolute values """

    interval_end_date: str = field(
        default_factory=lambda: datetime.date.today().isoformat()
    )
    """ The last date of the interval. Defaults to today. """


class ConfigSingleton(metaclass=Singleton):
    def __init__(self) -> None:
        self.cfg: Optional[DatatestConfig] = None

    def set_config(self, config: DatatestConfig) -> None:
        assert config is not None

        self.cfg = config

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

    def __call__(
        self,
        parser: argparse.ArgumentParser | pytest.Parser,
        namespace,
        values,
        option_string=None,
    ):
        current_conf = ConfigSingleton.get()
        if option_string == "--conf":
            config = OmegaConf.load(values)
            config = OmegaConf.merge(STRUCTURED_CONFIG, current_conf, config)
        else:
            # TODO: We're not handling nested configuration here
            conf_for_param = {option_string.replace("--", ""): values}
            config = OmegaConf.merge(STRUCTURED_CONFIG, current_conf, conf_for_param)
        ConfigSingleton().set_config(config)

    def format_usage(self) -> str:
        return " | ".join(self.option_strings)


def init_parser_options_from_config(
    parser: argparse.ArgumentParser | pytest.Parser, defaults: Optional[dict] = None
) -> argparse.ArgumentParser | pytest.Parser:
    """Initialize an argparse or pytest parser from a configuration file.

    Argparse and pytest's configuration parser are remarkably similar, but they
    do lack a few options. This function attempts to monkey patch each options to
    generate a consistent api across both.

    Args:
        parser: A pytest or argparse argument parser for configuration
        defaults: A dictionary of default overrides for options

    Returns:
        A pytest or argparse parser with a unified configuration across both
    """
    if defaults is None:
        defaults = {}
    ConfigSingleton().set_config(STRUCTURED_CONFIG)
    if isinstance(parser, argparse.ArgumentParser):
        parser.addoption = parser.add_argument
    parser.addoption("--conf", action=IngestConfigAction)
    for f in fields(DatatestConfig):
        parser.addoption(
            f"--{f.name}",
            action=IngestConfigAction,
            default=defaults.get(f.name) or f.default,
            help=f,
        )
    return parser


if __name__ == "__main__":
    conf = OmegaConf.structured(DatatestConfig)
    conf = OmegaConf.merge(conf, OmegaConf.from_cli())
    config_singleton = ConfigSingleton()
    config_singleton.set_config(conf)
