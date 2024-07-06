from amlaidatatests.config import ConfigSingleton, DatatestConfig, IngestConfigAction
from amlaidatatests.base import AbstractTableTest
from omegaconf import OmegaConf
import argparse
from dataclasses import fields

import pytest

pytest_plugins = [
    "amlaidatatests.tests.fixtures.fixtures",
]

STRUCTURED_CONF = OmegaConf.structured(DatatestConfig)


def pytest_addoption(parser, defaults={}) -> None:
    """Pytest hook for adding options to the pytest CLI

    Iterates thought each of each of the config objects in
    the the structured config and adds them to pytest.

    Args:
        parser (_type_): _description_
        defaults       : Default override for underlying field
    """
    # Use also to initialize the configuration singleton. This allows
    if hasattr(parser, "__AMLAIDATATESTS_ARGS_ADDED"):
        return
    ConfigSingleton().set_config(STRUCTURED_CONF)
    parser.addoption(f"--conf", action=IngestConfigAction)
    for field in fields(DatatestConfig):
        parser.addoption(
            f"--{field.name}",
            action=IngestConfigAction,
            default=defaults.get(field.name) or field.default,
        )
    setattr(parser, "__AMLAIDATATESTS_ARGS_ADDED", True)


def pytest_configure(config):
    """Pytest hook to configure pytests before tests run

    1) Iterates through each of the configuration variables
    and verify that any mandatory variables are set

    Args:
        config (_type_): _description_
    """
    _cfg = ConfigSingleton().get()
    for field in fields(DatatestConfig):
        getattr(_cfg, field.name)


def pytest_make_parametrize_id(config, val, argname):
    if isinstance(val, AbstractTableTest):
        return val.id
    # return None to let pytest handle the formatting
    return None


def pytest_make_parametrize_id(config, val, argname):
    if isinstance(val, AbstractTableTest):
        return val.id
    # return None to let pytest handle the formatting
    return None
