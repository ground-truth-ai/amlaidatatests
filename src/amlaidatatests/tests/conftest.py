"""pytest configuration file for integration with amlaitests"""

import logging
from dataclasses import fields
from typing import Optional

import pytest
from omegaconf import OmegaConf

from amlaidatatests.base import AbstractTableTest
from amlaidatatests.config import (
    ConfigSingleton,
    DatatestConfig,
    init_parser_options_from_config,
)
from pathlib import Path
import pathlib

pytest_plugins = [
    "amlaidatatests.tests.fixtures.fixtures",
]


STRUCTURED_CONF = OmegaConf.structured(DatatestConfig)

logging.captureWarnings(True)


def pytest_addoption(parser: pytest.Parser, defaults: Optional[dict] = None) -> None:
    """Pytest hook to add configuration options to the pytest cli

    This hook iterates though each of each of the config objects in
    the [ConfigSingleton] and adds them to pytest cli.

    Args:
        parser         : Pytest parser
        defaults       : Default override for underlying field
    """
    if defaults is None:
        defaults = {}

    # Use this attribute to mark the attributes as already added.
    # We use multiple levels of conftest to allow both the tests
    # themselves and the tests to run independently of one another
    if hasattr(parser, "__AMLAIDATATESTS_ARGS_ADDED"):
        return
    # Initialize and set the default configuration
    ConfigSingleton().set_config(STRUCTURED_CONF)
    parser = init_parser_options_from_config(parser=parser, defaults=defaults)
    setattr(parser, "__AMLAIDATATESTS_ARGS_ADDED", True)


def pytest_configure(config) -> None:
    # pylint: disable=unused-argument
    """Pytest hook to configure pytests before tests run

    1) Iterates through each of the configuration variables
    and verify that any mandatory variables are set. This is
    because

    Args:
        config (_type_): _description_
    """
    cfg = ConfigSingleton().get()
    for field in fields(DatatestConfig):
        # Getting the attribute like this forces Omegaconf to resolve it. If the
        # field is missing, it raises an exception
        getattr(cfg, field.name)

    logging.captureWarnings(True)


def pytest_make_parametrize_id(config, val, argname) -> str | None:
    # pylint: disable=unused-argument
    if isinstance(val, AbstractTableTest):
        return val.id
    # return None to let pytest handle the formatting
    return None


@pytest.hookimpl(optionalhook=True)
def pytest_html_results_summary(prefix, summary, postfix) -> None:
    # pylint: disable=unused-argument
    prefix.extend(
        [
            "<p>Due a limitation of pytest, warnings are not shown in the header of"
            " this report but available in the table</p>"
        ]
    )


@pytest.hookimpl(optionalhook=True)
def pytest_html_results_table_header(cells) -> None:
    cells.insert(1, '<th class="sortable int" data-column-type="int">Warnings</th>')


@pytest.hookimpl(optionalhook=True)
def pytest_html_results_table_row(report, cells) -> None:
    cells.insert(1, f'<td class="col-int">{report.warning}</td>')


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # pylint: disable=unused-argument
    outcome = yield
    report = outcome.get_result()
    report.warning = report.caplog.count("WARNING")


@pytest.hookimpl(trylast=True)
def pytest_itemcollected(item):
    old_nodeid = item._nodeid
    # src/amlaidatatests/tests/test_account_party_link.py::test_column_type[source_system]'
    path, function = old_nodeid.rsplit("::", maxsplit=1)
    pth = pathlib.Path(path)
    new_nodeid = f"{pth.name}::{function}"
    item._nodeid = new_nodeid
    return item
