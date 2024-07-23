"""pytest configuration file for integration with amlaitests"""

import logging
from dataclasses import fields
from typing import Optional

import pytest
from omegaconf import OmegaConf

from amlaidatatests.base import AbstractBaseTest
from amlaidatatests.config import (
    ConfigSingleton,
    DatatestConfig,
    init_parser_options_from_config,
)
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
    parser_group = parser.getgroup(
        "amlaidatatests options", description="Settings for amlaidatatests"
    )
    init_parser_options_from_config(parser=parser_group, defaults=defaults)
    setattr(parser, "__AMLAIDATATESTS_ARGS_ADDED", True)


def pytest_configure(config) -> None:
    # pylint: disable=unused-argument
    """Pytest hook to configure pytests before tests run

    Iterates through each of the configuration variables and verify that any
    mandatory variables are set. Omegaconf will verify it.

    Args:
        config: pytest config
    """
    if config.getoption("-h"):
        # Don't validate if help requested - we don't want to error
        return
    cfg = ConfigSingleton().get()
    for field in fields(DatatestConfig):
        # Getting the attribute like this forces Omegaconf to resolve it. If the
        # field is missing, it raises an exception
        try:
            getattr(cfg, field.name)
        except Exception as e:
            raise pytest.UsageError(e)

    logging.captureWarnings(True)


def pytest_make_parametrize_id(config, val, argname) -> str | None:
    """Pytest hook. Generates a name from parameterized pytests

    This hook connects the id in the [AbstractBaseTest] class with pytest so if
    pytest encounters any class derived from [AbstractBaseTest] in a
    parameterized test it can

    Args:
        config: unused pytesthook argument
        val: the value from a parameterized test
        argname: unused pytesthook argument

    Returns:
        A string identifying the test, or none. If none, pytest will generate a
        test id internally.
    """
    # pylint: disable=unused-argument
    if isinstance(val, AbstractBaseTest):
        return val.id
    # return None to let pytest handle the formatting
    return None


@pytest.hookimpl(optionalhook=True)
def pytest_html_results_summary(prefix, summary, postfix) -> None:
    """Pytest-html hook. Does not run if pytest-html is not installed

    Add a warning to the report

    Args:
        prefix: table prefix in html report
        summary: table summary in html report
        postfix: table postfix in html report
    """
    # pylint: disable=unused-argument
    prefix.extend(
        [
            "<p>Due a limitation of pytest, warnings are not shown in the header of"
            " this report but available in the table</p>"
        ]
    )


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Pytest-html hook. Does not run if pytest-html is not installed

    Compile the number of warnings captured by each test for consumption by the
    hooks further down this file

    Args:
        item: The reported pytest item call: the stage of the report collection
        process
    """
    # pylint: disable=unused-argument
    outcome = yield
    report = outcome.get_result()
    report.warning = report.caplog.count("WARNING")


@pytest.hookimpl(optionalhook=True)
def pytest_html_results_table_header(cells) -> None:
    """Pytest-html hook. Does not run if pytest-html is not installed

    Add a warning column header to the results table to provide a count  of the
    number of warnings in the output

    Args:
        cells: list of cells from pytest-html
    """
    cells.insert(1, '<th class="sortable int" data-column-type="int">Warnings</th>')


@pytest.hookimpl(optionalhook=True)
def pytest_html_results_table_row(report, cells) -> None:
    """Pytest-html hook. Does not run if pytest-html is not installed

    Add a warning column to the results table to provide a count of the number
    of warnings in the test output

    Args:
        report: warning output cells: list of cells from pytest-html
    """
    cells.insert(1, f'<td class="col-int">{report.warning}</td>')


@pytest.hookimpl(trylast=True)
def pytest_itemcollected(item):
    """Pytest hook running after each item is collected

    Strips out the full path for reporting compactness.

    Conventional format:
    tests/test_account_party_link.py::test_column_type[source_system]

    Normal format:
    test_account_party_link.py::test_column_type[source_system]

    Args:
        item: The collected node_id

    Returns:
        The modified node
    """
    old_nodeid = item._nodeid
    # e.g. normal format is
    #
    path, function = old_nodeid.rsplit("::", maxsplit=1)
    pth = pathlib.Path(path)
    new_nodeid = f"{pth.name}::{function}"
    item._nodeid = new_nodeid
    return item
