import logging
from dataclasses import fields

import pytest
from omegaconf import OmegaConf

from amlaidatatests.base import AbstractTableTest
from amlaidatatests.config import ConfigSingleton, DatatestConfig, IngestConfigAction

pytest_plugins = [
    "amlaidatatests.tests.fixtures.fixtures",
]


STRUCTURED_CONF = OmegaConf.structured(DatatestConfig)

logging.captureWarnings(True)


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
    parser.addoption("--conf", action=IngestConfigAction)
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

    logging.captureWarnings(True)


def pytest_make_parametrize_id(config, val, argname):
    if isinstance(val, AbstractTableTest):
        return val.id
    # return None to let pytest handle the formatting
    return None


def pytest_html_results_summary(prefix, summary, postfix):
    prefix.extend(
        [
            "<p>Due a limitation of pytest, warnings are not shown in the header of"
            " this report but available in the table</p>"
        ]
    )


def pytest_html_results_table_header(cells):
    cells.insert(1, '<th class="sortable int" data-column-type="int">Warnings</th>')


def pytest_html_results_table_row(report, cells):
    cells.insert(1, f'<td class="col-int">{report.warning}</td>')


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    report.warning = report.caplog.count("WARNING")

def pytest_collection_modifyitems(items):
    # will execute as late as possible
    print(items)
