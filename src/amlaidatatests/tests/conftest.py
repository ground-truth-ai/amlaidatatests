"""pytest configuration file for integration with amlaitests"""

import logging
import pathlib
import typing
from dataclasses import dataclass, fields
from typing import Dict, Optional, Tuple

import pytest
from _pytest._code.code import ExceptionRepr, TerminalRepr
from omegaconf import OmegaConf

from amlaidatatests.base import AbstractBaseTest
from amlaidatatests.config import (
    ConfigSingleton,
    DatatestConfig,
    init_parser_options_from_config,
)

pytest_plugins = [
    "amlaidatatests.tests.fixtures.fixtures",
]


STRUCTURED_CONF = OmegaConf.structured(DatatestConfig)

logging.captureWarnings(True)


@pytest.hookimpl(trylast=True)
def pytest_addoption(parser: pytest.Parser, defaults: Optional[dict] = None) -> None:
    """Pytest hook to add configuration options to the pytest cli

    This hook iterates though each of each of the config objects in
    the [ConfigSingleton] and adds them to pytest cli.

    Args:
        parser         : Pytest parser
        defaults       : Default override for underlying field
    """
    if defaults is None:
        defaults = {"connection_string": "duckdb://"}

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


# store history of failures per test module
_test_failed_incremental: Dict[str, str] = {}


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Pytest hook on test report completion time

    1) Compiles the number of warnings captured by each test for consumption by the
    hooks further down this file
    2) Check if the test failed due to the table being missing.
    Record that the test failed and xfail all other tests in the module

    Args:
        item: The reported pytest item call: the stage of the report collection
        process
    """
    # pylint: disable=unused-argument
    outcome = yield
    report = outcome.get_result()
    report.warning = report.caplog.count("WARNING")
    # Tab
    if dict(report.user_properties).get("table_missing"):
        # the test has failed
        # retrieve the class name of the test
        module = str(item.module)
        # retrieve the name of the test function
        test_name = item.originalname or item.name
        # store in _test_failed_incremental the original name of the failed test
        _test_failed_incremental[module] = test_name


def pytest_runtest_setup(item):
    # retrieve the module name of the test
    module_name = str(item.module)
    # check if a previous test has failed for this module
    if test_name := _test_failed_incremental.get(module_name):
        # if name found, xfail this test
        if test_name is not None:
            pytest.xfail(
                "Skipping test as a previous test was unable "
                f"to query table ({test_name})"
            )


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
    ```tests/test_account_party_link.py::test_column_type[source_system]```

    Normal format:
    ```test_account_party_link.py::test_column_type[source_system]```

    Args:
        item: The collected node_id

    Returns:
        The modified node
    """
    old_nodeid = item._nodeid
    # e.g. normal format is
    path, function = old_nodeid.rsplit("::", maxsplit=1)
    pth = pathlib.Path(path)
    new_nodeid = f"{pth.name}::{function}"
    item._nodeid = new_nodeid
    return item


@dataclass
class AMLAITestReport:
    message: Optional[str]
    nodeid: str
    user_properties: dict[str, str]


def render_test_summary(
    terminalreporter, test_reports: list[AMLAITestReport], **markup: str
):
    for f in test_reports:
        # First line has exception header. This takes up
        # quite a bit of space so we'll remove it
        first_line = ""
        if f.message:
            first_line = f.message.split("\n")[0]
            first_line = first_line.replace(
                "amlaidatatests.exceptions.DataTestFailure: ", ""
            )
            find_str = "DataTestWarning: "
            first_line_idx = first_line.find(find_str)
            if first_line_idx > -1:
                first_line = first_line[first_line_idx + len(find_str) :]

        test_id: str = f.user_properties.get("test_id") or f.nodeid
        terminalreporter.write(test_id.ljust(7), bold=True, **markup)
        table_id: str = f.user_properties.get("table") or ""
        column_id: str = f.user_properties.get("column") or ""
        if table_id and column_id:
            terminalreporter.write(f"{table_id}.{column_id}".ljust(54))
        else:
            terminalreporter.write(f"{table_id}".ljust(54))
        terminalreporter.write(f"{first_line}\t")
        terminalreporter.write("\n")


def test_report_to_payload(
    test_reports: list[pytest.TestReport],
) -> list[AMLAITestReport]:
    """For each provided test report, convert into an
    AMLAITestReport object so we can standardize the
    output"""
    arr = []
    for rpt in test_reports:
        longrepr = rpt.longrepr
        message: str | None = None
        if isinstance(longrepr, Tuple):
            message = longrepr[-1]
        if isinstance(longrepr, pytest.ExceptionInfo):
            message = longrepr.getrepr().reprcrash.message
        if isinstance(longrepr, ExceptionRepr):
            message = longrepr.reprcrash.message
        if isinstance(longrepr, str):
            message = longrepr
        arr.append(
            AMLAITestReport(
                message=message,
                nodeid=rpt.nodeid,
                user_properties=dict(rpt.user_properties),
            )
        )
    return arr


def warn_report_to_payload(
    warning_reports: list, parsed_test_reports: list[AMLAITestReport]
):
    """Warnings don't have any information about the test output,
    only that a warning was captured. This means we need to get the outputs
     for any reports which might have errored"""
    arr = []
    lookup = {rpt.nodeid: rpt for rpt in parsed_test_reports}
    for rpt in warning_reports:
        main_test_report = lookup.get(rpt.nodeid)
        user_properties = {}
        if main_test_report:
            user_properties = main_test_report.user_properties
        arr.append(
            AMLAITestReport(
                message=rpt.message,
                nodeid=rpt.nodeid,
                user_properties=user_properties,
            )
        )
    return arr


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    terminalreporter.ensure_newline()
    terminalreporter.section("amlaidatatests summary", sep="=", blue=True, bold=True)

    passed_tests = test_report_to_payload(terminalreporter.getreports("passed"))
    failed_tests = test_report_to_payload(terminalreporter.getreports("failed"))
    skipped_tests = test_report_to_payload(terminalreporter.getreports("skipped"))
    errored_tests = test_report_to_payload(terminalreporter.getreports("error"))

    terminalreporter.section(
        f"tests passed: {len(passed_tests)}", sep="-", blue=True, bold=True
    )

    terminalreporter.section(
        f"skipped: {len(skipped_tests)}", sep="-", blue=True, bold=True
    )
    if skipped_tests:
        render_test_summary(terminalreporter, skipped_tests, light=True)

    # Warnings don't have user attributes on them, so we have to "join" the two lists
    warned_tests = terminalreporter.getreports("warnings")
    terminalreporter.section(
        f"warnings: {len(warned_tests)}", sep="-", blue=True, bold=True
    )
    if warned_tests:
        # Warnings can come from any test state, so we need to check all other test types
        all_reports = passed_tests + failed_tests + skipped_tests + errored_tests
        parsed_warnings = warn_report_to_payload(warned_tests, all_reports)
        render_test_summary(terminalreporter, parsed_warnings, yellow=True)

    terminalreporter.section(
        f"failures: {len(failed_tests)}", sep="-", blue=True, bold=True
    )
    if failed_tests:
        render_test_summary(terminalreporter, failed_tests, red=True)

    terminalreporter.section(
        f"errors: {len(errored_tests)}", sep="-", blue=True, bold=True
    )
    if errored_tests:
        render_test_summary(terminalreporter, errored_tests, red=True)


@pytest.fixture(autouse=True)
def auto_resource(record_property: typing.Callable[[str, typing.Any], None]):
    return record_property
