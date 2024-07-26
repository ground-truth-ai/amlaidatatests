"""Exceptions for amlaidatatests"""

import enum
import importlib
from enum import auto
from typing import Optional

import ibis
import pandas as pd


def get_test_failure_descriptions(test_id: str):
    template_res = importlib.resources.files("amlaidatatests.resources").joinpath(
        "test_descriptions_en_us.csv"
    )
    with importlib.resources.as_file(template_res) as template_file:
        # TODO: Implement a proper description loader
        df = pd.read_csv(template_file, na_values=[], keep_default_na=False)
        value = df[df["id"] == test_id]
        if len(value.index) > 0:
            return value.iloc[0]["description"]
        return None


class AMLAITestSeverity(enum.Enum):
    """Severity level configuration for tests"""

    ERROR = auto()
    """ Test should error if the test fails """
    WARN = auto()
    """ Test should issue a warning if the test fails """
    INFO = auto()
    """ Test should issue print info if the test fails """


class DataTestFailure(Exception):
    """amlaidatatests specific exception denoting a test failure. Could result
    in a failure if the test which raises it is configured to warn only"""

    def __init__(
        self,
        message: str,
        expr: Optional[ibis.Expr] = None,
        test_id: Optional[str] = None,
    ) -> None:
        self.message = message
        self.expr = expr
        self.test_id = test_id
        self.sql = str(ibis.get_backend().compile(expr)) if expr is not None else None

    @property
    def test_id(self):
        return self._test_id

    @test_id.setter
    def test_id(self, value):
        self._test_id = value
        self.description = get_test_failure_descriptions(value)

    def friendly_message(self) -> str:
        """Print a friendly message explaining why the test failed.

        Optionally uses provided ibis expression to compile into sql which the
        user should be able to paste directly into their console

        Returns:
            Message for presentation to the user
        """
        description_message = f"{self.description}:" if self.description else ""
        msg = f"""{description_message} {self.message or ""}"""
        if self.sql:
            msg += "\nTo reproduce this result, run:\n"
            msg += self.sql
        return msg.strip()

    def __str__(self):
        return self.friendly_message()


class DataTestWarning(Warning, DataTestFailure):
    """amlaidatatests specific exception denoting a test warning. Will never
    result in an test outright failing unless pytest is configured to fail on
    warnings"""

    def __init__(self, message: str, expr: Optional[ibis.Expr] = None) -> None:
        DataTestFailure.__init__(self, message=message, expr=expr)

    def __str__(self):
        return self.friendly_message()


class SkipTest(Exception):
    """An AML AI exception representing a decision to skip the test.

    We use a custom exception so we are able to capture testskips in
    unit tests

    Args:
        message: A message for the user as to why the test was skipped
    """

    def __init__(self, message: str) -> None:
        self.message = message
