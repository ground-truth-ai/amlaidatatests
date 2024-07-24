"""Main entry point for amlaidatatests."""

import os
import sys
from contextlib import contextmanager

import pytest

CURRENT_LOCATION = os.path.dirname(__file__)


@contextmanager
def cwd(path):
    oldpwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(oldpwd)


def run_tests(args):
    # Pass the entire set of arguments through to pytest
    args = [f"{CURRENT_LOCATION}", *args]
    with cwd(CURRENT_LOCATION):
        errcode = pytest.main(args=args)
    sys.exit(errcode)


if __name__ == "__main__":
    # An adaptor is used because the pytest cli can't handle directly
    # passing the fully-qualified module AND the addition of command line
    # options
    run_tests(sys.argv[1:])
