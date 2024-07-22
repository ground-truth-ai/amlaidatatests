"""Main entry point for amlaidatatests."""

import os
import sys

import pytest

CURRENT_LOCATION = os.path.dirname(__file__)

if __name__ == "__main__":
    # An adaptor is used because the pytest cli can't handle directly
    # passing the fully-qualified module AND the addition of command line
    # options
    errcode = pytest.main([CURRENT_LOCATION] + sys.argv[1:])
    sys.exit(errcode)
