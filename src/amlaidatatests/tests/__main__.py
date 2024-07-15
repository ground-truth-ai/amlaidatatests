import os
import sys
import pytest


CURRENT_LOCATION = os.path.dirname(__file__)

if __name__ == "__main__":
    errcode = pytest.main([CURRENT_LOCATION] + sys.argv[1:])
    sys.exit(errcode)
