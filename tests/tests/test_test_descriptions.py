import numpy as np
import pandas as pd
from ibis import BaseBackend
from pytest import FixtureRequest

from amlaidatatests.exceptions import read_test_description_file


def test_test_descriptions():
    df = read_test_description_file()
    ii = df.set_index("id")
    print(ii[ii.index.duplicated()])
    assert ii.index.duplicated().any() is np.False_
