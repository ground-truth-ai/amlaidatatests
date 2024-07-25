import ibis
import pytest
from ibis.expr.datatypes import String

from amlaidatatests.exceptions import SkipTest
from amlaidatatests.schema.base import ResolvedTableConfig
from amlaidatatests.tests import common


def test_skips_table_test_missing_optional_table(
    test_connection, test_raise_on_skip, request
):
    table = ibis.table(
        name="DOES_NOT_EXIST",
        schema={"a": String(nullable=False), "b": String(nullable=False)},
    )

    table_config = ResolvedTableConfig(
        name=table.get_name(), table=table, optional=True
    )

    t = common.AbstractTableTest(table_config=table_config)
    with pytest.raises(SkipTest, match=r"Skipping test: optional table"):
        t(test_connection, request)


def test_no_skip_mandatory_table(test_connection, test_raise_on_skip, request):
    table = ibis.table(
        name="DOES_NOT_EXIST",
        schema={"a": String(nullable=False), "b": String(nullable=False)},
    )

    table_config = ResolvedTableConfig(name=table.get_name(), table=table)

    t = common.AbstractTableTest(table_config=table_config)
    with pytest.raises(ValueError, match=r"Required table"):
        t(test_connection, request)
