

from amlaidatatests.base import FailTest
import ibis
import pytest
from ibis.expr.datatypes import String, Timestamp, Array, Struct
from amlaidatatests.tests import common
import datetime


def test_column_is_always_null(test_connection, create_test_table):
    schema = {"id": String(nullable=False)}

    tbl = create_test_table(ibis.memtable(data=[{'id': None}, {'id': None}], schema={"id": String()}))
    table = ibis.table(name=tbl, schema=schema)

    t = common.TestFieldNeverNull(table=table, column="id")

    with pytest.raises(expected_exception=FailTest, match=r'2 rows found with null values of id'):
        t(test_connection)

def test_column_optional_parent_always_present(test_connection, create_test_table):
    # Optional subfield should only be checked if the underlying field is missing
    schema = {"parent_id": Struct(nullable=True, fields={"id": String(nullable=False), "other": String()})}

    tbl = create_test_table(ibis.memtable(data=[{'parent_id': None}, {'parent_id': {'id': "hello"}}], schema=schema))
    table = ibis.table(name=tbl, schema=schema)

    t = common.TestFieldNeverNull(table=table, column="parent_id.id")

    t(test_connection)

def test_column_optional_parent_field_missing(test_connection, create_test_table):
    schema = {"parent_id": Struct(nullable=True, fields={"id": String(nullable=False)})}

    tbl = create_test_table(ibis.memtable(data=[{'parent_id': None}, {'parent_id': {'id': None, 'other': "hello"}}], schema=schema))
    table = ibis.table(name=tbl, schema=schema)

    t = common.TestFieldNeverNull(table=table, column="parent_id.id")

    with pytest.raises(expected_exception=FailTest, match=r'1 rows found with null values of parent_id.id'):
        t(test_connection)

def test_column_is_sometimes_null(test_connection, create_test_table):
    schema = {"id": String(nullable=False)}

    tbl = create_test_table(ibis.memtable(data=[{'id': None}, {'id': '12'}], schema={"id": String()}))
    table = ibis.table(name=tbl, schema=schema)

    t = common.TestFieldNeverNull(table=table, column="id")
    
    with pytest.raises(expected_exception=FailTest, match=r'1 rows found with null values of id'):
        t(test_connection)

def test_column_never_null(test_connection: ibis.BaseBackend, create_test_table):
    schema = {"id": String(nullable=False)}

    tbl = create_test_table(ibis.memtable(data=[{'id': '22'}, {'id': '12'}], schema=schema))
    table = ibis.table(name=tbl, schema=schema)

    t = common.TestFieldNeverNull(table=table, column="id")
    
    t(test_connection)

def test_string_column_blanks(test_connection: ibis.BaseBackend, create_test_table):
    tbl = create_test_table(ibis.memtable(data=[{'id': ''}, {'id': '   '}], schema={"id": str}))
    table = ibis.table(name=tbl, schema={"id": String(nullable=False)})

    t = common.TestFieldNeverWhitespaceOnly(table=table, column="id")
    
    with pytest.raises(expected_exception=FailTest, match=r'2 rows found with whitespace-only values of id'):
        t(test_connection)

def test_string_column_no_blanks(test_connection: ibis.BaseBackend, create_test_table):
    tbl = create_test_table(ibis.memtable(data=[{'id': '   a   '}, {'id': '  1234  '}], schema={"id": str}))

    table = ibis.table(name=tbl, schema={"id": String(nullable=False)})

    t = common.TestFieldNeverWhitespaceOnly(table=table, column="id")
    
    t(test_connection)


def test_date_column_1970(test_connection, create_test_table):
    tbl = create_test_table(ibis.memtable(data=[{'id': datetime.datetime(1970,1,1, tzinfo=datetime.UTC)}, 
                                                                   {'id': datetime.datetime(1970,1,1, tzinfo=datetime.UTC)}], 
                                                                   schema={"id": Timestamp(nullable=False, timezone="UTC")}))

    table = ibis.table(name=tbl, schema={"id": Timestamp(nullable=False, timezone="UTC")})
    t = common.TestDatetimeFieldNeverJan1970(table=table, column="id")
    
    with pytest.raises(expected_exception=FailTest, match=r'2 rows found with date on 1970-01-01'):
        t(test_connection)

def test_date_column_never_1970(test_connection, create_test_table):
    tbl = create_test_table(ibis.memtable(data=[{'id': datetime.datetime(1970,2,1, tzinfo=datetime.UTC)}, 
                                                                   {'id': datetime.datetime(1970,2,1, tzinfo=datetime.UTC)}], 
                                                                   schema={"id": Timestamp(nullable=False, timezone="UTC")}))

    table = ibis.table(name=tbl, schema={"id": Timestamp(nullable=False, timezone="UTC")})
    t = common.TestDatetimeFieldNeverJan1970(table=table, column="id")
    
    t(test_connection)