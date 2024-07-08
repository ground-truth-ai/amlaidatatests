import datetime
from amlaidatatests.base import FailTest
from amlaidatatests.schema.base import ResolvedTableConfig
import ibis
import pytest
from ibis.expr.datatypes import String, Timestamp, Boolean
from amlaidatatests.tests import common


def test_missing_key_local_table(test_connection, create_test_table):
    schema = {"id": String(nullable=False)}
    # Test behaviour if the other table has more than one key which isn't on
    # the table for testing. This shouldn't be a problem - there may well be
    # keys not on this table which are present on the base table
    lcl_tbl = create_test_table(ibis.memtable(data=[{"id": "1"}], schema={"id": str}))
    otr_tbl = create_test_table(
        ibis.memtable(data=[{"id": "1"}, {"id": "2"}], schema={"id": str})
    )

    table_config = ResolvedTableConfig(table=ibis.table(name=lcl_tbl, schema=schema))
    otr_table_config = ResolvedTableConfig(
        table=ibis.table(name=otr_tbl, schema=schema)
    )

    t = common.TestReferentialIntegrity(
        table_config=table_config, to_table_config=otr_table_config, keys=["id"]
    )
    # Should pass - RI works one way only
    t(test_connection)


def test_missing_key_other_table(test_connection, create_test_table):

    schema = {"id": String(nullable=False)}

    lcl_tbl = create_test_table(
        ibis.memtable(data=[{"id": "1"}, {"id": "2"}], schema=schema)
    )
    otr_tbl = create_test_table(ibis.memtable(data=[{"id": "1"}], schema=schema))

    table_config = ResolvedTableConfig(table=ibis.table(name=lcl_tbl, schema=schema))
    otr_table_config = ResolvedTableConfig(
        table=ibis.table(name=otr_tbl, schema=schema)
    )

    # Should fail - key doesn't exist
    with pytest.raises(FailTest, match=r"1 keys found in table"):
        t = common.TestReferentialIntegrity(
            table_config=table_config, to_table_config=otr_table_config, keys=["id"]
        )
        t(test_connection)


def test_passes_multiple_keys(test_connection, create_test_table):
    schema = {"id1": String(nullable=False), "id2": String(nullable=False)}

    lcl_tbl = create_test_table(
        ibis.memtable(
            data=[{"id1": "1", "id2": "2"}, {"id1": "2", "id2": "3"}], schema=schema
        )
    )
    otr_tbl = create_test_table(
        ibis.memtable(
            data=[{"id1": "1", "id2": "2"}, {"id1": "2", "id2": "3"}], schema=schema
        )
    )

    table_config = ResolvedTableConfig(table=ibis.table(name=lcl_tbl, schema=schema))
    otr_table_config = ResolvedTableConfig(
        table=ibis.table(name=otr_tbl, schema=schema)
    )
    t = common.TestReferentialIntegrity(
        table_config=table_config, to_table_config=otr_table_config, keys=["id1", "id2"]
    )
    t(test_connection)


def test_passes_duplicated_keys(test_connection, create_test_table):
    schema = {"id1": String(nullable=False), "id2": String(nullable=False)}

    lcl_tbl = create_test_table(
        ibis.memtable(
            data=[{"id1": "1", "id2": "2"}, {"id1": "1", "id2": "2"}], schema=schema
        )
    )
    otr_tbl = create_test_table(
        ibis.memtable(
            data=[{"id1": "1", "id2": "2"}, {"id1": "1", "id2": "2"}], schema=schema
        )
    )

    table_config = ResolvedTableConfig(table=ibis.table(name=lcl_tbl, schema=schema))
    otr_table_config = ResolvedTableConfig(
        table=ibis.table(name=otr_tbl, schema=schema)
    )

    t = common.TestReferentialIntegrity(
        table_config=table_config, to_table_config=otr_table_config, keys=["id1", "id2"]
    )
    t(test_connection)


def test_missing_multiple_keys_other_table(test_connection, create_test_table):
    schema = {"id1": String(nullable=False), "id2": String(nullable=False)}

    lcl_tbl = create_test_table(
        ibis.memtable(data=[{"id1": "1", "id2": "2"}], schema=schema)
    )
    otr_tbl = create_test_table(
        ibis.memtable(data=[{"id1": "2", "id2": "3"}], schema=schema)
    )

    table_config = ResolvedTableConfig(table=ibis.table(name=lcl_tbl, schema=schema))
    otr_table_config = ResolvedTableConfig(
        table=ibis.table(name=otr_tbl, schema=schema)
    )
    # Should fail - key doesn't exist
    with pytest.raises(FailTest, match=r"1 keys found in table"):
        t = common.TestReferentialIntegrity(
            table_config=table_config,
            to_table_config=otr_table_config,
            keys=["id1", "id2"],
        )
        t(test_connection)


def test_temporal_referential_integrity_missing_keys(
    test_connection, create_test_table
):
    schema = {
        "id": str,
        "validity_start_time": Timestamp(timezone="UTC"),
        "is_entity_deleted": Boolean(),
    }
    lcl_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                }
            ],
            schema=schema,
        )
    )
    otr_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "1",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
                {
                    "id": "2",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
            ],
            schema=schema,
        )
    )

    table_config = ResolvedTableConfig(table=ibis.table(name=lcl_tbl, schema=schema))
    otr_table_config = ResolvedTableConfig(
        table=ibis.table(name=otr_tbl, schema=schema)
    )

    t = common.TestTemporalReferentialIntegrity(
        table_config=table_config, to_table_config=otr_table_config, key="id"
    )
    with pytest.raises(FailTest, match=r"1 keys found in table"):
        t(test_connection)


def test_temporal_referential_integrity_key_in_time(test_connection, create_test_table):
    schema = {
        "id": str,
        "validity_start_time": Timestamp(timezone="UTC"),
        "is_entity_deleted": Boolean(),
    }
    lcl_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 2, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": True,
                },
            ],
            schema=schema,
        )
    )
    otr_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 2, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": True,
                },
            ],
            schema=schema,
        )
    )

    table_config = ResolvedTableConfig(table=ibis.table(name=lcl_tbl, schema=schema))
    otr_table_config = ResolvedTableConfig(
        table=ibis.table(name=otr_tbl, schema=schema)
    )

    t = common.TestTemporalReferentialIntegrity(
        table_config=table_config, to_table_config=otr_table_config, key="id"
    )
    t(test_connection)


def test_temporal_referential_integrity_key_out_of_time(
    test_connection, create_test_table
):
    schema = {
        "id": str,
        "validity_start_time": Timestamp(timezone="UTC"),
        "is_entity_deleted": Boolean(),
    }
    lcl_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 2, hour=1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": True,
                },
            ],
            schema=schema,
        )
    )
    otr_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 2, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": True,
                },
            ],
            schema=schema,
        )
    )

    table_config = ResolvedTableConfig(table=ibis.table(name=lcl_tbl, schema=schema))
    otr_table_config = ResolvedTableConfig(
        table=ibis.table(name=otr_tbl, schema=schema)
    )

    t = common.TestTemporalReferentialIntegrity(
        table_config=table_config, to_table_config=otr_table_config, key="id"
    )
    with pytest.raises(FailTest, match=r"1 keys found in table"):
        t(test_connection)


def test_temporal_referential_integrity_key_within_time_tolerance(
    test_connection, create_test_table
):
    schema = {
        "id": str,
        "validity_start_time": Timestamp(timezone="UTC"),
        "is_entity_deleted": Boolean(),
    }
    lcl_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 2, hour=1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": True,
                },
            ],
            schema=schema,
        )
    )
    otr_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 2, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": True,
                },
            ],
            schema=schema,
        )
    )

    table_config = ResolvedTableConfig(table=ibis.table(name=lcl_tbl, schema=schema))
    otr_table_config = ResolvedTableConfig(
        table=ibis.table(name=otr_tbl, schema=schema)
    )

    t = common.TestTemporalReferentialIntegrity(
        table_config=table_config,
        to_table_config=otr_table_config,
        key="id",
        tolerance="year",
    )
    t(test_connection)


def test_temporal_referential_integrity_key_out_of_time_tolerance(
    test_connection, create_test_table
):
    schema = {
        "id": str,
        "validity_start_time": Timestamp(timezone="UTC"),
        "is_entity_deleted": Boolean(),
    }
    lcl_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2022, 1, 1, hour=1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": True,
                },
            ],
            schema=schema,
        )
    )
    otr_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 2, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": True,
                },
            ],
            schema=schema,
        )
    )

    table_config = ResolvedTableConfig(table=ibis.table(name=lcl_tbl, schema=schema))
    otr_table_config = ResolvedTableConfig(
        table=ibis.table(name=otr_tbl, schema=schema)
    )

    t = common.TestTemporalReferentialIntegrity(
        table_config=table_config,
        to_table_config=otr_table_config,
        key="id",
        tolerance="year",
    )
    with pytest.raises(FailTest, match=r"1 keys found in table"):
        t(test_connection)


def test_temporal_referential_integrity_fails_before_period(
    test_connection, create_test_table
):
    schema = {
        "id": str,
        "validity_start_time": Timestamp(timezone="UTC"),
        "is_entity_deleted": Boolean(),
    }
    lcl_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2019, 1, 1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, hour=1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": True,
                },
            ],
            schema=schema,
        )
    )
    otr_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 2, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": True,
                },
            ],
            schema=schema,
        )
    )

    table_config = ResolvedTableConfig(table=ibis.table(name=lcl_tbl, schema=schema))
    otr_table_config = ResolvedTableConfig(
        table=ibis.table(name=otr_tbl, schema=schema)
    )

    t = common.TestTemporalReferentialIntegrity(
        table_config=table_config, to_table_config=otr_table_config, key="id"
    )
    with pytest.raises(FailTest, match=r"1 keys found in table"):
        t(test_connection)


def test_temporal_referential_integrity_fails_after_period(
    test_connection, create_test_table
):
    schema = {
        "id": str,
        "validity_start_time": Timestamp(timezone="UTC"),
        "is_entity_deleted": Boolean(),
    }
    lcl_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2022, 1, 1, hour=1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": True,
                },
            ],
            schema=schema,
        )
    )
    otr_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 2, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": True,
                },
            ],
            schema=schema,
        )
    )

    table_config = ResolvedTableConfig(table=ibis.table(name=lcl_tbl, schema=schema))
    otr_table_config = ResolvedTableConfig(
        table=ibis.table(name=otr_tbl, schema=schema)
    )

    t = common.TestTemporalReferentialIntegrity(
        table_config=table_config, to_table_config=otr_table_config, key="id"
    )
    with pytest.raises(FailTest, match=r"1 keys found in table"):
        t(test_connection)


def test_temporal_referential_integrity_fails_encompassing_period(
    test_connection, create_test_table
):
    schema = {
        "id": str,
        "validity_start_time": Timestamp(timezone="UTC"),
        "is_entity_deleted": Boolean(),
    }
    lcl_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2019, 1, 1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2022, 1, 1, hour=1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": True,
                },
            ],
            schema=schema,
        )
    )
    otr_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 2, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": True,
                },
            ],
            schema=schema,
        )
    )

    table_config = ResolvedTableConfig(table=ibis.table(name=lcl_tbl, schema=schema))
    otr_table_config = ResolvedTableConfig(
        table=ibis.table(name=otr_tbl, schema=schema)
    )

    t = common.TestTemporalReferentialIntegrity(
        table_config=table_config, to_table_config=otr_table_config, key="id"
    )
    with pytest.raises(FailTest, match=r"1 keys found in table"):
        t(test_connection)


def test_temporal_referential_integrity_immediate_delete_in_period(
    test_connection, create_test_table
):
    schema = {
        "id": str,
        "validity_start_time": Timestamp(timezone="UTC"),
        "is_entity_deleted": Boolean(),
    }
    # We assume the entity is created and deleted instantaneously. Other
    # tests should pick this up
    lcl_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, hour=1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": True,
                }
            ],
            schema=schema,
        )
    )
    otr_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 2, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": True,
                },
            ],
            schema=schema,
        )
    )

    table_config = ResolvedTableConfig(table=ibis.table(name=lcl_tbl, schema=schema))
    otr_table_config = ResolvedTableConfig(
        table=ibis.table(name=otr_tbl, schema=schema)
    )

    t = common.TestTemporalReferentialIntegrity(
        table_config=table_config, to_table_config=otr_table_config, key="id"
    )
    t(test_connection)


def test_temporal_referential_integrity_multiple_mutations_in_period(
    test_connection, create_test_table
):
    schema = {
        "id": str,
        "validity_start_time": Timestamp(timezone="UTC"),
        "is_entity_deleted": Boolean(),
    }
    lcl_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, hour=1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, hour=2, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": True,
                },
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, hour=3, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
            ],
            schema=schema,
        )
    )
    otr_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 2, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": True,
                },
            ],
            schema=schema,
        )
    )

    table_config = ResolvedTableConfig(table=ibis.table(name=lcl_tbl, schema=schema))
    otr_table_config = ResolvedTableConfig(
        table=ibis.table(name=otr_tbl, schema=schema)
    )

    t = common.TestTemporalReferentialIntegrity(
        table_config=table_config, to_table_config=otr_table_config, key="id"
    )
    t(test_connection)


def test_temporal_referential_integrity_multiple_mutations_out_of_period(
    test_connection, create_test_table
):
    schema = {
        "id": str,
        "validity_start_time": Timestamp(timezone="UTC"),
        "is_entity_deleted": Boolean(),
    }
    lcl_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, hour=1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, hour=2, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": True,
                },
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 3, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
            ],
            schema=schema,
        )
    )
    otr_tbl = create_test_table(
        ibis.memtable(
            data=[
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 1, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": False,
                },
                {
                    "id": "0",
                    "validity_start_time": datetime.datetime(
                        2020, 1, 2, tzinfo=datetime.UTC
                    ),
                    "is_entity_deleted": True,
                },
            ],
            schema=schema,
        )
    )

    table_config = ResolvedTableConfig(table=ibis.table(name=lcl_tbl, schema=schema))
    otr_table_config = ResolvedTableConfig(
        table=ibis.table(name=otr_tbl, schema=schema)
    )

    t = common.TestTemporalReferentialIntegrity(
        table_config=table_config, to_table_config=otr_table_config, key="id"
    )
    with pytest.raises(FailTest, match=r"1 keys found in table"):
        t(test_connection)