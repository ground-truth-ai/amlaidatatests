"""Configuration file specifying the tables and configuration for this schema
version"""

from amlaidatatests.schema.base import BaseSchemaConfiguration, TableConfig, TableType

from . import (
    account_party_link_schema,
    party_schema,
    party_supplementary_data_schema,
    risk_case_event_schema,
    transaction_schema,
)


class SchemaConfiguration(BaseSchemaConfiguration):
    TABLES = [
        TableConfig(name="party", schema=party_schema, entity_keys=["party_id"]),
        TableConfig(
            name="transaction",
            schema=transaction_schema,
            table_type=TableType.OPEN_ENDED_ENTITY,
            entity_keys=["transaction_id"],
        ),
        TableConfig(
            name="account_party_link",
            schema=account_party_link_schema,
            entity_keys=["account_id", "party_id"],
        ),
        TableConfig(
            name="risk_case_event",
            schema=risk_case_event_schema,
            table_type=TableType.EVENT,
            entity_keys=["risk_case_event_id"],
        ),
        TableConfig(
            name="party_supplementary_data",
            schema=party_supplementary_data_schema,
            optional=True,
            entity_keys=["party_id", "party_supplementary_data_id"],
        ),
    ]
