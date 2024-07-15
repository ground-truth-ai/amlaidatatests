from amlaidatatests.schema.base import BaseSchemaConfiguration, TableConfig

from . import (
    account_party_link_schema,
    party_schema,
    party_supplementary_data_schema,
    risk_case_event_schema,
    transaction_schema,
)


class SchemaConfiguration(BaseSchemaConfiguration):
    TABLES = [
        TableConfig(name="party", schema=party_schema),
        TableConfig(name="transaction", schema=transaction_schema),
        TableConfig(name="account_party_link", schema=account_party_link_schema),
        TableConfig(name="risk_case_event", schema=risk_case_event_schema),
        TableConfig(
            name="party_supplementary_data",
            schema=party_supplementary_data_schema,
            optional=True,
        ),
    ]
