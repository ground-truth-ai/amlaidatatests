"""Configuration file specifying the schema for the account_party_link
table"""

import ibis
from ibis import schema
from ibis.expr.datatypes import Boolean, String, Timestamp

#            account_id              VARCHAR(255),
#            party_id                VARCHAR(255),
#            validity_start_time     TIMESTAMP,
#            is_entity_deleted       BOOL,
#            role                    VARCHAR(255),
#            source_system           VARCHAR(255)

ACCOUNT_PARTY_LINK = schema(
    [
        ("account_id", "string"),
        ("party_id", "string"),
        ("validity_start_time", "timestamp"),
        ("is_entity_deleted", "boolean"),
        ("role", "string"),
        ("source_system", "string"),
    ]
)

account_party_link_schema = ibis.Schema(
    fields={
        "account_id": String(nullable=False),
        "party_id": String(nullable=False),
        "validity_start_time": Timestamp(nullable=False),
        "is_entity_deleted": Boolean(),
        "role": String(),
        "source_system": String(),
    }
)
