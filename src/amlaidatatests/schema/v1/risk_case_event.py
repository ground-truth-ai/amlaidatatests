"""Configuration file specifying the schema for the risk_case_event table"""

from ibis import schema
from ibis.expr.datatypes import String, Timestamp

#         risk_case_event_id      VARCHAR(255),
#         event_time              Int,
#         type                    VARCHAR(255),
#         party_id                VARCHAR(255),
#         risk_case_id            VARCHAR(255)

risk_case_event_schema = schema(
    {
        "risk_case_event_id": String(nullable=False),
        "event_time": Timestamp(nullable=False),
        "type": String(nullable=False),
        "party_id": String(nullable=False),
        "risk_case_id": String(nullable=False),
    }
)
