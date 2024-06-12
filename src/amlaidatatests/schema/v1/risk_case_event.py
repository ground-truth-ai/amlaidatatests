from ibis import schema

#         """risk_case_event_id        VARCHAR(255),
#         event_time              Int,
#         type                    VARCHAR(255),
#         party_id                VARCHAR(255),
#         risk_case_id            VARCHAR(255)

RISK_CASE_EVENT = schema([("risk_case_event_id", "string"),
                     ("event_time", "timestamp"),
                     ("type", "string"),
                     ("party_id", "string"),
                     ("risk_case_id", "string")])