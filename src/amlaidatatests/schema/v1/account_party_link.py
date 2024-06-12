from ibis import schema
from ibis.expr.datatypes.core import DataType

#            account_id              VARCHAR(255),
#            party_id                VARCHAR(255),
#            validity_start_time     TIMESTAMP,
#            is_entity_deleted       BOOL,
#            role                    VARCHAR(255),
#            source_system           VARCHAR(255)

ACCOUNT_PARTY_LINK = schema([("account_id", "string"),
                     ("party_id", "string"),
                     ("validity_start_time", "timestamp"),
                     ("is_entity_deleted", "boolean"),
                     ("role", "string"),
                     ("source_system", "string")
                     ])
