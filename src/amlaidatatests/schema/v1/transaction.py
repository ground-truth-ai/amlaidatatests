from amlaidatatests.tests.fixtures.common import UniqueCombinationOfColumns
from ibis import schema

#            """transaction_id           VARCHAR(255),0
#            validity_start_time     TIMESTAMP,
#            is_entity_deleted       BOOL,
#            source_system           VARCHAR(255),
#            type                    VARCHAR(255),
#            direction               VARCHAR(255),
#            account_id              VARCHAR(255),
#            counterparty_account    VARCHAR(255),
#            book_time               INTEGER,
#            normalized_booked_amount amount

TRANSACTION = schema([("transaction_id", "string"),
                     ("validity_start_time", "timestamp"),
                     ("is_entity_deleted", "boolean"),
                     ("source_system", "string"),
                     ("type", "string"),
                     ("direction", "string"),
                     ("account_id", "string"),
                     ("counterparty_account", "string"),
                     ("book_time", "timestamp"),
                     ("normalized_booked_amount", "struct<nanos: int, units: int>")]
                     )