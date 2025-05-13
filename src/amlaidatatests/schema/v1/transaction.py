"""Configuration file specifying the schema for the transaction table"""

import ibis
from ibis.expr.datatypes import Boolean, Int64, String, Struct, Timestamp

from amlaidatatests.schema.v1.common import CurrencyValue

transaction_schema = ibis.Schema(
    fields={
        "transaction_id": String(nullable=False),
        "validity_start_time": Timestamp(nullable=False, timezone="UTC"),
        "is_entity_deleted": Boolean(),
        "source_system": String(),
        "type": String(nullable=False),
        "direction": String(nullable=False),
        "account_id": String(),
        "counterparty_account": Struct(
            fields={
                "account_id": String(nullable=True),
                "region_code": String(nullable=True),
            }
        ),
        "book_time": Timestamp(timezone="UTC"),
        "normalized_booked_amount": CurrencyValue(),
    }
)
