from amlaidatatests.schema.v1.common import CurrencyValue
import ibis
from ibis.expr.datatypes import String, Timestamp, Boolean, Struct


transaction_schema = ibis.Schema(
    {
        "transaction_id": String(nullable=False),
        "validity_start_time": Timestamp(nullable=False),
        "is_entity_deleted": Boolean(),
        "source_system": String(),
        "type": String(nullable=False),
        "direction": String(nullable=False),
        "account_id": String(),
        "counterparty_account": Struct(fields={"account_id": String(nullable=True)}),
        "book_time": Timestamp(),
        "normalized_booked_amount": CurrencyValue(),
    }
)
