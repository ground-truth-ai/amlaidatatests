from amlaidatatests.schema.v1.common import ValueEntity
import ibis
from ibis.expr.datatypes import String, Timestamp, Boolean, Date


transaction_schema = ibis.Schema(
    {
        "transaction_id": String(nullable=False),
        "validity_start_time": Timestamp(nullable=False),
        "is_entity_deleted": Boolean(nullable=True),
        "source_system": String(nullable=True),
        "type": String(nullable=False),
        "direction": String(nullable=True),
        "account_id": Date(nullable=True),
        "counterparty_account": String(nullable=True),
        "book_time": Timestamp(),
        "normalized_booked_amount": ValueEntity()
    }
)