"""Configuration file specifying the schema for the party table"""

import ibis
from ibis.expr.datatypes import Array, Boolean, Date, String, Struct, Timestamp

from amlaidatatests.schema.v1.common import CurrencyValue

#    party_id                VARCHAR(255),
#    validity_start_time     Int,
#    is_entity_deleted       Bool,
#    source_system           VARCHAR(255),
#    type                    VARCHAR(255),
#    birth_date              DATE,
#    establishment_date      DATE,
#    occupation              VARCHAR(255),
#    gender                  VARCHAR(255),
#    nationalities           region,
#    residencies             region,
#    exit_date               DATE,
#    join_date               DATE,
#    assets_value_range      assets_range,
#    civil_status_code       VARCHAR(255),
#    education_level_code    VARCHAR(255)

party_schema = ibis.Schema(
    fields={
        "party_id": String(nullable=False),
        "validity_start_time": Timestamp(nullable=False),
        "is_entity_deleted": Boolean(),
        "source_system": String(),
        "type": String(nullable=False),
        "birth_date": Date(),
        "establishment_date": Date(),
        "occupation": String(),
        "gender": String(),
        "nationalities": Array(
            value_type=Struct(fields={"region_code": String(nullable=False)})
        ),
        "residencies": Array(
            value_type=Struct(fields={"region_code": String(nullable=False)})
        ),
        "exit_date": Date(),
        "join_date": Date(),
        "assets_value_range": Struct(
            fields={"start_amount": CurrencyValue(), "end_amount": CurrencyValue()},
        ),
        "civil_status_code": String(),
        "education_level_code": String(),
    }
)
