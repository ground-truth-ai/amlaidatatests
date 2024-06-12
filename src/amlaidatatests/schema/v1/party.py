from amlaidatatests.schema.v1.common import CurrencyValue
import ibis
from ibis.expr.datatypes import String, Int32, Timestamp, Boolean, Date, Struct, Array

#            party_id                  VARCHAR(255),
#            validity_start_time     Int,
#            is_entity_deleted       Bool,
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
    {
        "party_id": String(nullable=False),
        "validity_start_time": Timestamp(nullable=False),
        "is_entity_deleted": Boolean(nullable=True),
        "source_system": String(nullable=True),
        "type": String(nullable=False),
        "birth_date": Date(nullable=True),
        "establishment_date": Date(nullable=True),
        "occupation": String(nullable=True),
        "gender": String(nullable=True),
        "nationalities": Array(
            nullable=True, value_type=Struct(fields={"region_code": String()})
        ),
        "residencies": Array(
            nullable=True, value_type=Struct(fields={"region_code": String()})
        ),
        "exit_date": Date(nullable=True),
        "join_date": Date(nullable=True),
        "assets_value_range": Struct(
            nullable=True,
            fields={"start_amount": CurrencyValue(), "end_amount": CurrencyValue()},
        ),
        "civil_status_code": String(nullable=True),
        "education_level_code": String(nullable=True),
    }
)
