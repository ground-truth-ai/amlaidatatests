"""Configuration file specifying the schema for the party_supplementary_data
table"""

import ibis
from ibis.expr.datatypes import Boolean, Float64, String, Struct, Timestamp

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

party_supplementary_data_schema = ibis.Schema(
    fields={
        "party_supplementary_data_id": String(nullable=False),
        "validity_start_time": Timestamp(nullable=False),
        "is_entity_deleted": Boolean(),
        "source_system": String(),
        "party_id": String(nullable=False),
        "supplementary_data_payload": Struct(fields={"value": Float64(nullable=False)}),
    }
)
