import ibis
from amlaidatatests.schema.v1 import *

SCHEMAS = {
    "party": party_schema,
    "transaction": transaction_schema,
    "account_party_link": account_party_link_schema,
    "risk_case_event": risk_case_event_schema,
    "party_supplementary_data": party_supplementary_data_schema
}

def get_table_name(name: str):
    SUFFIX = "1234"
    return f"{name}_{SUFFIX}"


def get_table(name: str):
    s = SCHEMAS[name]
    return ibis.table(schema=s, name=get_table_name(name))