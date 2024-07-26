"""AML AI schema version 1. Full Google documentation is at:
https://cloud.google.com/financial-services/anti-money-laundering/docs/reference/rest
"""

from .account_party_link import account_party_link_schema
from .party import party_schema
from .party_supplementary_data import party_supplementary_data_schema
from .risk_case_event import risk_case_event_schema
from .transaction import transaction_schema
