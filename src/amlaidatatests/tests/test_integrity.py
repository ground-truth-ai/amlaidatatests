import pytest


@pytest.mark.parametrize("tables,keys", [(("party", "account_party_link"), ("party_id"))], )
def test_referential_integrity(tables: list[str], keys: list[str]):
    print(tables)
    print(keys)
    #test(table=table)
