import importlib.resources

import pandas as pd


def get_valid_region_codes() -> list:
    """Get a list of valid ISO 3166-2 currency codes

    The current list of currency codes was obtained from ...

    Returns:
        A list of valid currency codes
    """

    template_res = importlib.resources.files("amlaidatatests.resources").joinpath(
        "country_codes.csv"
    )
    with importlib.resources.as_file(template_res) as template_file:
        return pd.read_csv(template_file, na_values=[], keep_default_na=False)[
            "code"
        ].to_list()


def get_valid_currency_codes() -> list:
    """Get a list of valid 3-character ISO 4217 currency codes

    The current list of currency codes was obtained from ...

    Returns:
        A list of valid currency codes
    """
    template_res = importlib.resources.files("amlaidatatests.resources").joinpath(
        "currency_codes.csv"
    )
    with importlib.resources.as_file(template_res) as template_file:
        return pd.read_csv(template_file, na_values=[], keep_default_na=False)[
            "code"
        ].to_list()
