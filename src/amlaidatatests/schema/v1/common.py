"""Entities for AML AI schema"""

from ibis.expr.datatypes import Int64, String, Struct


def CurrencyValue(nullable=True):
    return Struct(
        nullable=nullable,
        fields={
            "units": Int64(nullable=False),
            "nanos": Int64(nullable=False),
            "currency_code": String(nullable=False),
        },
    )


def ValueEntity(nullable=True):
    return Struct(
        nullable=nullable,
        fields={
            "units": Int64(nullable=False),
            "nanos": Int64(nullable=False),
        },
    )
