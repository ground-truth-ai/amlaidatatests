=========================
Interpreting Schema Tests
=========================

amlaidatatests uses `ibis <https://ibis-project.org/>`_ internally to model
tables. This allows it to connect to a variety of different backends to suit
various customer deployment strategies.

This page documents the most important syntax from an ibis syntax which arise from
ibis and are used.

Nullable/non-nullable fields
============================

If a field is pre-pended by ``!``, it is non-nullable. amlaidatatests checks column
nullability seperately from the contents of the column itself. This means that the
backend/database itself verifies that the field's content is not-null.

Example
-------

``check column is the correct type on table: Column type mismatch: expected !string, found string``

In this example, the column is a nullable field (``string``), whilst the field itself is specified
as a non-nullable field (``!string``) in the AML AI schema.

Embedded fields
===============

The bigquery schema makes use of ``struct`` and ``array`` types. These types
have embedded underlying fields.

Struct
------

A struct can contain multiple fields embedded in it.

For example:
``struct<units: !int64, nanos: !int64, currency_code: !string>``

is a ``struct`` field containing multiple fields. Note that all these fields are
nullable, whilst the parent field is not.

amlaidatatests handles this by only validating the schema and fields if the parent
struct is present. If it is not, the field is not validated.
