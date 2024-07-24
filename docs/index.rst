==============================
AML AI Datatests documentation
==============================


Introduction
============

This utility tests data for consistency and data integrity for the
`Google Anti Money Laundering AI API <https://cloud.google.com/financial-services/anti-money-laundering/docs>`_.

The AML AI has an `input data model
<https://cloud.google.com/financial-services/anti-money-laundering/docs/reference/schemas/aml-input-data-model>`_.
This toolset is intended to test for a range of data integrity issues which can
occur during both training and testing scenarios in both production and development.

It aims to:
 * Validate the schema in which the data is being stored.
 * Validate the data stored in the schema.

amlaidatatests is based on two main libraries:
 * `Ibis Project <https://ibis-project.org/>`_ - which provides the ability to
   test the data in multiple databases. Only bigquery is supported at this time.
 * `pytest <https://docs.pytest.org/en/latest/contents.html/>`_ - which allows
   the use of multiple common python testing plugins.


Get Started
===========

These sections cover the basics of using and configuring amlaidatatests

.. toctree::
   :maxdepth: 1

   usage/installing
   usage/quickstart

Reference
=========

.. toctree::
   :maxdepth: 2

   configuration
   databases/index
   tests/index


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _CNN: http://cnn.com/
