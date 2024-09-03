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

.. toctree::
   :caption: Get Started
   :titlesonly:
   :maxdepth: 2

   get-started/installing/index
   get-started/quickstart/index

.. toctree::
   :caption: User Guides
   :titlesonly:
   :maxdepth: 2

   usage/interpreting-schematests
   usage/data-sources
   usage/troubleshooting

.. toctree::
   :caption: Reference
   :titlesonly:
   :maxdepth: 1

   databases/index
   reference/test-list
   reference/configuration
   about/index
