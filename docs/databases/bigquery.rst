========
Bigquery
========

Bigquery is the primary supported backend for amlaidatatests.

Connection String
=================

``bigquery://project/dataset``

:project: The Google cloud project
:dataset: Field content

Authentication
==============

amlaidatatests uses `Google Application Default Credentials<https://cloud.google.com/docs/authentication/provide-credentials-adc>`_

.. code-block:: bash

   gcloud auth login
