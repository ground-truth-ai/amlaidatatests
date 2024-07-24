========
Bigquery
========

`Bigquery <https://cloud.google.com/bigquery/docs/introduction>`_ is currently the only supported backend for amlaidatatests.

Connection String
=================

``bigquery://{project_id}/{dataset_id}?location``

:project: The Google cloud *billing* project
:dataset: The bigquery dataset id in which queries are executed
:location: The `bigquery location <https://cloud.google.com/bigquery/docs/locations#regions>`_ of the dataset


Authentication
==============

amlaidatatests uses `Google Application Default Credentials
<https://cloud.google.com/docs/authentication/provide-credentials-adc>`_ to
authenticate to GCP.

This means that the environment is inspected to locate your credentials and
these credentials are used. There are a range of options depending on the
environment you are using, and you should read more about these options `in the
google cloud documentation
<https://cloud.google.com/docs/authentication/provide-credentials-adc>`_. Fully
documenting the options to authenticate using Application Default Credentials is
beyond the scope of this documentation.

Authenticate for local development
----------------------------------

The simplest usage for interactive access is to:

#. `Install the gcloud SDK <https://cloud.google.com/sdk/docs/install>`_
   and interactively obtain user credentials.
#. Interactively authenticate:

   .. code-block:: console

      $ gcloud auth login
