==========
Quickstart
==========

This document provides the basic information you need to start using amlaidatatests.

It will run the full suite of tests against the a publically accessible AML AI sample dataset.

Steps
------------

#. Set-up and prepare your Google Project

    `Select or create a Cloud Platform project <https://console.cloud.google.com/project?_ga=2.113398791.1231111558.1721031991-1403473725.1708075965>`_

    `Enable billing for your project. <https://cloud.google.com/billing/docs/how-to/modify-project#enable_billing_for_a_project>`_

    `Enable the Google Cloud BigQuery API. <https://cloud.google.com/billing/docs/how-to/modify-project#enable_billing_for_a_project>`_

#. Install amlaidatatests using pip. See more options at :doc:`installing`

    .. code-block:: console

        $ pip install amlaidatatests

#. Install the gcloud SDK, following the instructions for your platform `here <https://cloud.google.com/sdk/docs/install>`_.

#. Authenticate to Google using the gcloud SDK

    .. code-block:: console

        $ gcloud auth login


#. Validate the official Google AML project sample data.

    .. important:: Location
        The Google sample dataset is located in the ``US`` location. The ``connection_string`` option must set this option to the US.

    .. code-block:: console
        :emphasize-text: MY-PROJECT

        $ python -m amlaidatatests.tests --connection_string=bigquery://MY-PROJECT?location=US --database=bigquery-public-data.aml_ai_input_dataset

#. Review the results

    .. code-block:: console

        src/amlaidatatests/tests/test_account_party_link.py ......................                                                                                             [ 11%]
        src/amlaidatatests/tests/test_party_supplementary_table.py .......................                                                                                     [ 24%]
        src/amlaidatatests/tests/test_party_table.py ......ssssss.......s.....sss.......s.....sss.........ssssssss.ss.....s..                                                  [ 62%]
        src/amlaidatatests/tests/test_risk_case_event_table.py ....................                                                                                            [ 73%]
        src/amlaidatatests/tests/test_transaction_table.py ..................................................                                                                  [100%]
