==========
Quickstart
==========

This document provides the basic information you need to start using amlaidatatests.

It will run the full suite of tests against the a publically accessible AML AI sample dataset.

Steps
------------

.. important:: Location
    This guide assumes you already have access to the AML AI test data published by Google. If you do not, you should contact Google to discuss AML AI and get access.


#. Set-up and prepare your Google Project

   `Select or create a Cloud Platform project <https://console.cloud.google.com/project?_ga=2.113398791.1231111558.1721031991-1403473725.1708075965>`_

   `Enable billing for your project. <https://cloud.google.com/billing/docs/how-to/modify-project#enable_billing_for_a_project>`_

   `Enable the Google Cloud BigQuery API. <https://cloud.google.com/billing/docs/how-to/modify-project#enable_billing_for_a_project>`_

#. Install amlaidatatests using pip. See more options at :doc:`../installing/index`

   .. code-block:: console

       $ pip install amlaidatatests

#. Install the gcloud SDK, following the `instructions for your platform <https://cloud.google.com/sdk/docs/install>`_.

#. Authenticate to Google using the gcloud SDK

   .. code-block:: console

       $ gcloud auth login


#. Validate the official Google AML project sample data.

   .. important:: Location
       The Google sample dataset is located in the ``US`` location. The ``connection_string`` option must set this option to the US.

       In the command below, replace MY-PROJECT with your GCP Project name. Read more about the ``connection_string`` for bigquery
       `here <../databases/bigquery>`_.

   .. code-block:: console
       :emphasize-text: MY-PROJECT

       $ amlaidatatests --connection_string=bigquery://{MY-PROJECT}?location=US --database=bigquery-public-data.aml_ai_input_dataset


#. Review code execution. Whilst the tests are running, amlaidatatests prints outputs to demonstrate the test progress

   .. parsed-literal::

       test_account_party_link.py .............F........                                                      [ 11%]
       test_party_supplementary_table.py .......................                                              [ 24%]
       test_party_table.py ......ssssss.......s.....sss.......s.....sss.........ssssssss.ss.....s..   [ 62%]
       test_risk_case_event_table.py ....................                                                     [ 73%]
       test_transaction_table.py .....F............................................                           [100%]

   A :green:`green '.'` represents a passed test.

   A :red:`red 'F'` represents a failed test.

   A :orange:`yellow 's'` represents a skipped test. If optional columns are not present on the tables provided, tests on those columns are automatically skipped.

#. Review summary output. Once all tests are complete, summary information is printed to screen. Note that this output has been truncated for brevity.

   .. parsed-literal::

       ============================ amlaidatatests summary ===============================================================================================================================================
       ------------------------------ tests passed: 251 --------------------------------------------------------------------------------------------------------------------------------------------------
       --------------------------------- skipped: 28 -----------------------------------------------------------------------------------------------------------------------------------------------------
       F003  party.occupation                                    Skipped: Skipping running test on non-existent (but not required) column occupation
       F003  party.assets_value_range                            Skipped: Skipping running test on non-existent (but not required) column assets_value_range
       F003  party.civil_status_code                             Skipped: Skipping running test on non-existent (but not required) column civil_status_code
       F003  party.education_level_code                          Skipped: Skipping running test on non-existent (but not required) column education_level_code
       -------------------------------------------------------------------------------------------------------------------- warnings: 3 ---------------------------------------------------------------
       F004  party_supplementary_data.supplementary_data_payload Schema is stricter than required: expected struct<value: !float64> found !struct<value: !float64>
       F004  transaction.account_id                              Schema is stricter than required: expected string found !string
       P033  transaction.book_time                               17 months had a volume of less than 75% of the average volume for all months
       -------------------------------------------------------------------------------------------------------------------- failures: 3 ---------------------------------------------------------------
       P043  risk_case_event.type                                Check >=1 party with AML_SAR: 0 rows met criteria. Expected at least 1.
       P051  transaction.normalized_booked_amount.nanos          Large number of transactions have the same value across any transaction type: 89 column values appeared unusually frequently
       P032  transaction.book_time                               >= 1 month contains txns less than X the monthly average: 36 months had a volume of less than 90% of the average volume for all months
       =========== 3 failed, 251 passed, 28 skipped, 3 warnings in 403.56s (0:06:43) =====================================================================================================================

   The first column represents the test id. Further information about individual tests are in the `tests reference <tests/index>`_.

   The second column represents the table and column which failed. Some tests are table level tests. Note that the database and table used are not reflected here for brevity.

   The third column gives a brief description of what failed.

#. Understand why the test failed. Most tests (other than schema tests) provide
   a sql query which describes the SQL which was executed during the test. In
   the example, above, we want to understand more about ``P043``.

   Append the flags:

   ``-k P043`` - to select only test `P043`.

   ``--show-sql`` - to display the SQL used during the test


   .. code-block:: console
    :emphasize-text: MY-PROJECT

    $ amlaidatatests --connection_string=bigquery://{MY-PROJECT}?location=US --database=bigquery-public-data.aml_ai_input_dataset

   This time, the full stack trace is displayed for all tests including a trace
   of the SQL used to execute the test. You can use this SQL in the bigquery
   console or from another tool to understand how the test was executed. In this
   example we can see that no rows were to the ``COUNTIF`` statement.

   .. parsed-literal::
       E   amlaidatatests.exceptions.DataTestFailure: Check >=1 party with AML_SAR: 0 rows met criteria. Expected at least 1.
       E   To reproduce this result, run:
       E   SELECT
       E     `t1`.`total_rows`,
       E     `t1`.`matching_rows`,
       E     ieee_divide(`t1`.`matching_rows`, `t1`.`total_rows`) AS `proportion`
       E   FROM (
       E     SELECT
       E       COUNT(*) AS `total_rows`,
       E       COUNTIF(`t0`.`type` = 'AML_SAR') AS `matching_rows`
       E     FROM `gtai-amlai-sandbox-uat`.`my_bq_input_dataset`.`risk_case_event` AS `t0`
       E   ) AS `t1`
       ================= amlaidatatests summary =============================================================================
       -------------------- tests passed: 0 ---------------------------------------------------------------------------------
       ----------------------- skipped: 0 -----------------------------------------------------------------------------------
       ---------------------- warnings: 0 -----------------------------------------------------------------------------------
       ---------------------- failures: 1 -----------------------------------------------------------------------------------
       P043   risk_case_event.type                                  Check >=1 party with AML_SAR: 0 rows met criteria. Expected at least 1.

Next Steps
----------

Read the :doc:`configuration reference <../../reference/configuration>` to:

- Select specific tests to run
- Print the sql used for any failed tests
