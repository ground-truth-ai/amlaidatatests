Configuration
============================================

Configuration Flags
-------------------

.. argparse::
   :module: amlaidatatests.cli
   :func: build_parser
   :prog: amlaidatatests

Useful Pytest Options
---------------------

amlaidatatests uses pytest to manage and run tests. This has a number of
advantages, particularly allowing the use of a wide variety of pytest plugins.

To view all available pytest configuration values, run ``amlaidatatests
--pytest-help``, or review the `pytest documentation
<https://docs.pytest.org/en/latest/reference/reference.html#command-line-flags>`_.

.. confval:: -k

   The pytest keyword option allows filtering
   tests to run. For example ``-k transaction_table and primary_key`` will run
   only tests with those keywords.
