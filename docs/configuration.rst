Configuration
============================================

Configuration Flags
-------------------

.. argparse::
   :module: amlaidatatests.cli
   :func: build_parser
   :prog: amlaidatatests

   foo
        This text will go right after the "foo" positional argument help.

   --conf
       Content appended to the --output option, regardless of the argument group. Lol

Useful Pytest Options
---------------------

amlaidatatests uses pytest to manage and run tests. This has a number of
advantages, particularly allowing the use of a wide variety of pytest plugins.

To view all available pytest configuration values, run ``amlaidatatests
--pytest-help``, or review the `pytest documentation
<https://docs.pytest.org/en/latest/reference/reference.html#command-line-flags>`_.

.. confval:: -k
   :type: ``str``

   The pytest keyword option allows filtering
   tests to run
