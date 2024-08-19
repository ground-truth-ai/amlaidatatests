==========
Installing
==========

pypi
====

The recommended way to install amlaidatatests is using pip.
Using pip this way is the best way to ensure that you quickly receive any updates.

You can install amlaidatatests directly using `pip <https://pypi.org/project/amlaidatatests/>`_.

We strongly recommend using a `virtualenv <https://pypi.org/project/virtualenv/>`_.

.. code-block:: console

   $ pip install amlaidatatests

airgapped/offline install
=========================


You can use `deps.dev <https://deps.dev/pypi/amlaidatatests>`_ to inspect our
dependencies and project for security vulnerabilities.

We also offer an alternative way to install using pre-packaged tar files the
dependencies required to run ``amlaidatatests``. You should also note that these
files were the latest at the time of the release, but may not be the latest
version by the time you download them.

These files are built for the limited range of operating systems. We can
consider adding further systems, see :ref:`Feature requests and
support`.

Installing into an air-gapped environment.

#. Identify a version corresponding to your operating system and python version
   you are planning to use. Or download it.
#. Copy the link to that version.
#. Make a directory for the vendored files to land in

   .. code-block:: console

      $ mkdir lib

#. Download and extract the files, replace LINK with the link from the table
   above. In a truly air-gapped environment you may need to carry out this step
   and transfer the downloaded file to the air-gapped machine.

   .. code-block:: console

      $ curl <LINK> | tar zxvf - -C lib

#. Install the dependencies directly from the folder

   .. code-block:: console

      $ pip install amlaidatatests --no-index --find-links lib/

Available Vendored Versions
---------------------------

.. csv-table::
   :url: https://storage.googleapis.com/amlai-public-vendored-artifacts-hosting/index.csv



Are you sure?
-------------

Many corporate environments place restrictions around package installations. We
strongly recommend following your internal corporate processes for using pip.

Many environments will already use commercial tools to manage and monitor
dependencies, including python dependencies, and keep an inventory of usage of
opensource dependencies across your your organization.

Using these tools is the best way to keep your organization safe. Security
vulnerabilities emerge over time and are often the result of manual discovery by
security researchers, and your use of ``amlaidatatests`` and its dependencies
can be appropriately logged. This process is not the same as virus or malware
scanning.

Many of these tools can be configured as a pypi mirror, blocking and logging the
use of dependencies in the organization so they can be mitigated if future
issues are identified whilst allowing for regular security updating through pip.

Regularly updating software is the best way to mitigate security
vulnerabilities.

Requirements
============

Operating Systems
-----------------

We test on modern linux distributions.

Python Versions
----------------

Python 3.10, 3.11, and 3.12.

Backends
--------

See :doc:`backends <../../databases/index>` for a list of supported backends.

Next Ste
============

Read the :doc:`quickstart <../quickstart/index>`
