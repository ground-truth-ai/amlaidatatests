=========================
Troubleshooting
=========================

roles/serviceusage.serviceUsageConsumer role
--------------------------------------------

Tests fail with a message suggesting that the user needs a serviceUsageConsumer
role. This error occurs frequently when using the google cloud console, which
automatically sets a quota project for all users.

The serviceUsageConsumer role is required if setting an API quota project. If a
quota project is set then the calling principal must have the
:code:`roles/serviceusage.serviceUsageConsumer` permission on the quota project.

To read more about this issue, see `this github issue <https://github.com/ground-truth-ai/amlaidatatests/issues/24>`_.

Solution
........

Option 1
````````

Grant the user the :code:`serviceUsageConsumer` role on the project.

Option 2
````````

Do not set an API quota project. Tests may still emit warnings relating to the
use of user credentials without a quota project. However, bigquery jobs are a
resource based API (that is, a bigquery job submitted by :code:`amlaidatatests`
is associated with a project).

amlaidatatests uses quota project configuration from the underlying python
library, which is documented `here
<https://cloud.google.com/docs/quotas/set-quota-project>`_.

*  Ensure the :code:`GOOGLE_CLOUD_QUOTA_PROJECT` environment variable is not
   set.

   bash: :code:`unset GOOGLE_CLOUD_QUOTA_PROJECT`

*  Review other ways of setting quota projects `here
   <https://cloud.google.com/docs/quotas/set-quota-project>`_ and ensure they do
   not apply to your configuration.

Full Error
..........

.. parsed-literal::
    google.api_core.exceptions.Forbidden: 403 GET https://bigquery.googleapis.com/bigquery/v2/projects/MY_PROJECT/datasets/MY_DATASET/tables/transaction?prettyPrint=false: Caller does not have required permission to use project MY_PROJECT. Grant the caller the roles/serviceusage.serviceUsageConsumer role, or a custom role with the serviceusage.services.use permission
