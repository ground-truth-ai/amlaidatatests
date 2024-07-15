Configuration
============================================

Configuration Flags
-------------------

.. confval:: connection_string
   :type: ``str``
   :required: True

   An ibis connection string

.. confval:: database
   :type: ``str``
   :default: **None**
   :required: False

   Like ibis, the AML AI API uses the word database to refer to a collection of tables,
   and the word catalog to refer to a collection of databases

.. confval:: id
   :type: ``str``
   :default: **None**
   :required: True

   ID is used as a way to associate a group of related tables. The location of the id variable
   is injected in the format set by :confval:`table_name_template`. By default, the format of
   this value is ``${table_name}_${id}``.

   For example, if :confval:`table_name_template` is unchanged, if ``id=1234``, then the resultant
   party table name will be ``party_1234``.

.. confval:: table_name_template
   :type: ``str``
   :default: **${table_name}_${id}**
   :required: False

   :confval:`table_name_template` is used to specify how id variable is injected into
