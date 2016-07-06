#########################
Sync - `/sync`
#########################


Methods
=======

- :http:post:`/sync` --- modify a build record, usually to register a build upload.
   :query query: ADQL/SQL Query String

   :reqheader Accept: ``application/json`` or ``application/x-votable+xml``

.. autoflask:: lsst.dax.dbserv:create_app(profile='development')
   :endpoints: dbREST.sync_query