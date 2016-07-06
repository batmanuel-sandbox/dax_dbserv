############
Introduction
############

dbserv provides a RESTFul API for querying a database. It is part of the webserv family of REST APIs for LSST.

Users and Authentication
========================

dbserv currently requires no authentication but does require  you to be on the NCSA network. All requests are
performed as a common database user.


Content types
=============

Data in the bodies is best supported through the ``application/json`` media type, but we also support VOTable and HTML
output.


Resources
=========

dbserv currently only implements the `sync` endpoint from TAP.

:doc:`Sync <sync>`
   sync endpoint to dbserv. See [TAP's /sync description](http://www.ivoa.net/documents/TAP/20100327/REC-TAP-1.0.html#_2_2_1___sync)
