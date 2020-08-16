Schema Migrations
==================

Datalite provides a module, ``datalite.migrations`` that handles schema migrations. When a class
definition is modified, ``datalite.migrations.basic_migration`` can be called to automatically
transfer records to a table fitting the new definitions.
