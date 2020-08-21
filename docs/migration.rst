Schema Migrations
==================

Datalite provides a module, ``datalite.migrations`` that handles schema migrations. When a class
definition is modified, ``datalite.migrations.basic_migration`` can be called to automatically
transfer records to a table fitting the new definitions.

Let us say we have made changes to the fields of a dataclass called ``Student`` and now,
we want these changes to be made to the database. More specifically, we had a field called
``studentt_id`` and realised this was a typo, we want it to be named into ``student_id``,
and we want the values that was previously hold in this column to be persistent despite the
name change. We can achieve this easily by:

.. code-block:: python

    datalite.basic_migration(Student, {'studentt_id': 'student_id'})

This will make all the changes, if we had not provided the second argument,
the values would be lost.