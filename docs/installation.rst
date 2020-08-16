Getting Started
=================

Welcome to the documentation of datalite. Datalite provides a simple, intuitive way to bind dataclasses
to sqlite3 databases. In its current version, it provides implicit support for conversion between
``int``, ``float``, ``str``, ``bytes`` classes and their ``sqlite3`` counterparts, default values,
basic schema migration and fetching functions.

Installation
############

Simply write:

.. code-block:: bash

    pip install datalite

In the shell. And then, whenever you want to use it in Python, you can use:

.. code-block:: python

    import datalite

