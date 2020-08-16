Fetching Functions
===================

A database is hardly useful if data does not persist between program runs. In ``datalite``
one can use ``datalite.fetch`` module to fetch data back from the database.

There are different sorts of fetching. One can fetch all the objects of a class
using ``fetch_all(class_)``, or an object with a specific object id using ``fetch_from(class_, obj_id)``.
There are more functions for plural conditional fetching (``fetch_if``, ``fetch_where``) where
all objects fitting a condition will be returned, as well as singular conditional fetching that returns
the first object that fits a condition (``fetch_equals``).

Pagination
##########

Pagination is a feature that allows a portion of the results to be returned. Since ``datalite``
is built to work with databases that may include large amounts of data, many systems using large
portions of data also make use of pagination. By building pagination inside the system, we hope to
allow easier usage.

* ``fetch_where``
* ``fetch_if``
* ``fetch_all``

Supports pagination, in general, pagination is controlled via two arguments ``page`` and ``element_count``,
``page`` argument specifies which page to be returned and ``element_count`` specifies how many elements
each page has. When ``page`` is set to 0, all results are returned irregardless of the value of the
``element_count``.

.. important::

    More information regarding the ``datalite.fetch`` functions can be found in the API reference.