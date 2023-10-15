# Datalite

[![Maintainability](https://api.codeclimate.com/v1/badges/9d4ce56bfbd3b63649be/maintainability)](https://codeclimate.com/github/ambertide/datalite/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/9d4ce56bfbd3b63649be/test_coverage)](https://codeclimate.com/github/ambertide/datalite/test_coverage)
[![PyPI version shields.io](https://img.shields.io/pypi/v/datalite.svg)](https://pypi.python.org/pypi/datalite/)
[![PyPI license](https://img.shields.io/pypi/l/datalite.svg)](https://pypi.python.org/pypi/datalite/)
[![Documentation Status](https://readthedocs.org/projects/datalite/badge/?version=latest)](https://datalite.readthedocs.io/en/latest/?badge=latest)

> [!WARNING]  
> This project is a hobby project and it should not be used for security-critical or user facing applications.

It should be noted that Datalite is not suitable for secure web applications, it really is only suitable for cases when you can trust user input.

Datalite is a simple Python
package that binds your dataclasses to a table in a sqlite3 database,
using it is extremely simple, say that you have a dataclass definition,
just add the decorator `@datalite(db_name="db.db")` to the top of the
definition, and the dataclass will now be bound to the file `db.db`

[Detailed API reference](https://datalite.readthedocs.io/en/latest/)

## Download and Install

You can install `datalite` simply by

```shell script
pip install datalite
```

Or you can clone the repository and run

```shell script
python setup.py
```

Datalite has no dependencies! As it is built on Python 3.7+ standard library. Albeit, its tests require `unittest` library.

## Datalite in Action

```python
from dataclasses import dataclass
from datalite import datalite


@datalite(db_path="db.db")
@dataclass
class Student:
    student_id: int
    student_name: str = "John Smith"
```

This snippet will generate a table in the sqlite3 database file `db.db` with
table name `student` and rows `student_id`, `student_name` with datatypes
integer and text, respectively. The default value for `student_name` is
`John Smith`.

## Basic Usage

### Entry manipulation

After creating an object traditionally, given that you used the `datalite` decorator,
the object has three new methods: `.create_entry()`, `.update_entry()`
and `.remove_entry()`, you can add the object to its associated table 
using the former, and remove it using the later. You can also update a record using
the middle.

```python
student = Student(10, "Albert Einstein")
student.create_entry()  # Adds the entry to the table associated in db.db.
student.student_id = 20 # Update an object on memory.
student.update_entry()  # Update the corresponding record in the database.
student.remove_entry()  # Removes from the table.
```

But what if you have created your object in a previous session, or wish
to remove an object unreachable? ie: If the object is already garbage 
collected by the Python interpreter? `remove_from(class_, obj_id)` is
a function that can be used for this express purpose, for instance:

```python
remove_from(Student, 2)  # Removes the Student with obj_id 2.
```

Object IDs are auto-incremented, and correspond to the order the entry were
inserted onto the system.

## Fetching Records
> :warning: **Limitation! Fetch can only fetch limited classes correctly**: int, float, bytes and str!

Finally, you may wish to recreate objects from a table that already exist, for
this purpose we have the module `fetch` module, from this you can import `
fetch_from(class_, obj_id)` as well as `is_fetchable(className, object_id)` 
former fetches a record from the SQL database given its unique object_id 
whereas the latter checks if it is fetchable (most likely to check if it exists.)

```python
>>> fetch_from(Student, 2)
Student(student_id=10, student_name='Albert Einstein')
```

We also have four helper methods, `fetch_range(class_, range_)` and
`fetch_all(class_)` are very similar: the former fetches the records
fetchable from the object id range provided by the user, whereas the
latter fetches all records. Both return a tuple of `class_` objects.

The last two helper methods, `fetch_if(class_, condition)` fetches all
the records of type `class_` that fit a certain condition. Here conditions
must be written is SQL syntax. For easier, only one conditional checks, there
is `fetch_equals(class_, field, value)` that checks the value of only one `field`
and returns the object whose `field` equals the provided `value`.

#### Pagination

`datalite` also supports pagination on `fetch_if`, `fetch_all` and `fetch_where`,
you can specify `page` number and `element_count` for each page (default 10), for
these functions in order to get a subgroup of records.
