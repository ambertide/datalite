# Datalite

Datalite is a simple Python
package that binds your dataclasses to a table in a sqlite3 database,
using it is extremely simple, say that you have a dataclass definition,
just add the decorator `@sqlify(db_name="db.db")` to the top of the
definition, and the dataclass will now be bound to the file `db.db`

For example:

```python
from dataclasses import dataclass
from datalite import sqlify


@sqlify(db_path="db.db")
@dataclass
class Student:
    student_id: int
    student_name: str = "John Smith"
```

This snippet will generate a table in the sqlite3 database file `db.db` with
table name `student` and rows `student_id`, `student_name` with datatypes
integer and text, respectively. The default value for `student_name` is
`John Smith`.

## Creating a new object instance

If you create a new object with default Python methods, the object will not 
be inserted into the table by default. However, the classes that are created
with `datalite` has a argument in their init method. So, if you write
`Student(1, create_entry=True)` rather than just saying `Student(1)`, the
entry equivalent of the newly created student will be inserted into
the table without any problems.

## Deleting an object instance

Another method that is added to any dataclass created with `datalite` is the
`.remove()` method. By deleting a class with the `.remove()` you will also
delete its equivalent entry from the database.