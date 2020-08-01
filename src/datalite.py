from os.path import exists
from pathlib import Path
import sqlite3 as sql
from dataclasses import Field, asdict
from typing import List, Dict, Optional, Callable, Any


def _database_exists(db_path: str) -> bool:
    """
    Check if a given database exists.
    :param db_path: Relative path of the database, including the extension.
    :return: True if database exists, False otherwise.
    """
    return exists(db_path)


def _create_db(db_path: str) -> None:
    """
    Create the database file.
    :param db_path: Relative path of the database file, including the extension.
    :return: None.
    """
    Path(db_path).touch()


def _convert_type(type_: Optional[type], type_overload: Dict[Optional[type], str]) -> str:
    """
    Given a Python type, return the str name of its
    SQLlite equivalent.
    :param type_: A Python type, or None.
    :param type_overload: A type table to overload the custom type table.
    :return: The str name of the sql type.
    """
    try:
        return type_overload[type_]
    except KeyError:
        raise TypeError("Requested type not in the default or overloaded type table.")


def _convert_sql_format(value: Any) -> str:
    """
    Given a Python value, convert to string representation
    of the equivalent SQL datatype.
    :param value: A value, ie: a literal, a variable etc.
    :return: The string representation of the SQL equivalent.
    >>> _convert_sql_format(1)
    "1"
    >>> _convert_sql_format("John Smith")
    '"John Smith"'
    """
    if isinstance(value, str):
        return f'"{value}"'
    else:
        return str(value)


def _get_default(default_object: object, type_overload: Dict[Optional[type], str]) -> str:
    """
    Check if the field's default object is filled,
    if filled return the string to be put in the,
    database.
    :param default_object: The default field of the field.
    :param type_overload: Type overload table.
    :return: The string to be put on the table statement,
    empty string if no string is necessary.
    """
    if type(default_object) in type_overload:
        if isinstance(default_object, str):
            return f' DEFAULT "{default_object}"'
        else:
            return f" DEFAULT {str(default_object)}"
    return ""


def _create_table(class_: type, cursor: sql.Cursor, type_overload: Dict[Optional[type], str]) -> None:
    """
    Create the table for a specific dataclass given
    :param class_: A dataclass.
    :param cursor: Current cursor instance.
    :param type_overload: Overload the Python -> SQLDatatype table
    with a custom table, this is that custom table.
    :return: None.
    """
    fields: List[Field] = [class_.__dataclass_fields__[key] for
                           key in class_.__dataclass_fields__.keys()]
    fields.sort(key=lambda field: field.name)  # Since dictionaries *may* be unsorted.
    sql_fields = ', '.join(f"{field.name} {_convert_type(field.type, type_overload)}"
                           f"{_get_default(field.default, type_overload)}" for field in fields)
    sql_fields = "obj_id INTEGER PRIMARY KEY AUTOINCREMENT, " + sql_fields
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {class_.__name__.lower()} ({sql_fields});")


def _create_entry(self, cur: sql.Cursor) -> None:
    """
    Given an object, create the entry for the object. As a side-effect,
    this will set the object_id attribute of the object to the unique
    id of the entry.
    :param cur: Cursor of the database.
    :param self: Instance of the object.
    :param args: Initialisation arguments.
    :param kwargs: Initialisation keyword arguments.
    :return: None.
    """
    table_name: str = self.__class__.__name__.lower()
    kv_pairs = [item for item in asdict(self).items()]
    kv_pairs.sort(key=lambda item: item[0])  # Sort by the name of the fields.
    cur.execute(f"INSERT INTO {table_name}("
                f"{', '.join(item[0] for item in kv_pairs)})"
                f" VALUES ({', '.join(_convert_sql_format(item[1]) for item in kv_pairs)});")
    self.__setattr__("obj_id", cur.lastrowid)


def _modify_init(dataclass_: type):
    def modifier(self, *args, **kwargs):
        self.__init__()
        if "create_entry" in kwargs and kwargs["create_entry"]:
            try:
                with sql.connect(dataclass_.__db_path__) as con:
                    cur: sql.Cursor = con.cursor()
                    self._create_entry(cur)
                    con.commit()
            except AttributeError:
                raise TypeError("Are you sure this is a datalite class?")
    return modifier


def sqlify(db_path: str, type_overload: Optional[Dict[Optional[type], str]] = None,
           *args, **kwargs) -> Callable:
    def decorator(dataclass_: type, *args_i, **kwargs_i):
        if not _database_exists(db_path):
            _create_db(db_path)
        type_table: Dict[Optional[type], str] = {None: "NULL", int: "INTEGER", float: "REAL",
                                                 str: "TEXT", bytes: "BLOB"}
        if type_overload is not None:
            type_table.update(type_overload)
        with sql.connect(db_path) as con:
            cur: sql.Cursor = con.cursor()
            _create_table(dataclass_, cur, type_table)
        dataclass_.__db_path__ == db_path  # We add the path of the database to class itself.
        dataclass_.__init__ = _modify_init(dataclass_)  # Replace the init method.
        return dataclass_
    return decorator
