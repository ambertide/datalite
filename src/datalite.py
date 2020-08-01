from os.path import exists
import sqlite3 as sql
from dataclasses import Field, _MISSING_TYPE
from typing import List, Dict, Optional


"""
The default type table for conversion between
Python types and SQLite3 Datatypes.
"""
type_table: Dict[Optional[type], str] = {None: "NULL", int: "INTEGER", float: "REAL",
                                         str: "TEXT", bytes: "BLOB"}


def _database_exists(db_name: str) -> bool:
    """
    Check if a given database exists.
    :param db_name: Name of the database, including the extension.
    :return: True if database exists, False otherwise.
    """
    return exists(db_name)


def _sqlify(type_: Optional[type], type_overload: Dict[Optional[type], str]) -> str:
    """
    Given a Python type, return the str name of its
    SQLlite equivalent.
    :param type_: A Python type, or None.
    :param type_overload: A type table to overload the custom type table.
    :return: The str name of the sql type.
    """
    types_dict = type_table.copy()
    types_dict.update(type_overload)
    try:
        return types_dict[type_]
    except KeyError:
        raise TypeError("Requested type not in the default or overloaded type table.")


def get_default(default_object: object) -> str:
    """
    Check if the field's default object is filled,
    if filled return the string to be put in the,
    database.
    :param default_object: The default field of the field.
    :return: The string to be put on the table statement,
    empty string if no string is necessary.
    """
    if isinstance(default_object, _MISSING_TYPE):
        return ""
    elif isinstance(default_object, str):
        return f' DEFAULT "{default_object}"'
    else:
        return f" DEFAULT {str(default_object)}"


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
    sql_fields = ', '.join(f"{field.name} {_sqlify(field.type, type_overload)}"
                           f"{get_default(field.default)}" for field in fields)
    cursor.execute(f"CREATE TABLE {class_.__name__.lower} ({sql_fields})")
