from typing import Dict, Optional, List, Callable
from dataclasses import Field, asdict
import sqlite3 as sql
from commons import _convert_sql_format, _convert_type

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
        return f' DEFAULT {_convert_sql_format(default_object)}'
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


def _create_entry(self) -> None:
    """
    Given an object, create the entry for the object. As a side-effect,
    this will set the object_id attribute of the object to the unique
    id of the entry.
    :param self: Instance of the object.
    :return: None.
    """
    with sql.connect(getattr(self, "db_path")) as con:
        cur: sql.Cursor = con.cursor()
        table_name: str = self.__class__.__name__.lower()
        kv_pairs = [item for item in asdict(self).items()]
        kv_pairs.sort(key=lambda item: item[0])  # Sort by the name of the fields.
        cur.execute(f"INSERT INTO {table_name}("
                    f"{', '.join(item[0] for item in kv_pairs)})"
                    f" VALUES ({', '.join(_convert_sql_format(item[1]) for item in kv_pairs)});")
        self.__setattr__("obj_id", cur.lastrowid)
        con.commit()


def _update_entry(self) -> None:
    """
    Given an object, update the objects entry in the bound database.
    :param self: The object.
    :return: None.
    """
    with sql.connect(getattr(self, "db_path")) as con:
        cur: sql.Cursor = con.cursor()
        table_name: str = self.__class__.__name__.lower()
        kv_pairs = [item for item in asdict(self).items()]
        kv_pairs.sort(key=lambda item: item[0])
        query = f"UPDATE {table_name} " + \
                f"SET {', '.join(item[0] + ' = ' + _convert_sql_format(item[1]) for item in kv_pairs)}" + \
                f"WHERE obj_id = {getattr(self, 'obj_id')};"
        cur.execute(query)
        con.commit()


def remove_from(class_: type, obj_id: int):
    with sql.connect(getattr(class_, "db_path")) as con:
        cur: sql.Cursor = con.cursor()
        cur.execute(f"DELETE FROM {class_.__name__.lower()} WHERE obj_id = {obj_id}")
        con.commit()


def _remove_entry(self) -> None:
    """
    Remove the object's record in the underlying database.
    :param self: self instance.
    :return: None.
    """
    remove_from(self.__class__, getattr(self, 'obj_id'))


def datalite(db_path: str, type_overload: Optional[Dict[Optional[type], str]] = None,
             *args, **kwargs) -> Callable:
    def decorator(dataclass_: type, *args_i, **kwargs_i):
        type_table: Dict[Optional[type], str] = {None: "NULL", int: "INTEGER", float: "REAL",
                                                 str: "TEXT", bytes: "BLOB"}
        if type_overload is not None:
            type_table.update(type_overload)
        with sql.connect(db_path) as con:
            cur: sql.Cursor = con.cursor()
            _create_table(dataclass_, cur, type_table)
        setattr(dataclass_, 'db_path', db_path)  # We add the path of the database to class itself.
        dataclass_.create_entry = _create_entry
        dataclass_.remove_entry = _remove_entry
        dataclass_.update_entry = _update_entry
        return dataclass_
    return decorator
