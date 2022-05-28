"""
Defines the Datalite decorator that can be used to convert a dataclass to
a class bound to a sqlite3 database.
"""
from sqlite3.dbapi2 import IntegrityError
from typing import Dict, Optional, Callable
from dataclasses import asdict
import sqlite3 as sql

from .constraints import ConstraintFailedError
from .commons import _convert_sql_format, _convert_type, _create_table, type_table


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
        try:
            cur.execute(f"INSERT INTO {table_name}("
                        f"{', '.join(item[0] for item in kv_pairs)})"
                        f" VALUES ({', '.join('?' for item in kv_pairs)})",
                        [item[1] for item in kv_pairs])
            self.__setattr__("obj_id", cur.lastrowid)
            con.commit()
        except IntegrityError:
            raise ConstraintFailedError("A constraint has failed.")


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
                f"SET {', '.join(item[0] + ' = ?' for item in kv_pairs)} " + \
                f"WHERE obj_id = {getattr(self, 'obj_id')};"
        cur.execute(query, [item[1] for item in kv_pairs])
        con.commit()


def remove_from(class_: type, obj_id: int):
    with sql.connect(getattr(class_, "db_path")) as con:
        cur: sql.Cursor = con.cursor()
        cur.execute(f"DELETE FROM {class_.__name__.lower()} WHERE obj_id = ?", (obj_id, ))
        con.commit()


def _remove_entry(self) -> None:
    """
    Remove the object's record in the underlying database.
    :param self: self instance.
    :return: None.
    """
    remove_from(self.__class__, getattr(self, 'obj_id'))


def datalite(db_path: str, type_overload: Optional[Dict[Optional[type], str]] = None) -> Callable:
    """Bind a dataclass to a sqlite3 database. This adds new methods to the class, such as
    `create_entry()`, `remove_entry()` and `update_entry()`.

    :param db_path: Path of the database to be binded.
    :param type_overload: Type overload dictionary.
    :return: The new dataclass.
    """
    def decorator(dataclass_: type, *args_i, **kwargs_i):
        types_table = type_table.copy()
        if type_overload is not None:
            types_table.update(type_overload)
        with sql.connect(db_path) as con:
            cur: sql.Cursor = con.cursor()
            _create_table(dataclass_, cur, types_table)
        setattr(dataclass_, 'db_path', db_path)  # We add the path of the database to class itself.
        setattr(dataclass_, 'types_table', types_table)  # We add the type table for migration.
        dataclass_.create_entry = _create_entry
        dataclass_.remove_entry = _remove_entry
        dataclass_.update_entry = _update_entry
        return dataclass_
    return decorator
