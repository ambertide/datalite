"""
This module includes functions to insert multiple records
to a bound database at one time, with one time open and closing
of the database file.
"""
from typing import TypeVar, Union, List, Tuple
from dataclasses import asdict
from warnings import warn
from .constraints import ConstraintFailedError
from .commons import _convert_sql_format, _create_table
import sqlite3 as sql

T = TypeVar('T')


class HeterogeneousCollectionError(Exception):
    """
    :raise : if the passed collection is not homogeneous.
        ie: If a List or Tuple has elements of multiple
        types.
    """
    pass


def _check_homogeneity(objects: Union[List[T], Tuple[T]]) -> None:
    """
    Check if all of the members a Tuple or a List
    is of the same type.

    :param objects: Tuple or list to check.
    :return: If all of the members of the same type.
    """
    class_ = objects[0].__class__
    if not all([isinstance(obj, class_) or isinstance(objects[0], obj.__class__)  for obj in objects]):
        raise HeterogeneousCollectionError("Tuple or List is not homogeneous.")


def _toggle_memory_protection(cur: sql.Cursor, protect_memory: bool) -> None:
    """
    Given a cursor to an sqlite3 connection, if memory protection is false,
        toggle memory protections off.

    :param cur: Cursor to an open SQLite3 connection.
    :param protect_memory: Whether or not should memory be protected.
    :return: Memory protections off.
    """
    if not protect_memory:
        warn("Memory protections are turned off, "
             "if operations are interrupted, file may get corrupt.", RuntimeWarning)
        cur.execute("PRAGMA synchronous = OFF")
        cur.execute("PRAGMA journal_mode = MEMORY")


def _mass_insert(objects: Union[List[T], Tuple[T]], db_name: str, protect_memory: bool = True) -> None:
    """
    Insert multiple records into an SQLite3 database.

    :param objects: Objects to insert.
    :param db_name: Name of the database to insert.
    :param protect_memory: Whether or not memory
        protections are on or off.
    :return: None
    """
    _check_homogeneity(objects)
    sql_queries = []
    first_index: int = 0
    table_name = objects[0].__class__.__name__.lower()

    for i, obj in enumerate(objects):
        kv_pairs = asdict(obj).items()
        setattr(obj, "obj_id", first_index + i + 1)
        sql_queries.append(f"INSERT INTO {table_name}(" +
                           f"{', '.join(item[0] for item in kv_pairs)})" +
                           f" VALUES ({', '.join(_convert_sql_format(item[1]) for item in kv_pairs)});")
    with sql.connect(db_name) as con:
        cur: sql.Cursor = con.cursor()
        try:
            _toggle_memory_protection(cur, protect_memory)
            cur.execute(f"SELECT obj_id FROM {table_name} ORDER BY obj_id DESC LIMIT 1")
            index_tuple = cur.fetchone()
            if index_tuple:
                first_index = index_tuple[0]
            cur.executescript("BEGIN TRANSACTION;\n" + '\n'.join(sql_queries) + '\nEND TRANSACTION;')
        except sql.IntegrityError:
            raise ConstraintFailedError
    con.commit()


def create_many(objects: Union[List[T], Tuple[T]], protect_memory: bool = True) -> None:
    """
    Insert many records corresponding to objects
    in a tuple or a list.

    :param protect_memory: If False, memory protections are turned off,
        makes it faster.
    :param objects: A tuple or a list of objects decorated
        with datalite.
    :return: None.
    """
    if objects:
        _mass_insert(objects, getattr(objects[0], "db_path"), protect_memory)
    else:
        raise ValueError("Collection is empty.")


def copy_many(objects: Union[List[T], Tuple[T]], db_name: str, protect_memory: bool = True) -> None:
    """
    Copy many records to another database, from
    their original database to new database, do
    not delete old records.

    :param objects: Objects to copy.
    :param db_name: Name of the new database.
    :param protect_memory: Wheter to protect memory during operation,
        Setting this to False will quicken the operation, but if the
        operation is cut short, database file will corrupt.
    :return: None
    """
    if objects:
        with sql.connect(db_name) as con:
            cur = con.cursor()
            _create_table(objects[0].__class__, cur)
            con.commit()
        _mass_insert(objects, db_name, protect_memory)
    else:
        raise ValueError("Collection is empty.")
