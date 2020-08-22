"""
This module includes functions to insert multiple records
    to a bound database at one time, with one time open and closing
    of the database file.
"""
from typing import TypeVar, Union, List, Tuple
from dataclasses import asdict
from .constraints import ConstraintFailedError
from .commons import _convert_sql_format
import sqlite3 as sql

T = TypeVar('T')


class MisformedCollectionError(Exception):
    pass


def is_homogeneous(objects: Union[List[T], Tuple[T]]) -> bool:
    """
    Check if all of the members a Tuple or a List
        is of the same type.

    :param objects: Tuple or list to check.
    :return: If all of the members of the same type.
    """
    class_ = objects[0].__class__
    return all([isinstance(obj, class_) for obj in objects])


def create_many_entries(objects: Union[List[T], Tuple[T]]) -> None:
    """
    Insert many records corresponding to objects
        in a tuple or a list.

    :param objects: A tuple or a list of objects decorated
        with datalite.
    :return: None.
    """
    if not objects or not is_homogeneous(objects):
        raise MisformedCollectionError("Tuple or List is empty or homogeneous.")
    sql_queries = []
    first_index: int = 0
    table_name = objects[0].__class__.__name__.lower()
    for obj in objects:
        kv_pairs = asdict(obj).items()
        sql_queries.append(f"INSERT INTO {table_name}(" +
                           f"{', '.join(item[0] for item in kv_pairs)})" +
                           f" VALUES ({', '.join(_convert_sql_format(item[1]) for item in kv_pairs)});")
    with sql.connect(getattr(objects[0], "db_path")) as con:
        cur: sql.Cursor = con.cursor()
        try:
            cur.execute(f"SELECT obj_id FROM {table_name} ORDER BY obj_id DESC LIMIT 1")
            index_tuple = cur.fetchone()
            if index_tuple:
                first_index = index_tuple[0]
            cur.executescript('\n'.join(sql_queries))
        except sql.IntegrityError:
            raise ConstraintFailedError
        con.commit()
    for i, obj in enumerate(objects):
        setattr(obj, "obj_id", first_index + i)
