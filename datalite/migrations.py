"""
Migrations module deals with migrating data when the object
definitions change. This functions deal with Schema Migrations.
"""
from dataclasses import Field
from os.path import exists
from typing import Dict, Tuple, List
import sqlite3 as sql

from .commons import _create_table, _get_table_cols


def _get_db_table(class_: type) -> Tuple[str, str]:
    """
    Check if the class is a datalite class, the database exists
    and the table exists. Return database and table names.

    :param class_: A datalite class.
    :return: A tuple of database and table names.
    """
    database_name: str = getattr(class_, 'db_path', None)
    if not database_name:
        raise TypeError(f"{class_.__name__} is not a datalite class.")
    table_name: str = class_.__name__.lower()
    if not exists(database_name):
        raise FileNotFoundError(f"{database_name} does not exist")
    with sql.connect(database_name) as con:
        cur: sql.Cursor = con.cursor()
        cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?;", (table_name, ))
        count: int = int(cur.fetchone()[0])
    if not count:
        raise FileExistsError(f"Table, {table_name}, already exists.")
    return database_name, table_name


def _get_table_column_names(database_name: str, table_name: str) -> Tuple[str]:
    """
    Get the column names of table.

    :param database_name: Name of the database the table
        resides in.
    :param table_name: Name of the table.
    :return: A tuple holding the column names of the table.
    """
    with sql.connect(database_name) as con:
        cur: sql.Cursor = con.cursor()
        cols: List[str] = _get_table_cols(cur, table_name)
    return tuple(cols)


def _copy_records(database_name: str, table_name: str):
    """
    Copy all records from a table.

    :param database_name: Name of the database.
    :param table_name: Name of the table.
    :return: A generator holding dataclass asdict representations.
    """
    with sql.connect(database_name) as con:
        cur: sql.Cursor = con.cursor()
        cur.execute(f'SELECT * FROM {table_name};')
        values = cur.fetchall()
        keys = _get_table_cols(cur, table_name)
        keys.insert(0, 'obj_id')
    records = (dict(zip(keys, value)) for value in values)
    return records


def _drop_table(database_name: str, table_name: str) -> None:
    """
    Drop a table.

    :param database_name: Name of the database.
    :param table_name: Name of the table to be dropped.
    :return: None.
    """
    with sql.connect(database_name) as con:
        cur: sql.Cursor = con.cursor()
        cur.execute(f'DROP TABLE {table_name};')
        con.commit()


def _modify_records(data, col_to_del: Tuple[str], col_to_add: Tuple[str],
                    flow: Dict[str, str]) -> Tuple[Dict[str, str]]:
    """
    Modify the asdict records in accordance
        with schema migration rules provided.

    :param data: Data kept as asdict in tuple.
    :param col_to_del: Column names to delete.
    :param col_to_add: Column names to add.
    :param flow: A dictionary that explain
        if the data from a deleted column
        will be transferred to a column
        to be added.
    :return: The modified data records.
    """
    records = []
    for record in data:
        record_mod = {}
        for key in record.keys():
            if key in col_to_del and key in flow:
                record_mod[flow[key]] = record[key]
            elif key in col_to_del:
                pass
            else:
                record_mod[key] = record[key]
        for key_to_add in col_to_add:
            if key_to_add not in record_mod:
                record_mod[key_to_add] = None
        records.append(record_mod)
    return records


def _migrate_records(class_: type, database_name: str, data,
                     col_to_del: Tuple[str], col_to_add: Tuple[str], flow: Dict[str, str]) -> None:
    """
    Migrate the records into the modified table.

    :param class_: Class of the entries.
    :param database_name: Name of the database.
    :param data: Data, asdict tuple.
    :param col_to_del: Columns to be deleted.
    :param col_to_add: Columns to be added.
    :param flow: Flow dictionary stating where
        column data will be transferred.
    :return: None.
    """
    with sql.connect(database_name) as con:
        cur: sql.Cursor = con.cursor()
        _create_table(class_, cur, getattr(class_, 'types_table'))
        con.commit()
    new_records = _modify_records(data, col_to_del, col_to_add, flow)
    for record in new_records:
        del record['obj_id']
        keys_to_delete = [key for key in record if record[key] is None]
        for key in keys_to_delete:
            del record[key]
        class_(**record).create_entry()


def basic_migrate(class_: type, column_transfer: dict = None) -> None:
    """
    Given a class, compare its previous table,
    delete the fields that no longer exist,
    create new columns for new fields. If the
    column_flow parameter is given, migrate elements
    from previous column to the new ones. It should be
    noted that, the obj_ids do not persist.

    :param class_: Datalite class to migrate.
    :param column_transfer: A dictionary showing which
        columns will be copied to new ones.
    :return: None.
    """
    database_name, table_name = _get_db_table(class_)
    table_column_names: Tuple[str] = _get_table_column_names(database_name, table_name)
    values = class_.__dataclass_fields__.values()
    data_fields: Tuple[Field] = tuple(field for field in values)
    data_field_names: Tuple[str] = tuple(field.name for field in data_fields)
    columns_to_be_deleted: Tuple[str] = tuple(column for column in table_column_names if column not in data_field_names)
    columns_to_be_added: Tuple[str] = tuple(column for column in data_field_names if column not in table_column_names)
    records = _copy_records(database_name, table_name)
    _drop_table(database_name, table_name)
    _migrate_records(class_, database_name, records, columns_to_be_deleted, columns_to_be_added, column_transfer)
