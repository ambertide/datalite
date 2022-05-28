import sqlite3 as sql
from typing import List, Tuple, Any
from .commons import _convert_sql_format, _get_table_cols


def _insert_pagination(query: str, page: int, element_count: int) -> str:
    """
    Insert the pagination arguments if page number is given.

    :param query: Query to insert to
    :param page: Page to get.
    :param element_count: Element count in each page.
    :return: The modified (or not) query.
    """
    if page:
        query += f" ORDER BY obj_id LIMIT {element_count} OFFSET {(page - 1) * element_count}"
    return query + ";"


def is_fetchable(class_: type, obj_id: int) -> bool:
    """
    Check if a record is fetchable given its obj_id and
    class_ type.

    :param class_: Class type of the object.
    :param obj_id: Unique obj_id of the object.
    :return: If the object is fetchable.
    """
    with sql.connect(getattr(class_, 'db_path')) as con:
        cur: sql.Cursor = con.cursor()
        try:
            cur.execute(f"SELECT 1 FROM {class_.__name__.lower()} WHERE obj_id = ?;", (obj_id, ))
        except sql.OperationalError:
            raise KeyError(f"Table {class_.__name__.lower()} does not exist.")
    return bool(cur.fetchall())


def fetch_equals(class_: type, field: str, value: Any, ) -> Any:
    """
    Fetch a class_ type variable from its bound db.

    :param class_: Class to fetch.
    :param field: Field to check for, by default, object id.
    :param value: Value of the field to check for.
    :return: The object whose data is taken from the database.
    """
    table_name = class_.__name__.lower()
    with sql.connect(getattr(class_, 'db_path')) as con:
        cur: sql.Cursor = con.cursor()
        cur.execute(f"SELECT * FROM {table_name} WHERE {field} = ?;", (value, ))
        obj_id, *field_values = list(cur.fetchone())
        field_names: List[str] = _get_table_cols(cur, class_.__name__.lower())
    kwargs = dict(zip(field_names, field_values))
    obj = class_(**kwargs)
    setattr(obj, "obj_id", obj_id)
    return obj


def fetch_from(class_: type, obj_id: int) -> Any:
    """
    Fetch a class_ type variable from its bound dv.

    :param class_: Class to fetch from.
    :param obj_id: Unique object id of the object.
    :return: The fetched object.
    """
    if not is_fetchable(class_, obj_id):
        raise KeyError(f"An object with {obj_id} of type {class_.__name__} does not exist, or"
                       f"otherwise is unreachable.")
    return fetch_equals(class_, 'obj_id', obj_id)


def _convert_record_to_object(class_: type, record: Tuple[Any], field_names: List[str]) -> Any:
    """
    Convert a given record fetched from an SQL instance to a Python Object of given class_.

    :param class_: Class type to convert the record to.
    :param record: Record to get data from.
    :param field_names: Field names of the class.
    :return: the created object.
    """
    kwargs = dict(zip(field_names, record[1:]))
    field_types = {key: value.type for key, value in class_.__dataclass_fields__.items()}
    for key in kwargs:
        if field_types[key] == bytes:
            kwargs[key] = bytes(kwargs[key], encoding='utf-8')
    obj_id = record[0]
    obj = class_(**kwargs)
    setattr(obj, "obj_id", obj_id)
    return obj


def fetch_if(class_: type, condition: str, page: int = 0, element_count: int = 10) -> tuple:
    """
    Fetch all class_ type variables from the bound db,
    provided they fit the given condition

    :param class_: Class type to fetch.
    :param condition: Condition to check for.
    :param page: Which page to retrieve, default all. (0 means closed).
    :param element_count: Element count in each page.
    :return: A tuple of records that fit the given condition
        of given type class_.
    """
    table_name = class_.__name__.lower()
    with sql.connect(getattr(class_, 'db_path')) as con:
        cur: sql.Cursor = con.cursor()
        cur.execute(_insert_pagination(f"SELECT * FROM {table_name} WHERE {condition}", page, element_count))
        records: list = cur.fetchall()
        field_names: List[str] = _get_table_cols(cur, table_name)
    return tuple(_convert_record_to_object(class_, record, field_names) for record in records)


def fetch_where(class_: type, field: str, value: Any, page: int = 0, element_count: int = 10) -> tuple:
    """
    Fetch all class_ type variables from the bound db,
    provided that the field of the records fit the
    given value.

    :param class_: Class of the records.
    :param field: Field to check.
    :param value: Value to check for.
    :param page: Which page to retrieve, default all. (0 means closed).
    :param element_count: Element count in each page.
    :return: A tuple of the records.
    """
    return fetch_if(class_, f"{field} = {_convert_sql_format(value)}", page, element_count)


def fetch_range(class_: type, range_: range) -> tuple:
    """
    Fetch the records in a given range of object ids.

    :param class_: Class of the records.
    :param range_: Range of the object ids.
    :return: A tuple of class_ type objects whose values
        come from the class_' bound database.
    """
    return tuple(fetch_from(class_, obj_id) for obj_id in range_ if is_fetchable(class_, obj_id))


def fetch_all(class_: type, page: int = 0, element_count: int = 10) -> tuple:
    """
    Fetchall the records in the bound database.

    :param class_: Class of the records.
    :param page: Which page to retrieve, default all. (0 means closed).
    :param element_count: Element count in each page.
    :return: All the records of type class_ in
        the bound database as a tuple.
    """
    try:
        db_path = getattr(class_, 'db_path')
    except AttributeError:
        raise TypeError("Given class is not decorated with datalite.")
    with sql.connect(db_path) as con:
        cur: sql.Cursor = con.cursor()
        try:
            cur.execute(_insert_pagination(f"SELECT * FROM {class_.__name__.lower()}", page, element_count))
        except sql.OperationalError:
            raise TypeError(f"No record of type {class_.__name__.lower()}")
        records = cur.fetchall()
        field_names: List[str] = _get_table_cols(cur, class_.__name__.lower())
    return tuple(_convert_record_to_object(class_, record, field_names) for record in records)
