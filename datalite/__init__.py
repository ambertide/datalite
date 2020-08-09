import sqlite3 as sql
from dataclasses import Field, asdict, dataclass
from typing import List, Dict, Optional, Callable, Any, Tuple


def _convert_type(type_: Optional[type], type_overload: Dict[Optional[type], str]) -> str:
    """
    Given a Python type, return the str name of its
    SQLlite equivalent.
    :param type_: A Python type, or None.
    :param type_overload: A type table to overload the custom type table.
    :return: The str name of the sql type.
    >>> _convert_type(int)
    "INTEGER"
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
        return dataclass_
    return decorator


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
            cur.execute(f"SELECT 1 FROM {class_.__name__.lower()} WHERE obj_id = {obj_id};")
        except sql.OperationalError:
            raise KeyError(f"Table {class_.__name__.lower()} does not exist.")
    return bool(cur.fetchall())


def _get_table_cols(cur: sql.Cursor, table_name: str) -> List[str]:
    """
    Get the column data of a table.
    :param cur: Cursor in database.
    :param table_name: Name of the table.
    :return: the information about columns.
    """
    cur.execute(f"PRAGMA table_info({table_name});")
    return [row_info[1] for row_info in cur.fetchall()][1:]


def fetch_from(class_: type, obj_id: int) -> Any:
    """
    Fetch a class_ type variable from its bound db.
    :param class_: Class to fetch.
    :param obj_id: Unique object id of the class.
    :return: The object whose data is taken from the database.
    """
    table_name = class_.__name__.lower()
    if not is_fetchable(class_, obj_id):
        raise KeyError(f"An object with the id {obj_id} in table {table_name} does not exist."
                       f"or is otherwise unable to be fetched.")
    with sql.connect(getattr(class_, 'db_path')) as con:
        cur: sql.Cursor = con.cursor()
        cur.execute(f"SELECT * FROM {class_.__name__.lower()} WHERE obj_id = {obj_id};")  # Guaranteed to work.
        field_values: List[str] = list(cur.fetchone())[1:]
        field_names: List[str] = _get_table_cols(cur, class_.__name__.lower())
    kwargs = dict(zip(field_names, field_values))
    obj = class_(**kwargs)
    setattr(obj, "obj_id", obj_id)
    return obj


def _convert_record_to_object(class_: type, record: Tuple[Any], field_names: List[str]) -> Any:
    """
    Convert a given record fetched from an SQL instance to a Python Object of given class_.
    :param class_: Class type to convert the record to.
    :param record: Record to get data from.
    :param field_names: Field names of the class.
    :return: the created object.
    """
    kwargs = dict(zip(field_names, record[1:]))
    obj_id = record[0]
    obj = class_(**kwargs)
    setattr(obj, "obj_id", obj_id)
    return obj


def fetch_if(class_: type, condition: str) -> tuple:
    """
    Fetch all class_ type variables from the bound db,
    provided they fit the given condition
    :param class_: Class type to fetch.
    :param condition: Condition to check for.
    :return: A tuple of records that fit the given condition
    of given type class_.
    """
    table_name = class_.__name__.lower()
    with sql.connect(getattr(class_, 'db_path')) as con:
        cur: sql.Cursor = con.cursor()
        cur.execute(f"SELECT * FROM {table_name} WHERE {condition};")
        records: list = cur.fetchall()
        field_names: List[str] = _get_table_cols(cur, table_name)
    return tuple(_convert_record_to_object(class_, record, field_names) for record in records)


def fetch_range(class_: type, range_: range) -> tuple:
    """
    Fetch the records in a given range of object ids.
    :param class_: Class of the records.
    :param range_: Range of the object ids.
    :return: A tuple of class_ type objects whose values
    come from the class_' bound database.
    """
    return tuple(fetch_from(class_, obj_id) for obj_id in range_ if is_fetchable(class_, obj_id))


def fetch_all(class_: type) -> tuple:
    """
    Fetchall the records in the bound database.
    :param class_: Class of the records.
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
            cur.execute(f"SELECT * FROM {class_.__name__.lower()}")
        except sql.OperationalError:
            raise TypeError(f"No record of type {class_.__name__.lower()}")
        records = cur.fetchall()
        field_names: List[str] = _get_table_cols(cur, class_.__name__.lower())
    return tuple(_convert_record_to_object(class_, record, field_names) for record in records)
