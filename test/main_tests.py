import unittest
try:
    from datalite import datalite
except ModuleNotFoundError:
    import importlib
    importlib.import_module('datalite', '../datalite/')
from sqlite3 import connect
from dataclasses import dataclass, asdict
from os import remove


@datalite(db_path='test.db')
@dataclass
class TestClass:
    integer_value: int = 1
    byte_value: bytes = b'a'
    float_value: float = 0.4
    str_value: str = 'a'

    def __eq__(self, other):
        return asdict(self) == asdict(other)


def getValFromDB(obj_id = 1):
    with connect('test.db') as db:
        cur = db.cursor()
        cur.execute(f'SELECT * FROM testclass WHERE obj_id = {obj_id}')
        fields = list(TestClass.__dataclass_fields__.keys())
        fields.sort()
        repr = dict(zip(fields, cur.fetchall()[0][1:]))
        field_types = {key: value.type for key, value in TestClass.__dataclass_fields__.items()}
        for key in fields:
            if field_types[key] == bytes:
                repr[key] = bytes(repr[key], encoding='utf-8')
        test_object = TestClass(**repr)
    return test_object


class DatabaseMain(unittest.TestCase):
    def setUp(self) -> None:
        self.test_object = TestClass(12, b'bytes', 0.4, 'TestValue')

    def test_creation(self):
        self.test_object.create_entry()
        self.assertEqual(self.test_object, getValFromDB())

    def test_update(self):
        self.test_object.create_entry()
        self.test_object.integer_value = 40
        self.test_object.update_entry()
        from_db = getValFromDB(getattr(self.test_object, 'obj_id'))
        self.assertEqual(self.test_object.integer_value, from_db.integer_value)

    def test_delete(self):
        with connect('test.db') as db:
            cur = db.cursor()
            cur.execute('SELECT * FROM testclass')
            objects = cur.fetchall()
        init_len = len(objects)
        self.test_object.create_entry()
        self.test_object.remove_entry()
        with connect('test.db') as db:
            cur = db.cursor()
            cur.execute('SELECT * FROM testclass')
            objects = cur.fetchall()
        self.assertEqual(len(objects), init_len)


if __name__ == '__main__':
    unittest.main()
