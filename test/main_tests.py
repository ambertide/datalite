import unittest
from datalite import datalite, fetch_if, fetch_all, fetch_range, fetch_from, fetch_equals
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


@datalite(db_path='test.db')
@dataclass
class FetchClass:
    ordinal: int
    str_: str

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



class DatabaseFetchCalls(unittest.TestCase):
    def setUp(self) -> None:
        self.objs = [FetchClass(1, 'a'), FetchClass(2, 'b'), FetchClass(3, 'b')]
        [obj.create_entry() for obj in self.objs]

    def testFetchFrom(self):
        t_obj = fetch_from(FetchClass, self.objs[0].obj_id)
        self.assertEqual(self.objs[0], t_obj)

    def testFetchEquals(self):
        t_obj = fetch_equals(FetchClass, 'str_', self.objs[0].str_)
        self.assertEqual(self.objs[0], t_obj)

    def testFetchAll(self):
        t_objs = fetch_all(FetchClass)
        self.assertEqual(tuple(self.objs), t_objs)

    def testFetchIf(self):
        t_objs = fetch_if(FetchClass, "str_ = \"b\"")
        self.assertEqual(tuple(self.objs[1:]), t_objs)

    def testFetchRange(self):
        t_objs = fetch_range(FetchClass, range(self.objs[0].obj_id, self.objs[2].obj_id))
        self.assertEqual(tuple(self.objs[0:2]), t_objs)

    def tearDown(self) -> None:
        [obj.remove_entry() for obj in self.objs]


if __name__ == '__main__':
    unittest.main()
