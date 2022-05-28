import unittest
from datalite import datalite
from datalite.constraints import Unique, ConstraintFailedError
from datalite.fetch import fetch_if, fetch_all, fetch_range, fetch_from, fetch_equals, fetch_where
from datalite.mass_actions import create_many, copy_many
from sqlite3 import connect
from dataclasses import dataclass, asdict
from math import floor
from datalite.migrations import basic_migrate, _drop_table


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

@datalite(db_path='test.db')
@dataclass
class Migrate1:
    ordinal: int
    conventional: str


@datalite(db_path='test.db')
@dataclass
class Migrate2:
    cardinal: Unique[int] = 1
    str_: str = "default"


@datalite(db_path='test.db')
@dataclass
class ConstraintedClass:
    unique_str: Unique[str]


@datalite(db_path='test.db')
@dataclass
class MassCommit:
    str_: str


def getValFromDB(obj_id = 1):
    with connect('test.db') as db:
        cur = db.cursor()
        cur.execute(f'SELECT * FROM testclass WHERE obj_id = {obj_id}')
        fields = list(TestClass.__dataclass_fields__.keys())
        fields.sort()
        repr = dict(zip(fields, cur.fetchall()[0][1:]))
        field_types = {key: value.type for key, value in TestClass.__dataclass_fields__.items()}
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

    def testFetchWhere(self):
        t_objs = fetch_where(FetchClass, 'str_', 'b')
        self.assertEqual(tuple(self.objs[1:]), t_objs)

    def testFetchRange(self):
        t_objs = fetch_range(FetchClass, range(self.objs[0].obj_id, self.objs[2].obj_id))
        self.assertEqual(tuple(self.objs[0:2]), t_objs)

    def tearDown(self) -> None:
        [obj.remove_entry() for obj in self.objs]


class DatabaseFetchPaginationCalls(unittest.TestCase):
    def setUp(self) -> None:
        self.objs = [FetchClass(i, f'{floor(i/10)}') for i in range(30)]
        [obj.create_entry() for obj in self.objs]

    def testFetchAllPagination(self):
        t_objs = fetch_all(FetchClass, 1, 10)
        self.assertEqual(tuple(self.objs[:10]), t_objs)

    def testFetchWherePagination(self):
        t_objs = fetch_where(FetchClass, 'str_', '0', 2, 5)
        self.assertEqual(tuple(self.objs[5:10]), t_objs)

    def testFetchIfPagination(self):
        t_objs = fetch_if(FetchClass, 'str_ = "0"', 1, 5)
        self.assertEqual(tuple(self.objs[:5]), t_objs)

    def tearDown(self) -> None:
        [obj.remove_entry() for obj in self.objs]


class DatabaseMigration(unittest.TestCase):
    def setUp(self) -> None:
        self.objs = [Migrate1(i, "a") for i in range(10)]
        [obj.create_entry() for obj in self.objs]

    def testBasicMigrate(self):
        global Migrate1, Migrate2
        Migrate1 = Migrate2
        Migrate1.__name__ = 'Migrate1'
        basic_migrate(Migrate1, {'ordinal': 'cardinal'})
        t_objs = fetch_all(Migrate1)
        self.assertEqual([obj.ordinal for obj in self.objs], [obj.cardinal for obj in t_objs])
        self.assertEqual(["default" for _ in range(10)], [obj.str_ for obj in t_objs])

    def tearDown(self) -> None:
        t_objs = fetch_all(Migrate1)
        [obj.remove_entry() for obj in t_objs]
        _drop_table('test.db', 'migrate1')


def helperFunc():
    obj = ConstraintedClass("This string is supposed to be unique.")
    obj.create_entry()


class DatabaseConstraints(unittest.TestCase):
    def setUp(self) -> None:
        self.obj = ConstraintedClass("This string is supposed to be unique.")
        self.obj.create_entry()

    def testUniquness(self):
        self.assertRaises(ConstraintFailedError, helperFunc)

    def testNullness(self):
        self.assertRaises(ConstraintFailedError, lambda : ConstraintedClass(None).create_entry())

    def tearDown(self) -> None:
        self.obj.remove_entry()


class DatabaseMassInsert(unittest.TestCase):
    def setUp(self) -> None:
        self.objs = [MassCommit(f'cat + {i}') for i in range(30)]


    def testMassCreate(self):
        with connect('other.db') as con:
            cur = con.cursor()
            cur.execute(f'CREATE TABLE IF NOT EXISTS MASSCOMMIT (obj_id, str_)')

        start_tup = fetch_all(MassCommit)
        create_many(self.objs, protect_memory=False)
        _objs = fetch_all(MassCommit)
        self.assertEqual(_objs, start_tup + tuple(self.objs))

    def _testMassCopy(self):
        setattr(MassCommit, 'db_path', 'other.db')
        start_tup = fetch_all(MassCommit)
        copy_many(self.objs, 'other.db', False)
        tup = fetch_all(MassCommit)
        self.assertEqual(tup, start_tup + tuple(self.objs))

    def tearDown(self) -> None:
        [obj.remove_entry() for obj in self.objs]


if __name__ == '__main__':
    unittest.main()
