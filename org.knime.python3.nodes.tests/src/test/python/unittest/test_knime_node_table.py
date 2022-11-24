import unittest

import knime.api.schema as ks
from knime.api.schema import _ColumnSlicingOperation
<<<<<<< HEAD
import knime_node_table as knt
=======
import knime.api.table as knt
import knime_node_arrow_table as knat
>>>>>>> AP-19592: Handle Missing Geospatial Values

# ------------------------------------------------------------------
# Tests for Schema and Table
# ------------------------------------------------------------------


class SchemaTest(unittest.TestCase):
    def test_schema_ops(self):
        types = [ks.int32(), ks.int64(), ks.double(), ks.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        s = ks.Schema.from_types(types, names)
        sv = s[0:2]
        self.assertEqual(4, s.num_columns)
        self.assertIsInstance(sv, ks._ColumnarView)
        self.assertNotIsInstance(sv, knt._TabularView)
        self.assertIsInstance(sv.operation, _ColumnSlicingOperation)
        self.assertIsInstance(sv.delegate, ks.Schema)
        # the following line delegates the call to -> apply operation -> call column_names on result
        self.assertEqual(["Ints", "Longs"], sv.column_names)
        self.assertIsInstance(s[2], ks._ColumnarView)
        self.assertEqual("Doubles", s[2].name)
        self.assertIsInstance(s[3], ks._ColumnarView)
        self.assertEqual(ks.string(), s[3].ktype)
        with self.assertRaises(AttributeError):
            # try calling something random
            s[1].foobar(2)

    def test_column_iteration(self):
        types = [ks.int32(), ks.int64(), ks.double(), ks.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        s = ks.Schema.from_types(types, names)

        columns = [c for c in s]
        self.assertEqual(types, [c.ktype for c in columns])
        self.assertEqual(names, [c.name for c in columns])

    def test_schema_list_appending(self):
        types = [ks.int32(), ks.int64(), ks.double(), ks.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        new_names = ["a", "b", "c"]

        s = ks.Schema.from_types(types, names)
        column_list = [ks.Column(ktype=ks.double(), name=n) for n in new_names]
        s = s.append(column_list).get()

        s2 = ks.Schema.from_types(types, names)
        for n in new_names:
            s2 = s2.append(ks.Column(ktype=ks.double(), name=n))
        s2 = s2.get()
        self.assertEqual(s, s2, "Appended list does not match single appending")

    def test_remove(self):
        types = [ks.int32(), ks.int64(), ks.double(), ks.string()]
        names = ["1", "2", "3", "4"]
        s = ks.Schema(types, names)
        _s = s.remove(names[0])

        # Check Type
        self.assertIsInstance(_s, ks._ColumnarView)
        self.assertIsInstance(s, ks.Schema)
        self.assertIsInstance(_s.operation, _ColumnSlicingOperation)
        self.assertIsInstance(_s.delegate, ks.Schema)

        # Check Errors
        with self.assertRaises(ValueError):
            s.remove("5")

        with self.assertRaises(ValueError):
            s.remove(["5"])

        with self.assertRaises(ValueError):
            s.remove(["4", "5"])

        with self.assertRaises(IndexError):
            s.remove(-1)

        with self.assertRaises(IndexError):
            s.remove(len(names))

        # Check Functionality
        _s = s.remove("4")
        self.assertEqual(s.num_columns - 1, _s.get().num_columns)

        _s = s.remove(["4"])
        self.assertEqual(s.num_columns - 1, _s.get().num_columns)

        _s = s.remove(["4", "3"])
        self.assertEqual(s.num_columns - 2, _s.get().num_columns)

        _s = _s.remove("1").remove("2")
        self.assertEqual(0, _s.get().num_columns)

        _s = s.remove(3)
        self.assertEqual(s.num_columns - 1, _s.get().num_columns)

    def test_schema_from_columns(self):
        types = [ks.int32(), ks.int64(), ks.double(), ks.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        columns = [ks.Column(t, n) for t, n in zip(types, names)]

        # test if from columns works in general
        s = ks.Schema.from_columns(columns)
        for i, col in enumerate(s):
            self.assertEqual(types[i], col.ktype)
            self.assertEqual(names[i], col.name)

        # test if from columns works with only one column in the list
        for t, n in zip(types, names):
            columns = [ks.Column(t, n)]
            s = ks.Schema.from_columns(columns)
            self.assertEqual(t, s[0].ktype)
            self.assertEqual(n, s[0].name)

        # test if from columns works with only one column
        for t, n in zip(types, names):
            column = ks.Column(t, n)
            s = ks.Schema.from_columns(column)
            self.assertEqual(t, s[0].ktype)
            self.assertEqual(n, s[0].name)


if __name__ == "__main__":
    unittest.main()
