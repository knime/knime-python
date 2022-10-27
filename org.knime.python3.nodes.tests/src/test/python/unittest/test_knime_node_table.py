import unittest
import pyarrow as pa
import pandas as pd

import knime_schema as ks
import knime_node_table as knt
import knime_node_arrow_table as knat

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
        self.assertIsInstance(sv.operation, ks._ColumnSlicingOperation)
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
        self.assertIsInstance(_s.operation, ks._ColumnSlicingOperation)
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


class TableTest(unittest.TestCase):
    def setUp(self):
        import knime.api.table as ktn

        ktn._backend = knat._ArrowBackend(lambda: DummyDataSink())

    def test_remove(self):
        df = pd.DataFrame()
        df["1"] = [1, 2, 3, 4]
        df["2"] = [5.0, 6.0, 7.0, 8.0]
        t = knt.Table.from_pandas(df)
        _t = t.remove("1")

        # Check Type
        self.assertIsInstance(_t, ks._ColumnarView)
        self.assertIsInstance(t, knt.Table)
        self.assertIsInstance(_t.operation, ks._ColumnSlicingOperation)
        self.assertIsInstance(_t.delegate, knt.Table)

        # Check Errors
        with self.assertRaises(ValueError):
            t.remove("5")

        with self.assertRaises(ValueError):
            t.remove(["5"])

        with self.assertRaises(ValueError):
            t.remove(["4", "1"])

        # Check Functionality
        _t = t.remove("1")
        _df = _t.to_pandas()
        self.assertNotIn("1", _df.columns)

        # Check Functionality
        __t = _t.remove("2")
        _df = __t.to_pandas()
        self.assertNotIn("2", _df.columns)
        self.assertNotIn("1", _df.columns)

        # Check Functionality
        __t = _t.remove(["2"])
        _df = __t.to_pandas()
        self.assertNotIn("2", _df.columns)
        self.assertNotIn("1", _df.columns)

    def test_table_ops(self):
        df = pd.DataFrame()
        df[0] = [1, 2, 3, 4]
        df[1] = [5.0, 6.0, 7.0, 8.0]
        t = knt.Table.from_pandas(df)
        tv = t[:1]  # Row Key and first column
        self.assertIsInstance(tv, ks._ColumnarView)
        self.assertIsInstance(tv, knt._TabularView)
        self.assertIsInstance(tv.operation, ks._ColumnSlicingOperation)
        self.assertIsInstance(tv.delegate, knat.ArrowTable)
        self.assertEqual(t.num_rows, tv.num_rows)
        self.assertEqual(t.num_columns - 1, tv.num_columns)
        col_df = tv.to_pandas()
        self.assertEqual(list(df.iloc[:, 0]), list(col_df.iloc[:, 0]))


# ----------------------------------------------------------


class TestDataSource:
    def __init__(self, absolute_path, column_names):
        self._absolute_path = absolute_path
        self._column_names = column_names

    def getAbsolutePath(self):
        return self._absolute_path

    def isFooterWritten(self):
        return True

    def hasColumnNames(self):
        return True

    def getColumnNames(self):
        return self._column_names


class DummyDataSink:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def write(self, data):
        pass

    def close(self):
        pass


class ArrowTableTest(unittest.TestCase):
    @staticmethod
    def _generate_test_table():
        """
        Creates a Dataframe from a KNIME table on disk
        @param path: path for the KNIME Table
        @return: Arrow Table containing data from KNIME's TestDataGenerator
        """
        import os

        knime_generated_table_path = os.path.normpath(
            os.path.join(__file__, "..", "generatedTestData.zip")
        )
        column_names = [
            "StringCol",
            "StringListCol",
            "StringSetCol",
            "IntCol",
            "IntListCol",
            "IntSetCol",
            "LongCol",
            "LongListCol",
            "LongSetCol",
            "DoubleCol",
            "DoubleListCol",
            "DoubleSetCol",
            "TimestampCol",
            "TimestampListCol",
            "TimestampSetCol",
            "BooleanCol",
            "BooleanListCol",
            "BooleanSetCol",
            "URICol",
            "URIListCol",
            "URISetCol",
            "MissingValStringCol",
            "MissingValStringListCol",
            "MissingValStringSetCol",
            "LongStringColumnName",
            "LongDoubleColumnName",
            "Local Date",
            "Local Time",
            "Local Date Time",
            "Zoned Date Time",
            "Period",
            "Duration",
        ]

        test_data_source = TestDataSource(knime_generated_table_path, column_names)
        import knime_arrow as ka

        ads = ka.ArrowDataSource(test_data_source)
        return knat.ArrowSourceTable(ads)

    @classmethod
    def setUpClass(cls):
        import knime.api.table as ktn

        ktn._backend = knat._ArrowBackend(lambda: DummyDataSink())
        cls._test_table = cls._generate_test_table()

    def test_table_setup(self):
        table = self._test_table
        self.assertTrue(table.num_columns > 0)
        self.assertTrue(table.num_rows > 0)
        self.assertIsInstance(table, knat.ArrowSourceTable)
        self.assertIsInstance(table, knt.Table)
        self.assertTrue(hasattr(table, "to_batches"))

    def test_table_view(self):
        table = self._test_table
        tv = table[:, :]
        self.assertTrue(tv.num_columns > 0)
        self.assertTrue(tv.num_rows > 0)
        self.assertEqual(table.num_columns, tv.num_columns)
        self.assertEqual(table.num_rows, tv.num_rows)
        self.assertNotIsInstance(tv, knat.ArrowSourceTable)
        self.assertIsInstance(tv, knt._Tabular)
        self.assertIsInstance(tv, knt._TabularView)
        with self.assertRaises(RuntimeError):
            for b in tv.to_batches():
                print(b)

    def test_row_slicing(self):
        table = self._test_table
        sliced = table[:, 3:13]
        self.assertEqual(table.num_columns, sliced.num_columns)
        self.assertEqual(10, sliced.num_rows)

        sliced = table[:, :13]
        self.assertEqual(table.num_columns, sliced.num_columns)
        self.assertEqual(13, sliced.num_rows)

        sliced = table[:, -5:]
        self.assertEqual(table.num_columns, sliced.num_columns)
        self.assertEqual(5, sliced.num_rows)

    def test_column_slicing_slice(self):
        table = self._test_table
        # use a slice
        sliced = table[3:13]
        self.assertEqual(table.num_rows, sliced.num_rows)
        self.assertEqual(10, sliced.num_columns)

        sliced = table[:13]
        self.assertEqual(table.num_rows, sliced.num_rows)
        self.assertEqual(13, sliced.num_columns)

        sliced = table[-5:]
        self.assertEqual(table.num_rows, sliced.num_rows)
        self.assertEqual(5, sliced.num_columns)

    def test_column_slicing_int(self):
        table = self._test_table
        # use a single index
        sliced = table[2]
        self.assertEqual(1, sliced.num_columns)
        self.assertEqual(sliced.column_names[0], table.column_names[2])

    def test_column_slicing_str(self):
        table = self._test_table
        # use a single column name
        sliced = table[table.column_names[2]]
        self.assertEqual(1, sliced.num_columns)
        self.assertEqual(sliced.column_names[0], table.column_names[2])

    def test_column_slicing_int_list(self):
        table = self._test_table
        # use a list of indices in wild order
        indices = [3, 7, 2]
        sliced = table[indices]
        self.assertEqual(len(indices), sliced.num_columns)
        self.assertTrue(
            all(
                table.column_names[i] == sliced.column_names[e]
                for e, i in enumerate(indices)
            )
        )

    def test_column_slicing_str_list(self):
        table = self._test_table
        # use a list of indices in wild order
        indices = [3, 7, 2]
        names = [table.column_names[i] for i in indices]
        sliced = table[names]
        self.assertEqual(len(names), sliced.num_columns)
        self.assertListEqual(
            [table.column_names[i] for i in indices],
            [sliced.column_names[i] for i in range(len(indices))],
        )

        data = sliced.to_pyarrow()
        self.assertEqual(data.schema.names[0], "<Row Key>")
        self.assertListEqual(
            [table.column_names[i] for i in indices],
            [data.schema.names[i + 1] for i in range(len(indices))],
        )

    def test_both_slicings(self):
        table = self._test_table
        sliced = table[5:10, 5:10]
        self.assertEqual(5, sliced.num_columns)
        self.assertEqual(5, sliced.num_rows)
        data = sliced.to_pyarrow()
        self.assertEqual(5, len(data))

    def test_to_from_pyarrow(self):
        table = self._test_table
        data = table.to_pyarrow()
        other = knt.Table.from_pyarrow(data)
        self.assertEqual(table.num_rows, other.num_rows)
        self.assertEqual(table.num_columns, other.num_columns)
        self.assertEqual(table.column_names, other.column_names)

    def test_to_from_pandas(self):
        table = self._test_table
        data = table.to_pandas()
        other = knt.Table.from_pandas(data)
        self.assertEqual(table.num_rows, other.num_rows)
        self.assertEqual(table.num_columns, other.num_columns)
        self.assertEqual(table.column_names, other.column_names)

    def test_to_batches(self):
        table = self._test_table
        batches = list(table.to_batches())
        self.assertTrue(len(batches) > 0)
        self.assertIsInstance(batches[0], knt.Table)
        self.assertEqual(table.num_columns, batches[0].num_columns)

    def test_batches(self):
        table = self._test_table
        batches = list(table.batches())
        self.assertTrue(len(batches) > 0)
        self.assertIsInstance(batches[0], knt.Table)
        self.assertEqual(table.num_columns, batches[0].num_columns)

    def test_missing_row_key(self):
        table = self._test_table
        data = table.to_pyarrow()
        data_no_row_key = data.select([4, 5, 6])
        self.assertRaises(TypeError, lambda: knt.Table.from_pyarrow(data_no_row_key))

    def test_schema(self):
        table = self._test_table
        schema = table.schema
        self.assertEqual(table.num_columns, schema.num_columns)
        self.assertEqual(schema.column_names[0], "StringCol")
        self.assertEqual(schema[3].ktype, ks.int32())


class BatchOutputTableTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import knime.api.table as ktn

        ktn._backend = knat._ArrowBackend(lambda: DummyDataSink())

    @classmethod
    def tearDownClass(cls):
        import knime.api.table as ktn

        ktn._backend.close()
        knt._backend = None

    def _generate_test_pyarrow_table(self, index):
        nr = 10
        return pa.Table.from_pydict(
            {
                "Key": [f"Row{r + index * nr}" for r in range(nr)],
                "Ints": [r + index * nr for r in range(nr)],
            }
        )

    def _generate_test_pyarrow_batch(self, index):
        nr = 10
        return pa.RecordBatch.from_pydict(
            {
                "Key": [f"Row{r + index * nr}" for r in range(nr)],
                "Ints": [r + index * nr for r in range(nr)],
            }
        )

    def _generate_test_pandas(self, index):
        nr = 10
        return pd.DataFrame(
            {
                "Key": [f"Row{r + index * nr}" for r in range(nr)],
                "Ints": [r + index * nr for r in range(nr)],
            }
        )

    def _generate_test_batch(self, index):
        return knt.Table.from_pyarrow(self._generate_test_pyarrow_table(index))

    def test_create_append(self):
        out = knt.BatchOutputTable.create()
        for i in range(5):
            out.append(self._generate_test_batch(i))

    def test_append_pandas(self):
        out = knt.BatchOutputTable.create()
        for i in range(5):
            out.append(self._generate_test_pandas(i))

    def test_append_pyarrow_table(self):
        out = knt.BatchOutputTable.create()
        for i in range(5):
            out.append(self._generate_test_pyarrow_table(i))

    def test_append_pyarrow_batch(self):
        out = knt.BatchOutputTable.create()
        for i in range(5):
            out.append(self._generate_test_pyarrow_batch(i))

    def test_append_wrong_type(self):
        out = knt.BatchOutputTable.create()
        with self.assertRaises(TypeError):
            out.append([1, 2, 3, 4])

    def test_append_mixed(self):
        out = knt.BatchOutputTable.create()
        out.append(self._generate_test_pyarrow_table(0))
        out.append(self._generate_test_pandas(1))
        out.append(self._generate_test_pyarrow_batch(2))
        out.append(self._generate_test_batch(3))

    def test_create_with_generator(self):
        def batch_generator():
            for i in range(5):
                yield self._generate_test_batch(i)

        out = knt.BatchOutputTable.from_batches(batch_generator())


if __name__ == "__main__":
    unittest.main()
