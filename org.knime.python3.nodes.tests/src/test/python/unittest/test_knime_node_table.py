import unittest
import pyarrow as pa
import pandas as pd
import datetime as dt
import json

import pythonpath
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
        self.assertEqual(ks.string(), s[3].type)
        with self.assertRaises(AttributeError):
            # try calling something random
            s[1].foobar(2)

    def test_column_iteration(self):
        types = [ks.int32(), ks.int64(), ks.double(), ks.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        s = ks.Schema.from_types(types, names)

        columns = [c for c in s]
        self.assertEqual(types, [c.type for c in columns])
        self.assertEqual(names, [c.name for c in columns])


class TableTest(unittest.TestCase):
    def setUp(self):
        def sink_factory():
            return None

        knt._backend = knat._ArrowBackend(sink_factory)

    def test_table_ops(self):
        df = pd.DataFrame()
        df[0] = [1, 2, 3, 4]
        df[1] = [5.0, 6.0, 7.0, 8.0]
        t = knt.Table.from_pandas(df)
        tv = t[0:2]  # Row Key and first column
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


class DummyJavaDataSink:
    def __init__(self) -> None:
        import os

        self._path = os.path.join(os.curdir, "test_data_sink")

    def getAbsolutePath(self):
        return self._path

    def reportBatchWritten(self, offset):
        pass

    def setColumnarSchema(self, schema):
        pass

    def setFinalSize(self, size):
        import os

        os.remove(self._path)

    def write(self, data):
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

        knime_generated_table_path = os.path.join(os.curdir, "generatedTestData.zip")
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
        knt._backend = knat._ArrowBackend(lambda: DummyJavaDataSink())
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
        self.assertTrue(
            all(
                table.column_names[i] == sliced.column_names[e]
                for e, i in enumerate(indices)
            )
        )

        data = sliced.to_pyarrow()
        self.assertTrue(
            all(
                table.column_names[i] == data.schema.names[e]
                for e, i in enumerate(indices)
            )
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

    def test_batches(self):
        table = self._test_table
        batches = list(table.to_batches())
        self.assertTrue(len(batches) > 0)
        self.assertIsInstance(batches[0], knt.Table)
        self.assertEqual(table.num_columns, batches[0].num_columns)


class BatchOutputTableTest(unittest.TestCase):
    def _generate_test_batch(self, index):
        nr = 10
        t = pa.Table.from_pydict(
            {
                "Key": [f"Row{r + index * nr}" for r in range(nr)],
                "Ints": [r + index * nr for r in range(nr)],
            }
        )
        return knt.Table.from_pyarrow(t)

    def test_create_append(self):
        out = knt.BatchOutputTable.create()
        for i in range(5):
            out.append(self._generate_test_batch(i))

    def test_create_with_generator(self):
        def batch_generator():
            for i in range(5):
                yield self._generate_test_batch(i)

        out = knt.BatchOutputTable.from_batches(batch_generator())


if __name__ == "__main__":
    unittest.main()
