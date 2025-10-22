import unittest
import pyarrow as pa
import pandas as pd

import knime_schema as ks
from knime.api.schema import _ColumnSlicingOperation
import knime.api.table as kt
import knime._arrow._table as kat


class TableTest(unittest.TestCase):
    def setUp(self):
        import knime.api.table as ktn

        ktn._backend = kat._ArrowBackend(lambda: DummyDataSink())

    def test_remove(self):
        df = pd.DataFrame()
        df["1"] = [1, 2, 3, 4]
        df["2"] = [5.0, 6.0, 7.0, 8.0]
        t = kt.Table.from_pandas(df)
        _t = t.remove("1")

        # Check Type
        self.assertIsInstance(_t, ks._ColumnarView)
        self.assertIsInstance(t, kt.Table)
        self.assertIsInstance(_t.operation, _ColumnSlicingOperation)
        self.assertIsInstance(_t.delegate, kt.Table)

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
        t = kt.Table.from_pandas(df)
        tv = t[:1]  # Row ID and first column
        self.assertIsInstance(tv, ks._ColumnarView)
        self.assertIsInstance(tv, kt._TabularView)
        self.assertIsInstance(tv.operation, _ColumnSlicingOperation)
        self.assertIsInstance(tv.delegate, kat.ArrowTable)
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
    def __init__(self):
        self.last_data = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def write(self, data):
        self.last_data = data

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
        import knime._arrow._backend as ka

        ads = ka.ArrowDataSource(test_data_source)
        return kat.ArrowSourceTable(ads)

    def generate_test_table_with_rowid_column(self):
        """Creates a KNIME table that has a column named <RowID>"""
        rowid_table = pa.table(
            [
                pa.array([1]),
            ],
            ["<RowID>"],
        )
        return kt.Table.from_pyarrow(rowid_table, row_ids="generate")

    @classmethod
    def setUpClass(cls):
        import knime.api.table as ktn

        ktn._backend = kat._ArrowBackend(lambda: DummyDataSink())
        cls._test_table = cls._generate_test_table()

    def test_table_setup(self):
        table = self._test_table
        self.assertTrue(table.num_columns > 0)
        self.assertTrue(table.num_rows > 0)
        self.assertIsInstance(table, kat.ArrowSourceTable)
        self.assertIsInstance(table, kt.Table)
        self.assertTrue(hasattr(table, "to_batches"))

    def test_table_view(self):
        table = self._test_table
        tv = table[:, :]
        self.assertTrue(tv.num_columns > 0)
        self.assertTrue(tv.num_rows > 0)
        self.assertEqual(table.num_columns, tv.num_columns)
        self.assertEqual(table.num_rows, tv.num_rows)
        self.assertNotIsInstance(tv, kat.ArrowSourceTable)
        self.assertIsInstance(tv, kt._Tabular)
        self.assertIsInstance(tv, kt._TabularView)
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
        self.assertEqual(data.schema.names[0], "<RowID>")
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
        other = kt.Table.from_pyarrow(data)
        self.assertEqual(table.num_rows, other.num_rows)
        self.assertEqual(table.num_columns, other.num_columns)
        self.assertEqual(table.column_names, other.column_names)

        with self.assertRaises(RuntimeError):
            self.generate_test_table_with_rowid_column().to_pyarrow()

    def test_to_from_pandas(self):
        table = self._test_table
        data = table.to_pandas()
        other = kt.Table.from_pandas(data)
        self.assertEqual(table.num_rows, other.num_rows)
        self.assertEqual(table.num_columns, other.num_columns)
        self.assertEqual(table.column_names, other.column_names)

        with self.assertRaises(RuntimeError):
            self.generate_test_table_with_rowid_column().to_pandas()

    def test_pandas_roundtrip_with_data(self):
        """
        This test works up to Py 3.11 and Pandas <2, but fails with
        Py 3.12 and Pandas 2.1
        """
        table = self._test_table
        data = table.to_pandas()
        # if we omit the 2 columns below, the roundtrip works also in Py3.12
        # data.drop(["TimestampCol", "URICol"], axis=1, inplace=True)
        other = kt.Table.from_pandas(data)
        data2 = other.to_pandas()

        pd.testing.assert_frame_equal(data, data2)

    def test_to_batches(self):
        table = self._test_table
        batches = list(table.to_batches())
        self.assertTrue(len(batches) > 0)
        self.assertIsInstance(batches[0], kt.Table)
        self.assertEqual(table.num_columns, batches[0].num_columns)

    def test_batches(self):
        table = self._test_table
        batches = list(table.batches())
        self.assertTrue(len(batches) > 0)
        self.assertIsInstance(batches[0], kt.Table)
        self.assertEqual(table.num_columns, batches[0].num_columns)

    def test_schema(self):
        table = self._test_table
        schema = table.schema
        self.assertEqual(table.num_columns, schema.num_columns)
        self.assertEqual(schema.column_names[0], "StringCol")
        self.assertEqual(schema[3].ktype, ks.int32())

    def test_row_ids_from_pandas(self):
        df_len = 5

        def create_df(index):
            return pd.DataFrame(
                {
                    "col1": ["a", "b", "c", "d", "e"],
                    "col2": [1, 2, 3, 4, 5],
                },
                index=index,
            )

        def get_row_ids(table):
            return table.to_pyarrow()[0].to_pylist()

        # ========== row_ids = "keep"

        # Simple range index
        table = kt.Table.from_pandas(create_df(pd.RangeIndex(df_len)), row_ids="keep")
        self.assertListEqual(get_row_ids(table), [f"{i}" for i in range(df_len)])

        # Strings
        index = ["a", "b", "foo", "g", "e"]
        table = kt.Table.from_pandas(create_df(pd.Index(index)), row_ids="keep")
        self.assertListEqual(get_row_ids(table), index)

        # Floats
        index = [2.0, 7.1, 1000.1, 0.0001, 3.14]
        table = kt.Table.from_pandas(create_df(pd.Index(index)), row_ids="keep")
        self.assertListEqual(get_row_ids(table), [str(i) for i in index])

        # ========== row_ids = "generate"

        generated_keys = [f"Row{i}" for i in range(df_len)]

        # Simple range index
        table = kt.Table.from_pandas(
            create_df(pd.RangeIndex(df_len)), row_ids="generate"
        )
        self.assertListEqual(get_row_ids(table), generated_keys)

        # Strings
        index = ["a", "b", "foo", "g", "e"]
        table = kt.Table.from_pandas(create_df(pd.Index(index)), row_ids="generate")
        self.assertListEqual(get_row_ids(table), generated_keys)

        # Floats
        index = [2.0, 7.1, 1000.1, 0.0001, 3.14]
        table = kt.Table.from_pandas(create_df(pd.Index(index)), row_ids="generate")
        self.assertListEqual(get_row_ids(table), generated_keys)

        # ========== row_ids = "auto"

        generated_keys = [f"Row{i}" for i in range(df_len)]

        # Simple range index
        table = kt.Table.from_pandas(create_df(pd.RangeIndex(df_len)), row_ids="auto")
        self.assertListEqual(get_row_ids(table), generated_keys)

        # Strings
        index = ["a", "b", "foo", "g", "e"]
        table = kt.Table.from_pandas(create_df(pd.Index(index)), row_ids="auto")
        self.assertListEqual(get_row_ids(table), index)

        # Floats
        index = [2.0, 7.1, 1000.1, 0.0001, 3.14]
        table = kt.Table.from_pandas(create_df(pd.Index(index)), row_ids="auto")
        self.assertListEqual(get_row_ids(table), [str(i) for i in index])

        # Integers
        index = [5, 2, 1, 4, 7]
        table = kt.Table.from_pandas(create_df(pd.Index(index)), row_ids="auto")
        self.assertListEqual(get_row_ids(table), [f"Row{i}" for i in index])

        # Range starting at 10
        index = pd.RangeIndex(10, 10 + df_len)
        table = kt.Table.from_pandas(create_df(index), row_ids="auto")
        self.assertListEqual(
            get_row_ids(table), [f"Row{i}" for i in range(10, 10 + df_len)]
        )

        # ========== row_ids = "unsupported"
        with self.assertRaises(ValueError):
            kt.Table.from_pandas(create_df(None), row_ids="unsupported")

    def test_row_ids_from_pyarrow(self):
        table_len = 5

        def create_table(row_key_col=None):
            columns = [
                pa.array(["a", "b", "c", "d", "e"]),
                pa.array([1, 2, 3, 4, 5]),
            ]
            names = ["col1", "col2"]
            if not row_key_col is None:
                columns = [row_key_col, *columns]
                names = ["<RowID>", *names]

            return pa.table(columns, names=names)

        def get_row_ids(table):
            # Remove all columns except for the row ID column to avoid error when <RowID> exists twice
            while len(table.column_names) > 0:
                table = table.remove(0)

            return table.to_pyarrow()[0].to_pylist()

        # ========== row_ids = "keep"

        # Correct string col for RowID
        row_ids = ["a", "b", "foo", "g", "e"]
        table = kt.Table.from_pyarrow(create_table(pa.array(row_ids)), row_ids="keep")
        self.assertListEqual(get_row_ids(table), row_ids)
        self.assertEqual(table.num_columns, 2)

        # Use first string column as RowID
        table = kt.Table.from_pyarrow(create_table(), row_ids="keep")
        self.assertListEqual(get_row_ids(table), create_table()[0].to_pylist())
        self.assertEqual(table.num_columns, 1)

        # Column named "<RowID>" but with a wrong type
        row_ids = [4, 1, 3, 2, 8]
        with self.assertRaises(TypeError):
            kt.Table.from_pyarrow(create_table(pa.array(row_ids)), row_ids="keep")

        # ========== row_ids = "generate"

        # Simple: Create new RowIDs
        table = kt.Table.from_pyarrow(create_table(), row_ids="generate")
        self.assertListEqual(get_row_ids(table), [f"Row{i}" for i in range(table_len)])
        self.assertEqual(table.num_columns, 2)

        # RowID column present but generate another one
        row_ids = ["a", "b", "foo", "g", "e"]
        table = kt.Table.from_pyarrow(
            create_table(pa.array(row_ids)), row_ids="generate"
        )
        self.assertListEqual(get_row_ids(table), [f"Row{i}" for i in range(table_len)])
        self.assertEqual(table.num_columns, 3)

        # ========== row_ids = "auto"

        # First column is string but not "<RowID>"
        table = kt.Table.from_pyarrow(create_table(), row_ids="auto")
        self.assertListEqual(get_row_ids(table), [f"Row{i}" for i in range(table_len)])
        self.assertEqual(table.num_columns, 2)

        # First column is string and "<RowID>"
        row_ids = ["a", "b", "foo", "g", "e"]
        table = kt.Table.from_pyarrow(create_table(pa.array(row_ids)), row_ids="auto")
        self.assertListEqual(get_row_ids(table), row_ids)
        self.assertEqual(table.num_columns, 2)

        # First column is integer and "<RowID>"
        row_ids = [4, 1, 3, 2, 8]
        table = kt.Table.from_pyarrow(create_table(pa.array(row_ids)), row_ids="auto")
        self.assertListEqual(get_row_ids(table), [f"Row{i}" for i in row_ids])
        self.assertEqual(table.num_columns, 2)

        # First column cannot converted to RowID but is named "<RowID>"
        row_ids = [2.0, 7.1, 1000.1, 0.0001, 3.14]
        table = kt.Table.from_pyarrow(create_table(pa.array(row_ids)), row_ids="auto")
        self.assertListEqual(get_row_ids(table), [f"Row{i}" for i in range(table_len)])
        self.assertEqual(table.num_columns, 3)

        # ========== row_ids = "unsupported"
        with self.assertRaises(ValueError):
            kt.Table.from_pyarrow(create_table(), row_ids="unsupported")

    def test_constant_batch_size_check(self):
        def _create_pyarrow_table(batch_sizes):
            return pa.Table.from_batches(
                [
                    pa.record_batch(data=[pa.array(list(range(size)))], names=["data"])
                    for size in batch_sizes
                ]
            )

        # Constant batch sizes + last batch smaller - should work
        knime_table = kt.Table.from_pyarrow(_create_pyarrow_table([10, 10, 7]))
        self.assertEqual(knime_table.num_rows, 27)
        self.assertEqual(len(knime_table._table.to_batches()), 3)

        # Non-constant batch sizes - should fail
        self.assertRaises(
            ValueError,
            lambda: kt.Table.from_pyarrow(_create_pyarrow_table([10, 10, 12, 10])),
        )

        # Last batch bigger than the other batches - should fail
        self.assertRaises(
            ValueError,
            lambda: kt.Table.from_pyarrow(_create_pyarrow_table([10, 10, 12])),
        )


class BatchOutputTableTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import knime.api.table as ktn

        ktn._backend = kat._ArrowBackend(lambda: DummyDataSink())

    @classmethod
    def tearDownClass(cls):
        import knime.api.table as ktn

        ktn._backend.close()
        kt._backend = None

    def _generate_test_pyarrow_table(self, index):
        nr = 10
        return pa.Table.from_pydict(
            {
                "Key": [f"MyRow{r + index * nr}" for r in range(nr)],
                "Ints": [r + index * nr for r in range(nr)],
            }
        )

    def _generate_test_pyarrow_batch(self, index):
        nr = 10
        return pa.RecordBatch.from_pydict(
            {
                "Key": [f"MyRow{r + index * nr}" for r in range(nr)],
                "Ints": [r + index * nr for r in range(nr)],
            }
        )

    def _generate_test_pandas(self, index):
        nr = 10
        return pd.DataFrame(
            {
                "Key": [f"MyRow{r + index * nr}" for r in range(nr)],
                "Ints": [r + index * nr for r in range(nr)],
            }
        )

    def _generate_test_batch(self, index, row_ids="auto"):
        return kt.Table.from_pyarrow(
            self._generate_test_pyarrow_table(index), row_ids=row_ids
        )

    def test_create_append(self):
        out = kt.BatchOutputTable.create()
        for i in range(5):
            out.append(self._generate_test_batch(i))

    def test_append_pandas(self):
        out = kt.BatchOutputTable.create()
        for i in range(5):
            out.append(self._generate_test_pandas(i))

    def test_append_pyarrow_table(self):
        out = kt.BatchOutputTable.create()
        for i in range(5):
            out.append(self._generate_test_pyarrow_table(i))

    def test_append_pyarrow_batch(self):
        out = kt.BatchOutputTable.create()
        for i in range(5):
            out.append(self._generate_test_pyarrow_batch(i))

    def test_append_wrong_type(self):
        out = kt.BatchOutputTable.create()
        with self.assertRaises(TypeError):
            out.append([1, 2, 3, 4])

    def test_append_mixed(self):
        out = kt.BatchOutputTable.create()
        out.append(self._generate_test_pyarrow_table(0))
        out.append(self._generate_test_pandas(1))
        out.append(self._generate_test_pyarrow_batch(2))
        out.append(self._generate_test_batch(3))

    def test_create_with_generator(self):
        def batch_generator():
            for i in range(5):
                yield self._generate_test_batch(i)

        out = kt.BatchOutputTable.from_batches(batch_generator())

    def test_row_ids(self):
        def assert_last_batch(batch_table, row_ids, num_cols):
            written_batch = batch_table._sink.last_data
            self.assertListEqual(written_batch[0].to_pylist(), row_ids)
            # NB: batch.num_columns also counts the RowID column
            self.assertEqual(written_batch.num_columns - 1, num_cols)

        # ========== row_ids = "generate"
        generated_row_ids = [f"Row{i}" for i in range(40)]

        batch_table = kt.BatchOutputTable.create(row_ids="generate")
        batch_table.append(self._generate_test_pyarrow_batch(0))
        assert_last_batch(batch_table, generated_row_ids[:10], 2)
        batch_table.append(self._generate_test_pyarrow_table(1))
        assert_last_batch(batch_table, generated_row_ids[10:20], 2)
        batch_table.append(self._generate_test_pandas(2))
        assert_last_batch(batch_table, generated_row_ids[20:30], 2)
        batch_table.append(self._generate_test_batch(3))
        assert_last_batch(batch_table, generated_row_ids[30:], 2)

        # ========== row_ids = "keep"
        kept_row_ids = [f"MyRow{i}" for i in range(40)]

        # RowID available for each batch
        batch_table = kt.BatchOutputTable.create(row_ids="keep")
        batch_table.append(self._generate_test_pyarrow_batch(0))
        assert_last_batch(batch_table, kept_row_ids[:10], 1)
        batch_table.append(self._generate_test_pyarrow_table(1))
        assert_last_batch(batch_table, kept_row_ids[10:20], 1)
        batch_table.append(self._generate_test_pandas(2).set_index("Key"))
        assert_last_batch(batch_table, kept_row_ids[20:30], 1)
        batch_table.append(self._generate_test_batch(3, row_ids="keep"))
        assert_last_batch(batch_table, kept_row_ids[30:], 1)

        # No RowID available
        batch_table = kt.BatchOutputTable.create(row_ids="keep")
        with self.assertRaises(TypeError):
            batch_table.append(
                pa.RecordBatch.from_pydict(
                    {
                        "Ints": [i for i in range(10)],
                        "Doubles": [i * 0.1 for i in range(10)],
                    }
                )
            )

        # ========== row_ids = "auto"
        with self.assertRaises(ValueError):
            kt.BatchOutputTable.create(row_ids="auto")


if __name__ == "__main__":
    unittest.main()
