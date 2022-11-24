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
        tv = t[:1]  # Row Key and first column
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
        other = kt.Table.from_pyarrow(data)
        self.assertEqual(table.num_rows, other.num_rows)
        self.assertEqual(table.num_columns, other.num_columns)
        self.assertEqual(table.column_names, other.column_names)

    def test_to_from_pandas(self):
        table = self._test_table
        data = table.to_pandas()
        other = kt.Table.from_pandas(data)
        self.assertEqual(table.num_rows, other.num_rows)
        self.assertEqual(table.num_columns, other.num_columns)
        self.assertEqual(table.column_names, other.column_names)

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

    def test_row_keys_from_pandas(self):
        df_len = 5

        def create_df(index):
            return pd.DataFrame(
                {
                    "col1": ["a", "b", "c", "d", "e"],
                    "col2": [1, 2, 3, 4, 5],
                },
                index=index,
            )

        def get_row_keys(table):
            return table.to_pyarrow()[0].to_pylist()

        # ========== row_keys = "keep"

        # Simple range index
        table = kt.Table.from_pandas(create_df(pd.RangeIndex(df_len)), row_keys="keep")
        self.assertListEqual(get_row_keys(table), [f"{i}" for i in range(df_len)])

        # Strings
        index = ["a", "b", "foo", "g", "e"]
        table = kt.Table.from_pandas(create_df(pd.Index(index)), row_keys="keep")
        self.assertListEqual(get_row_keys(table), index)

        # Floats
        index = [2.0, 7.1, 1000.1, 0.0001, 3.14]
        table = kt.Table.from_pandas(create_df(pd.Index(index)), row_keys="keep")
        self.assertListEqual(get_row_keys(table), [str(i) for i in index])

        # ========== row_keys = "generate"

        generated_keys = [f"Row{i}" for i in range(df_len)]

        # Simple range index
        table = kt.Table.from_pandas(
            create_df(pd.RangeIndex(df_len)), row_keys="generate"
        )
        self.assertListEqual(get_row_keys(table), generated_keys)

        # Strings
        index = ["a", "b", "foo", "g", "e"]
        table = kt.Table.from_pandas(create_df(pd.Index(index)), row_keys="generate")
        self.assertListEqual(get_row_keys(table), generated_keys)

        # Floats
        index = [2.0, 7.1, 1000.1, 0.0001, 3.14]
        table = kt.Table.from_pandas(create_df(pd.Index(index)), row_keys="generate")
        self.assertListEqual(get_row_keys(table), generated_keys)

        # ========== row_keys = "auto"

        generated_keys = [f"Row{i}" for i in range(df_len)]

        # Simple range index
        table = kt.Table.from_pandas(create_df(pd.RangeIndex(df_len)), row_keys="auto")
        self.assertListEqual(get_row_keys(table), generated_keys)

        # Strings
        index = ["a", "b", "foo", "g", "e"]
        table = kt.Table.from_pandas(create_df(pd.Index(index)), row_keys="auto")
        self.assertListEqual(get_row_keys(table), index)

        # Floats
        index = [2.0, 7.1, 1000.1, 0.0001, 3.14]
        table = kt.Table.from_pandas(create_df(pd.Index(index)), row_keys="auto")
        self.assertListEqual(get_row_keys(table), [str(i) for i in index])

        # Integers
        index = [5, 2, 1, 4, 7]
        table = kt.Table.from_pandas(create_df(pd.Index(index)), row_keys="auto")
        self.assertListEqual(get_row_keys(table), [f"Row{i}" for i in index])

        # Range starting at 10
        index = pd.RangeIndex(10, 10 + df_len)
        table = kt.Table.from_pandas(create_df(index), row_keys="auto")
        self.assertListEqual(
            get_row_keys(table), [f"Row{i}" for i in range(10, 10 + df_len)]
        )

        # ========== row_keys = "unsupported"
        with self.assertRaises(ValueError):
            kt.Table.from_pandas(create_df(None), row_keys="unsupported")

    def test_row_keys_from_pyarrow(self):
        table_len = 5

        def create_table(row_key_col=None):
            columns = [
                pa.array(["a", "b", "c", "d", "e"]),
                pa.array([1, 2, 3, 4, 5]),
            ]
            names = ["col1", "col2"]
            if not row_key_col is None:
                columns = [row_key_col, *columns]
                names = ["<Row Key>", *names]

            return pa.table(columns, names=names)

        def get_row_keys(table):
            return table.to_pyarrow()[0].to_pylist()

        # ========== row_keys = "keep"

        # Correct string col for row keys
        row_keys = ["a", "b", "foo", "g", "e"]
        table = kt.Table.from_pyarrow(create_table(pa.array(row_keys)), row_keys="keep")
        self.assertListEqual(get_row_keys(table), row_keys)
        self.assertEqual(table.num_columns, 2)

        # Use first string column as row keys
        table = kt.Table.from_pyarrow(create_table(), row_keys="keep")
        self.assertListEqual(get_row_keys(table), create_table()[0].to_pylist())
        self.assertEqual(table.num_columns, 1)

        # Column named "<Row Key>" but with a wrong type
        row_keys = [4, 1, 3, 2, 8]
        with self.assertRaises(TypeError):
            kt.Table.from_pyarrow(create_table(pa.array(row_keys)), row_keys="keep")

        # ========== row_keys = "generate"

        # Simple: Create new row keys
        table = kt.Table.from_pyarrow(create_table(), row_keys="generate")
        self.assertListEqual(get_row_keys(table), [f"Row{i}" for i in range(table_len)])
        self.assertEqual(table.num_columns, 2)

        # Row key column present but generate another one
        row_keys = ["a", "b", "foo", "g", "e"]
        table = kt.Table.from_pyarrow(
            create_table(pa.array(row_keys)), row_keys="generate"
        )
        self.assertListEqual(get_row_keys(table), [f"Row{i}" for i in range(table_len)])
        self.assertEqual(table.num_columns, 3)

        # ========== row_keys = "auto"

        # First column is string but not "<Row Key>"
        table = kt.Table.from_pyarrow(create_table(), row_keys="auto")
        self.assertListEqual(get_row_keys(table), [f"Row{i}" for i in range(table_len)])
        self.assertEqual(table.num_columns, 2)

        # First column is string and "<Row Key>"
        row_keys = ["a", "b", "foo", "g", "e"]
        table = kt.Table.from_pyarrow(create_table(pa.array(row_keys)), row_keys="auto")
        self.assertListEqual(get_row_keys(table), row_keys)
        self.assertEqual(table.num_columns, 2)

        # First column is integer and "<Row Key>"
        row_keys = [4, 1, 3, 2, 8]
        table = kt.Table.from_pyarrow(create_table(pa.array(row_keys)), row_keys="auto")
        self.assertListEqual(get_row_keys(table), [f"Row{i}" for i in row_keys])
        self.assertEqual(table.num_columns, 2)

        # First column cannot converted to row keys but is named "<Row Key>"
        # TODO is this what we want? Keep the column and generate a new row key column?
        row_keys = [2.0, 7.1, 1000.1, 0.0001, 3.14]
        table = kt.Table.from_pyarrow(create_table(pa.array(row_keys)), row_keys="auto")
        self.assertListEqual(get_row_keys(table), [f"Row{i}" for i in range(table_len)])
        self.assertEqual(table.num_columns, 3)

        # ========== row_keys = "unsupported"
        with self.assertRaises(ValueError):
            kt.Table.from_pyarrow(create_table(), row_keys="unsupported")


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

    def _generate_test_batch(self, index, row_keys="auto"):
        return kt.Table.from_pyarrow(
            self._generate_test_pyarrow_table(index), row_keys=row_keys
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

    def test_row_keys(self):
        def assert_last_batch(batch_table, row_keys, num_cols):
            written_batch = batch_table._sink.last_data
            self.assertListEqual(written_batch[0].to_pylist(), row_keys)
            # NB: batch.num_columns also counts the row key column
            self.assertEqual(written_batch.num_columns - 1, num_cols)

        # ========== row_keys = "generate"
        generated_row_keys = [f"Row{i}" for i in range(40)]

        batch_table = kt.BatchOutputTable.create(row_keys="generate")
        batch_table.append(self._generate_test_pyarrow_batch(0))
        assert_last_batch(batch_table, generated_row_keys[:10], 2)
        batch_table.append(self._generate_test_pyarrow_table(1))
        assert_last_batch(batch_table, generated_row_keys[10:20], 2)
        batch_table.append(self._generate_test_pandas(2))
        assert_last_batch(batch_table, generated_row_keys[20:30], 2)
        batch_table.append(self._generate_test_batch(3))
        assert_last_batch(batch_table, generated_row_keys[30:], 2)

        # ========== row_keys = "keep"
        kept_row_keys = [f"MyRow{i}" for i in range(40)]

        # Row keys available for each batch
        batch_table = kt.BatchOutputTable.create(row_keys="keep")
        batch_table.append(self._generate_test_pyarrow_batch(0))
        assert_last_batch(batch_table, kept_row_keys[:10], 1)
        batch_table.append(self._generate_test_pyarrow_table(1))
        assert_last_batch(batch_table, kept_row_keys[10:20], 1)
        batch_table.append(self._generate_test_pandas(2).set_index("Key"))
        assert_last_batch(batch_table, kept_row_keys[20:30], 1)
        batch_table.append(self._generate_test_batch(3, row_keys="keep"))
        assert_last_batch(batch_table, kept_row_keys[30:], 1)

        # No row keys available
        batch_table = kt.BatchOutputTable.create(row_keys="keep")
        with self.assertRaises(TypeError):
            batch_table.append(
                pa.RecordBatch.from_pydict(
                    {
                        "Ints": [i for i in range(10)],
                        "Doubles": [i * 0.1 for i in range(10)],
                    }
                )
            )

        # ========== row_keys = "auto"
        with self.assertRaises(ValueError):
            kt.BatchOutputTable.create(row_keys="auto")


if __name__ == "__main__":
    unittest.main()
