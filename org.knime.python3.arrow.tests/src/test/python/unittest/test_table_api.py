import unittest
import knime_table as kt
import knime_arrow_table as kat
import knime_arrow_types as katy
import pandas as pd
import pyarrow as pa
import numpy as np


class BatchTest(unittest.TestCase):
    def setUp(self):
        kt._backend = kat.ArrowBackend(None)

    def test_from_pandas(self):
        df = pd.DataFrame()
        df["0"] = [1, 2, 3, 4]
        df["1"] = [1.0, 2.0, 3.0, 4.0]
        b = kt.batch(df)
        self.assertEqual(len(df.columns) + 1, b.num_columns)  # row key is added
        self.assertEqual(len(df), b.num_rows)
        self.assertEqual((len(df), len(df.columns) + 1), b.shape)

    def test_pandas_rowkey_coversion(self):
        # create batch from pyarrow
        d = {
            "<Row Key>": ["r0", "r1", "r2", "r3"],
            "0": [1, 2, 3, 4],
            "1": [1.0, 2.0, 3.0, 4.0],
        }
        rb = pa.RecordBatch.from_pydict(d)
        b = kt.batch(rb)

        # convert to pandas
        df = b.to_pandas()
        self.assertEqual(len(df.columns) + 1, b.num_columns)  # row key column vanishes
        self.assertEqual(len(df), b.num_rows)

        b2 = kt.batch(df)
        self.assertEqual(b.num_columns, b2.num_columns)  # row key column is added back
        self.assertEqual(b.num_rows, b2.num_rows)
        self.assertEqual(b.shape, b2.shape)
        self.assertEqual(b.to_pyarrow().to_pydict(), b2.to_pyarrow().to_pydict())

    def test_from_pyarrow(self):
        d = {
            "RowKey": ["Row1", "Row2", "Row3", "Row4"],
            "0": [1, 2, 3, 4],
            "1": [1.0, 2.0, 3.0, 4.0],
        }
        rb = pa.RecordBatch.from_pydict(d)
        b = kt.batch(rb)
        self.assertEqual(len(d), b.num_columns)
        self.assertEqual(len(d["0"]), b.num_rows)
        self.assertEqual((len(d["0"]), len(d)), b.shape)

    def test_row_selection(self):
        d = {
            "RowKey": ["Row1", "Row2", "Row3", "Row4"],
            "0": [1, 2, 3, 4],
            "1": [1.0, 2.0, 3.0, 4.0],
        }
        rb = pa.RecordBatch.from_pydict(d)
        b = kt.batch(rb)

        self.assertTrue(isinstance(b[:], kt.SlicedDataView))

        # test max row selection
        out = b[:2].to_pyarrow()
        self.assertEqual(3, len(out.columns))
        self.assertEqual(2, len(out))
        self.assertEqual(1, out["0"][0].as_py())
        self.assertEqual(2, out["0"][1].as_py())

        # test range row selection
        out = b[1:3].to_pyarrow()
        self.assertEqual(3, len(out.columns))
        self.assertEqual(2, len(out))
        self.assertEqual(2, out["0"][0].as_py())
        self.assertEqual(3, out["0"][1].as_py())

        # test range row selection with None as end
        out = b[2:].to_pyarrow()
        self.assertEqual(3, len(out.columns))
        self.assertEqual(2, len(out))
        self.assertEqual(3, out["0"][0].as_py())
        self.assertEqual(4, out["0"][1].as_py())

        # test range row selection with None,None
        out = b[:].to_pyarrow()
        self.assertEqual(3, len(out.columns))
        self.assertEqual(4, len(out))

        # test invalid row selection raises
        with self.assertRaises(TypeError):
            b["foo"].to_pyarrow()

    def test_column_selection(self):
        d = {
            "RowKey": ["Row1", "Row2", "Row3", "Row4"],
            "0": [1, 2, 3, 4],
            "1": [1.0, 2.0, 3.0, 4.0],
            "2": ["a", "b", "c", "d"],
        }
        rb = pa.RecordBatch.from_pydict(d)
        b = kt.batch(rb)

        # test individual column selection
        out = b[:, [2]].to_pyarrow()
        self.assertEqual(1, len(out.columns))
        self.assertEqual(4, len(out))
        with self.assertRaises(KeyError):
            print(out["0"])
        self.assertEqual(1.0, out["1"][0].as_py())

        # test list column selection
        out = b[:, [3, 1]].to_pyarrow()
        self.assertEqual(2, len(out.columns))
        self.assertEqual(4, len(out))
        self.assertEqual(["2", "0"], out.schema.names)
        with self.assertRaises(KeyError):
            print(out["1"])
        self.assertEqual(1, out["0"][0].as_py())

        # test range column selection
        out = b[:, 1:2].to_pyarrow()
        self.assertEqual(1, len(out.columns))
        self.assertEqual(4, len(out))
        with self.assertRaises(KeyError):
            print(out["2"])
        self.assertEqual(1, out["0"][0].as_py())
        self.assertEqual(2, out["0"][1].as_py())

        # test range row selection with None as start
        out = b[:, :3].to_pyarrow()
        self.assertEqual(3, len(out.columns))
        self.assertEqual(4, len(out))
        with self.assertRaises(KeyError):
            print(out["2"])
        self.assertEqual(1, out["0"][0].as_py())
        self.assertEqual(2, out["0"][1].as_py())

        # test range row selection with None as end
        out = b[:, 2:].to_pyarrow()
        self.assertEqual(2, len(out.columns))
        self.assertEqual(4, len(out))
        with self.assertRaises(KeyError):
            print(out["0"])
        self.assertEqual(1.0, out[0][0].as_py())

        # test range column selection with None,None
        out = b[:, :].to_pyarrow()
        self.assertEqual(4, len(out.columns))
        self.assertEqual(4, len(out))

        # test selection by name
        out = b[:, ["0", "2"]].to_pyarrow()
        self.assertEqual(2, len(out.columns))
        self.assertEqual(4, len(out))
        with self.assertRaises(KeyError):
            print(out["1"])
        self.assertEqual("2", out.schema.names[1])

        # test invalid column selection raises
        with self.assertRaises(TypeError):
            b[:, "foo"].to_pyarrow()

        with self.assertRaises(IndexError):
            b[:, ["foo"]].to_pyarrow()


class MockSingleBatchDataSource:
    def __init__(self, data_dict):
        self._table = pa.Table.from_pydict(data_dict)
        self._batch = pa.RecordBatch.from_pydict(data_dict)

    @property
    def schema(self):
        return self._table.schema

    def close(self):
        pass

    @property
    def num_rows(self):
        return len(self._table)

    def __len__(self):
        return 1

    def __getitem__(self, idx):
        if idx != 0:
            raise KeyError()
        return self._batch

    def to_arrow_table(self):
        return self._table


class TableTest(unittest.TestCase):
    def test_row_selection(self):
        d = {"0": [1, 2, 3, 4], "1": [1.0, 2.0, 3.0, 4.0]}
        b = kat.ArrowReadTable(MockSingleBatchDataSource(d))

        self.assertTrue(isinstance(b[:], kt.SlicedDataView))

        # test max row selection
        out = b[:2].to_pyarrow()
        self.assertEqual(2, len(out.columns))
        self.assertEqual(2, len(out))
        self.assertEqual(1, out["0"][0].as_py())
        self.assertEqual(2, out["0"][1].as_py())

        # test range row selection
        out = b[1:3].to_pyarrow()
        self.assertEqual(2, len(out.columns))
        self.assertEqual(2, len(out))
        self.assertEqual(2, out["0"][0].as_py())
        self.assertEqual(3, out["0"][1].as_py())

        # test range row selection with None as end
        out = b[2:].to_pyarrow()
        self.assertEqual(2, len(out.columns))
        self.assertEqual(2, len(out))
        self.assertEqual(3, out["0"][0].as_py())
        self.assertEqual(4, out["0"][1].as_py())

        # test range row selection with None,None
        out = b[:].to_pyarrow()
        self.assertEqual(2, len(out.columns))
        self.assertEqual(4, len(out))

        # test invalid row selection raises
        with self.assertRaises(TypeError):
            b["foo"].to_pyarrow()

    def test_column_selection(self):
        d = {"0": [1, 2, 3, 4], "1": [1.0, 2.0, 3.0, 4.0], "2": ["a", "b", "c", "d"]}
        b = kat.ArrowReadTable(MockSingleBatchDataSource(d))

        self.assertTrue(isinstance(b[:, :], kt.SlicedDataView))

        # test individual column selection
        out = b[:, [1]].to_pyarrow()
        self.assertEqual(1, len(out.columns))
        self.assertEqual(4, len(out))
        with self.assertRaises(KeyError):
            print(out["0"])
        self.assertEqual(1.0, out["1"][0].as_py())

        # test list column selection
        out = b[:, [2, 0]].to_pyarrow()
        self.assertEqual(2, len(out.columns))
        self.assertEqual(4, len(out))
        self.assertEqual(["2", "0"], out.schema.names)
        with self.assertRaises(KeyError):
            print(out["1"])
        self.assertEqual(1, out["0"][0].as_py())

        # test range column selection
        out = b[:, 0:1].to_pyarrow()
        self.assertEqual(1, len(out.columns))
        self.assertEqual(4, len(out))
        with self.assertRaises(KeyError):
            print(out["2"])
        self.assertEqual(1, out["0"][0].as_py())
        self.assertEqual(2, out["0"][1].as_py())

        # test range row selection with None as start
        out = b[:, :2].to_pyarrow()
        self.assertEqual(2, len(out.columns))
        self.assertEqual(4, len(out))
        with self.assertRaises(KeyError):
            print(out["2"])
        self.assertEqual(1, out["0"][0].as_py())
        self.assertEqual(2, out["0"][1].as_py())

        # test range row selection with None as end
        out = b[:, 1:].to_pyarrow()
        self.assertEqual(2, len(out.columns))
        self.assertEqual(4, len(out))
        with self.assertRaises(KeyError):
            print(out["0"])
        self.assertEqual(1.0, out[0][0].as_py())

        # test range column selection with None,None
        out = b[:, :].to_pyarrow()
        self.assertEqual(3, len(out.columns))
        self.assertEqual(4, len(out))

        # test selection by name
        out = b[:, ["0", "2"]].to_pyarrow()
        self.assertEqual(2, len(out.columns))
        self.assertEqual(4, len(out))
        with self.assertRaises(KeyError):
            print(out["1"])
        self.assertEqual("2", out.schema.names[1])

        # test invalid column selection raises
        with self.assertRaises(TypeError):
            b[:, "foo"].to_pyarrow()

        with self.assertRaises(IndexError):
            b[:, ["foo"]].to_pyarrow()


class SentinelReplacementTest(unittest.TestCase):
    def test_batch_replacement(self):
        d = {"0": [None, 2, 3, 4], "1": [1.0, 2.0, None, 4.0]}
        rb = pa.RecordBatch.from_pydict(d)

        for s in ["min", "max", 42]:
            expected = s
            if s == "min":
                expected = -9223372036854775808
            elif s == "max":
                expected = 9223372036854775807

            out = katy.insert_sentinel_for_missing_values(rb, s)
            self.assertTrue(out["0"][0].is_valid)
            self.assertEqual(expected, out["0"][0].as_py())
            self.assertFalse(out["1"][2].is_valid)
            roundtrip = katy.sentinel_to_missing_value(out, s)
            self.assertEqual(
                rb,
                roundtrip,
                f"\nBatches not equal after roundtrip with sentinel {s}: \n\t{rb.to_pydict()}\n\t{roundtrip.to_pydict()}",
            )

    def test_batch_replacement_api(self):
        kt._backend = kat.ArrowBackend(None)
        d = {
            "RowKey": ["Row1", "Row2", "Row3", "Row4"],
            "0": [None, 2, 3, 4],
            "1": [1.0, 2.0, None, 4.0],
        }
        rb = pa.RecordBatch.from_pydict(d)
        self.assertEqual(pa.int64(), rb.schema[1].type)
        b = kt.batch(rb)

        for s in ["min", "max", 42]:
            out = b.to_pyarrow(sentinel=s)
            expected = s
            if s == "min":
                expected = -9223372036854775808
            elif s == "max":
                expected = 9223372036854775807

            self.assertTrue(out["0"][0].is_valid)
            self.assertEqual(expected, out["0"][0].as_py())
            self.assertFalse(out["1"][2].is_valid)

            roundtrip_batch = kt.batch(out, sentinel=s)
            roundtrip_batch_pa = roundtrip_batch.to_pyarrow()
            self.assertEqual(pa.int64(), roundtrip_batch_pa.schema[1].type)
            roundtrip_dict = roundtrip_batch_pa.to_pydict()
            self.assertEqual(d, roundtrip_dict)

    def test_batch_replacement_api_pandas(self):
        df = pd.DataFrame()
        df["0"] = [1, 2, 3, 4]
        df["1"] = [1.0, 2.0, np.nan, 4.0]
        df["2"] = [[1, 2, 3], [0, 3, 2], [6, 5, 4], [4, 3, 2]]
        b = kt.batch(df, sentinel=1)
        p = b.to_pyarrow()
        d = p.to_pydict()
        self.assertEqual(pa.int64(), p.schema[1].type)
        self.assertFalse(p["0"][0].is_valid)

        for s in ["min", "max", 42]:
            out = b.to_pandas(sentinel=s)
            expected = s
            if s == "min":
                expected = -9223372036854775808
            elif s == "max":
                expected = 9223372036854775807

            self.assertTrue(np.isfinite(out["0"][0]), f"sentinel={s}")
            self.assertEqual(expected, out["0"][0], f"sentinel={s}")
            self.assertFalse(np.isfinite(out["1"][2]), f"sentinel={s}")

            roundtrip_batch = kt.batch(out, sentinel=s)
            roundtrip_batch_pa = roundtrip_batch.to_pyarrow()
            roundtrip_dict = roundtrip_batch_pa.to_pydict()
            self.assertEqual(
                pa.int64(), roundtrip_batch_pa.schema[1].type
            )  # is a logical type because from_pandas wraps known types
            self.assertEqual(d, roundtrip_dict)

    def test_table_replacement(self):
        d = {"0": [1, 2, None, 4], "1": [1.0, 2.0, 3.0, 4.0]}
        rb = pa.Table.from_pydict(d)
        self.assertFalse(rb["0"][2].is_valid)
        out = katy.insert_sentinel_for_missing_values(rb, 42)
        self.assertTrue(out["0"][2].is_valid)
        self.assertEqual(42, out["0"][2].as_py())

    def test_special_list_type(self):
        s = pd.Series([[1, 2, 3], [0, 3, 2], [6, 5, 4], [4, 3, 2]])
        a = pa.Array.from_pandas(s)
        t = katy.LogicalTypeExtensionType(
            None, pa.list_(pa.int64()), "java_value_factory"
        )
        b = pa.ExtensionArray.from_storage(t, a)


class FixedSizeListTest(unittest.TestCase):
    def test_fixed_size_list(self):
        l = [0, 1, 2]
        fsl = kt._FixedSizeListView(l, "test")

        self.assertEqual(len(l), len(fsl))

        for i, v in enumerate(fsl):
            self.assertEqual(v, l[i])

        with self.assertRaises(AttributeError):
            fsl.append(3)

        l.append(3)
        self.assertEqual(len(l), len(fsl))

    def test_string_repr(self):
        l = []
        fsl = kt._FixedSizeListView(l, "test")
        self.assertEqual("0 tests: []", str(fsl))

        l.append(0)
        self.assertEqual("1 test: [0]", str(fsl))

        l.append(1)
        l.append(2)
        l.append(3)
        self.assertEqual("4 tests: [0, 1, 2, 3]", str(fsl))


class EmptyTableCreationTest(unittest.TestCase):
    def test_create_empty_table(self):
        with self.assertRaises(ValueError):
            t = kat.ArrowWriteTable(None, None)

    def test_create_table_from_empty_df(self):
        df = pd.DataFrame()
        t = kat.ArrowWriteTable(None, df)


class PandasToPyArrowConversionTest(unittest.TestCase):
    def test_primitive_type_wrapping(self):
        df = pd.DataFrame(
            {
                "RowKey": ["Row1", "Row2", "Row3", "Row4"],
                "ints": [0, 1, 2, 3],
                "strings": ["a", "b", "c", "d"],
                "doubles": [1.2, 2.3, 3.4, 4.5],
            }
        )
        raw_t = pa.Table.from_pandas(df)
        self.assertEqual(pa.int64(), raw_t.schema[1].type)
        self.assertEqual(pa.string(), raw_t.schema[2].type)
        self.assertEqual(pa.float64(), raw_t.schema[3].type)
        wrapped_t = katy.wrap_primitive_arrays(raw_t)
        self.assertTrue(katy.is_value_factory_type(wrapped_t.schema[1].type))
        self.assertTrue(katy.is_value_factory_type(wrapped_t.schema[2].type))
        self.assertTrue(katy.is_value_factory_type(wrapped_t.schema[3].type))
        self.assertEqual(pa.int64(), wrapped_t.schema[1].type.storage_type)
        self.assertEqual(pa.string(), wrapped_t.schema[2].type.storage_type)
        self.assertEqual(pa.float64(), wrapped_t.schema[3].type.storage_type)

    def test_primitive_list_type_wrapping(self):
        df = pd.DataFrame(
            {
                "RowKey": ["Row1", "Row2"],
                "ints": [[0, 1, 2, 3], [1, 2, 3, 4]],
                "strings": [["a", "b", "c", "d"], ["b", "c", "d", "e"]],
                "doubles": [[1.2, 2.3, 3.4, 4.5], [2.3, 3.4, 4.5, 5.6]],
            }
        )
        raw_t = pa.Table.from_pandas(df)
        self.assertEqual(pa.list_(pa.int64()), raw_t.schema[1].type)
        self.assertEqual(pa.list_(pa.string()), raw_t.schema[2].type)
        self.assertEqual(pa.list_(pa.float64()), raw_t.schema[3].type)
        wrapped_t = katy.wrap_primitive_arrays(raw_t)
        self.assertTrue(katy.is_value_factory_type(wrapped_t.schema[1].type))
        self.assertTrue(katy.is_value_factory_type(wrapped_t.schema[2].type))
        self.assertTrue(katy.is_value_factory_type(wrapped_t.schema[3].type))
        self.assertEqual(pa.list_(pa.int64()), wrapped_t.schema[1].type.storage_type)
        self.assertEqual(pa.list_(pa.string()), wrapped_t.schema[2].type.storage_type)
        self.assertEqual(pa.list_(pa.float64()), wrapped_t.schema[3].type.storage_type)

    def test_null_type_wrapping(self):
        df = pd.DataFrame(
            {
                "RowKey": ["Row1", "Row2", "Row3", "Row4"],
                "missing": [None, None, None, None],
                "missingList": [[None, None], [None, None, None], None, [None, None]],
            }
        )
        raw_t = pa.Table.from_pandas(df)
        self.assertEqual(pa.null(), raw_t.schema[1].type)
        self.assertEqual(pa.list_(pa.null()), raw_t.schema[2].type)
        wrapped_t = katy.wrap_primitive_arrays(raw_t)
        self.assertTrue(katy.is_value_factory_type(wrapped_t.schema[1].type))
        self.assertTrue(katy.is_value_factory_type(wrapped_t.schema[2].type))
        self.assertEqual(pa.null(), wrapped_t.schema[1].type.storage_type)
        self.assertTrue(katy.is_list_type(wrapped_t.schema[2].type.storage_type))
        self.assertTrue(
            katy.is_value_factory_type(wrapped_t.schema[2].type.storage_type.value_type)
        )
        self.assertEqual(
            pa.null(), wrapped_t.schema[2].type.storage_type.value_type.storage_type
        )
        self.assertFalse(wrapped_t[2][2].is_valid)
        self.assertTrue(wrapped_t[2][3].is_valid)


if __name__ == "__main__":
    unittest.main()
