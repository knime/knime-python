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
        b = kt.Batch.from_pandas(df)
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
        b = kt.Batch.from_pyarrow(rb)

        # convert to pandas
        df = b.to_pandas()
        self.assertEqual(len(df.columns) + 1, b.num_columns)  # row key column vanishes
        self.assertEqual(len(df), b.num_rows)

        b2 = kt.Batch.from_pandas(df)
        self.assertEqual(b.num_columns, b2.num_columns)  # row key column is added back
        self.assertEqual(b.num_rows, b2.num_rows)
        self.assertEqual(b.shape, b2.shape)
        self.assertEqual(b.to_pyarrow().to_pydict(), b2.to_pyarrow().to_pydict())

    def test_from_pyarrow(self):
        d = {"0": [1, 2, 3, 4], "1": [1.0, 2.0, 3.0, 4.0]}
        rb = pa.RecordBatch.from_pydict(d)
        b = kt.Batch.from_pyarrow(rb)
        self.assertEqual(len(d), b.num_columns)
        self.assertEqual(len(d["0"]), b.num_rows)
        self.assertEqual((len(d["0"]), len(d)), b.shape)

    def test_row_selection(self):
        d = {"0": [1, 2, 3, 4], "1": [1.0, 2.0, 3.0, 4.0]}
        rb = pa.RecordBatch.from_pydict(d)
        b = kt.Batch.from_pyarrow(rb)

        # test max row selection
        out = b.to_pyarrow(rows=2)
        self.assertEqual(2, len(out.columns))
        self.assertEqual(2, len(out))
        self.assertEqual(1, out["0"][0].as_py())
        self.assertEqual(2, out["0"][1].as_py())

        # test range row selection
        out = b.to_pyarrow(rows=(1, 3))
        self.assertEqual(2, len(out.columns))
        self.assertEqual(2, len(out))
        self.assertEqual(2, out["0"][0].as_py())
        self.assertEqual(3, out["0"][1].as_py())

        # test range row selection with None as start
        out = b.to_pyarrow(rows=(None, 2))
        self.assertEqual(2, len(out.columns))
        self.assertEqual(2, len(out))
        self.assertEqual(1, out["0"][0].as_py())
        self.assertEqual(2, out["0"][1].as_py())

        # test range row selection with None as end
        out = b.to_pyarrow(rows=(2, None))
        self.assertEqual(2, len(out.columns))
        self.assertEqual(2, len(out))
        self.assertEqual(3, out["0"][0].as_py())
        self.assertEqual(4, out["0"][1].as_py())

        # test range row selection with None,None
        out = b.to_pyarrow(rows=(None, None))
        self.assertEqual(2, len(out.columns))
        self.assertEqual(4, len(out))

        # test invalid row selection raises
        with self.assertRaises(IndexError):
            b.to_pyarrow(rows="foo")

    def test_column_selection(self):
        d = {"0": [1, 2, 3, 4], "1": [1.0, 2.0, 3.0, 4.0], "2": ["a", "b", "c", "d"]}
        rb = pa.RecordBatch.from_pydict(d)
        b = kt.Batch.from_pyarrow(rb)

        # test individual column selection
        out = b.to_pyarrow(columns=[1])
        self.assertEqual(1, len(out.columns))
        self.assertEqual(4, len(out))
        with self.assertRaises(KeyError):
            print(out["0"])
        self.assertEqual(1.0, out["1"][0].as_py())

        # test list column selection
        out = b.to_pyarrow(columns=[2, 0])
        self.assertEqual(2, len(out.columns))
        self.assertEqual(4, len(out))
        self.assertEqual(["2", "0"], out.schema.names)
        with self.assertRaises(KeyError):
            print(out["1"])
        self.assertEqual(1, out["0"][0].as_py())

        # test range column selection
        out = b.to_pyarrow(columns=(0, 1))
        self.assertEqual(1, len(out.columns))
        self.assertEqual(4, len(out))
        with self.assertRaises(KeyError):
            print(out["2"])
        self.assertEqual(1, out["0"][0].as_py())
        self.assertEqual(2, out["0"][1].as_py())

        # test range row selection with None as start
        out = b.to_pyarrow(columns=(None, 2))
        self.assertEqual(2, len(out.columns))
        self.assertEqual(4, len(out))
        with self.assertRaises(KeyError):
            print(out["2"])
        self.assertEqual(1, out["0"][0].as_py())
        self.assertEqual(2, out["0"][1].as_py())

        # test range row selection with None as end
        out = b.to_pyarrow(columns=(1, None))
        self.assertEqual(2, len(out.columns))
        self.assertEqual(4, len(out))
        with self.assertRaises(KeyError):
            print(out["0"])
        self.assertEqual(1.0, out[0][0].as_py())

        # test range column selection with None,None
        out = b.to_pyarrow(columns=(None, None))
        self.assertEqual(3, len(out.columns))
        self.assertEqual(4, len(out))

        # test selection by name
        out = b.to_pyarrow(columns=["0", "2"])
        self.assertEqual(2, len(out.columns))
        self.assertEqual(4, len(out))
        with self.assertRaises(KeyError):
            print(out["1"])
        self.assertEqual("2", out.schema.names[1])

        # test invalid row selection raises
        with self.assertRaises(IndexError):
            b.to_pyarrow(columns="foo")

        with self.assertRaises(IndexError):
            b.to_pyarrow(columns=["foo"])


# TODO: test table by mocking data sink


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
        d = {"0": [None, 2, 3, 4], "1": [1.0, 2.0, None, 4.0]}
        rb = pa.RecordBatch.from_pydict(d)
        self.assertEqual(pa.int64(), rb.schema[0].type)
        b = kt.Batch.from_pyarrow(rb)

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
            self.assertEqual(pa.int64(), roundtrip_batch_pa.schema[0].type)
            roundtrip_dict = roundtrip_batch_pa.to_pydict()
            self.assertEqual(d, roundtrip_dict)

    def test_batch_replacement_api_pandas(self):
        df = pd.DataFrame()
        df["0"] = [1, 2, 3, 4]
        df["1"] = [1.0, 2.0, np.nan, 4.0]
        b = kt.Batch.from_pandas(df, sentinel=1)
        p = b.to_pyarrow()
        d = p.to_pydict()
        self.assertEqual(pa.int64(), p.schema[1].type.storage_type)
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
                pa.int64(), roundtrip_batch_pa.schema[1].type.storage_type
            )  # is a logical type because from_pandas wraps known types
            self.assertEqual(d, roundtrip_dict)

    def test_table_replacement(self):
        d = {"0": [1, 2, None, 4], "1": [1.0, 2.0, 3.0, 4.0]}
        rb = pa.Table.from_pydict(d)
        self.assertFalse(rb["0"][2].is_valid)
        out = katy.insert_sentinel_for_missing_values(rb, 42)
        self.assertTrue(out["0"][2].is_valid)
        self.assertEqual(42, out["0"][2].as_py())


if __name__ == "__main__":
    unittest.main()
