import tempfile
import unittest
import pyarrow as pa
import pandas as pd
import numpy as np

import knime._arrow._types as katy
import knime.api.table as kt
import knime.api.schema as ks

import testing_utility


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
        self.assertEqual(pa.string(), wrapped_t.schema[1].type.storage_type)
        self.assertTrue(katy.is_list_type(wrapped_t.schema[2].type.storage_type))
        self.assertTrue(
            katy.is_value_factory_type(wrapped_t.schema[2].type.storage_type.value_type)
        )
        self.assertEqual(
            pa.string(), wrapped_t.schema[2].type.storage_type.value_type.storage_type
        )
        self.assertFalse(wrapped_t[2][2].is_valid)
        self.assertTrue(wrapped_t[2][3].is_valid)

    def test_null_type_wrapping_save_load(self):
        df = pd.DataFrame(
            {
                "RowKey": ["Row1", "Row2", "Row3", "Row4"],
                "missing": [None, None, None, None],
                "strings": ["a", "b", "c", "d"],
            }
        )
        wrapped_t = katy.wrap_primitive_arrays(pa.Table.from_pandas(df))

        read_t = _read_write_ipc(wrapped_t)

        self.assertEqual(df["missing"].array, read_t[1].to_pylist())
        self.assertEqual(df["strings"].array, read_t[2].to_pylist())

    def test_list_of_null_type_wrapping_save_load(self):
        df = pd.DataFrame(
            {
                "RowKey": ["Row1", "Row2", "Row3", "Row4"],
                "missing": [[None, None], [None, None, None], None, [None, None]],
                "strings": ["a", "b", "c", "d"],
            }
        )
        wrapped_t = katy.wrap_primitive_arrays(pa.Table.from_pandas(df))

        read_t = _read_write_ipc(wrapped_t)

        self.assertEqual(df["missing"].array, read_t[1].to_pylist())
        self.assertEqual(df["strings"].array, read_t[2].to_pylist())


def _read_write_ipc(input_table):
    with tempfile.TemporaryFile() as tmpfile:
        with pa.ipc.new_file(tmpfile, input_table.schema) as writer:
            writer.write_table(input_table)

        tmpfile.seek(0)

        with pa.ipc.open_file(tmpfile) as reader:
            return reader.read_all()


class NullColumnTypeTest(unittest.TestCase):
    def setUp(self):
        _, new = testing_utility._generate_backends()
        kt._backend = new

    def test_null_type_in_table_schema(self):
        # create a table with nulls in a column
        df = pd.DataFrame({"Nulls": [None, None, None], "Ints": [1, 2, 3]})

        # create KNIME table
        t = kt.Table.from_pandas(df)

        # make sure the column has type VoidValueFactory with storage type string
        internal_table = t._get_table()
        self.assertEqual(3, len(internal_table.columns))

        self.assertTrue(katy.is_value_factory_type(internal_table[1].type))
        self.assertEqual(pa.string(), internal_table[1].type.storage_type)
        self.assertEqual(
            katy._arrow_to_knime_primitive_types[pa.null()],
            internal_table[1].type.logical_type,
        )

        self.assertTrue(katy.is_value_factory_type(internal_table[2].type))
        self.assertEqual(pa.int64(), internal_table[2].type.storage_type)
        self.assertEqual(
            katy._arrow_to_knime_primitive_types[pa.int64()],
            internal_table[2].type.logical_type,
        )

        # make sure KNIME schema reports ks.null()
        s = t.schema
        self.assertEqual(ks.null(), s[0].ktype)
        self.assertEqual(ks.int64(), s[1].ktype)

        # test that schema serialization roundtrip works
        data = s.serialize()
        s2 = ks.Schema.deserialize(data)
        self.assertEqual(s, s2)

        # convert to pyarrow table, check that it's pa.null()
        pat = t.to_pyarrow()
        self.assertEqual(pa.string(), pat[0].type)  # rowID
        self.assertEqual(pa.null(), pat[1].type)
        self.assertEqual(pa.int64(), pat[2].type)

        # in pandas it is simply dtype "object"
        df2 = t.to_pandas()
        self.assertEqual(np.dtype("object"), df2["Nulls"].dtype)
        df2.reset_index(drop=True, inplace=True)
        self.assertFalse(np.any(df2["Nulls"]))
        self.assertTrue(np.all(df["Ints"] == df2["Ints"]))

    def test_list_of_null_type_in_table_schema(self):
        # create a table with list of nulls in a column
        df = pd.DataFrame({"Nulls": [[None, None], None, [None]], "Ints": [1, 2, 3]})

        # create KNIME table
        t = kt.Table.from_pandas(df)

        # make sure the column has type list of VoidValueFactory with storage type string
        internal_table = t._get_table()
        self.assertEqual(3, len(internal_table.columns))

        self.assertTrue(katy.is_value_factory_type(internal_table[1].type))
        self.assertTrue(isinstance(internal_table[1].type.storage_type, pa.ListType))
        self.assertTrue(
            katy.is_value_factory_type(internal_table[1].type.storage_type.value_type)
        )
        self.assertEqual(
            pa.string(), internal_table[1].type.storage_type.value_type.storage_type
        )
        self.assertEqual(
            katy._arrow_to_knime_list_type,
            internal_table[1].type.logical_type,
        )
        self.assertEqual(
            katy._arrow_to_knime_primitive_types[pa.null()],
            internal_table[1].type.storage_type.value_type.logical_type,
        )

        self.assertTrue(katy.is_value_factory_type(internal_table[2].type))
        self.assertEqual(pa.int64(), internal_table[2].type.storage_type)
        self.assertEqual(
            katy._arrow_to_knime_primitive_types[pa.int64()],
            internal_table[2].type.logical_type,
        )

        # make sure KNIME schema reports List of ks.null()
        s = t.schema
        self.assertEqual(ks.list_(ks.null()), s[0].ktype)
        self.assertEqual(ks.int64(), s[1].ktype)

        # test that schema serialization roundtrip works
        data = s.serialize()
        s2 = ks.Schema.deserialize(data)
        self.assertEqual(s, s2)

        # convert to pyarrow table, check that it's list of pa.null()
        pat = t.to_pyarrow()
        self.assertEqual(pa.string(), pat[0].type)  # rowID
        self.assertEqual(pa.list_(pa.null()), pat[1].type)
        self.assertEqual(pa.int64(), pat[2].type)

        # in pandas it is simply dtype "object"
        df2 = t.to_pandas()
        self.assertEqual(np.dtype("object"), df2["Nulls"].dtype)
        df2.reset_index(drop=True, inplace=True)
        self.assertTrue(np.all([None, None] == df2["Nulls"][0]))
        self.assertIsNone(df2["Nulls"][1])
        self.assertEqual([None], df2["Nulls"][2])
        self.assertTrue(np.all(df["Ints"] == df2["Ints"]))


if __name__ == "__main__":
    unittest.main()
