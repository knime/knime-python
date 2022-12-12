import tempfile
import unittest
import pyarrow as pa
import pandas as pd

import knime._arrow._types as katy


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
