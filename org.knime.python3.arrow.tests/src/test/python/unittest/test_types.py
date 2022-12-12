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

    def test_null_type_wrapping_save_load(self):
        df = pd.DataFrame(
            {
                "RowKey": ["Row1", "Row2", "Row3", "Row4"],
                "missing": [None, None, None, None],
                "missingList": [[None, None], [None, None, None], None, [None, None]],
                "strings": ["a", "b", "c", "d"],
            }
        )
        wrapped_t = katy.wrap_primitive_arrays(pa.Table.from_pandas(df))

        with tempfile.TemporaryFile() as tmpfile:
            with pa.ipc.new_file(tmpfile, wrapped_t.schema) as writer:
                writer.write_table(wrapped_t)

            tmpfile.seek(0)

            with pa.ipc.open_file(tmpfile) as reader:
                read_t = reader.read_all()

        ######################################
        # START - CODE FOR SHOWING THE PROBLEM
        ######################################

        def print_buffers(array):
            print(
                [
                    b.to_pybytes() if b is not None else None
                    for b in array.chunks[0].buffers()
                ]
            )

        print("==== BEFORE READ ====")
        print("Buffers for missing:")
        print_buffers(wrapped_t[1])
        print("Buffers for missingList:")
        print_buffers(wrapped_t[2])
        print("Buffers for strings:")
        print_buffers(wrapped_t[3])

        # After reading the buffers are assined incorrectly to the arrays
        print()
        print("==== AFTER READ ====")
        print("Buffers for missing:")
        print_buffers(read_t[1])
        print("Buffers for missingList:")
        print_buffers(read_t[2])
        print("Buffers for strings:")
        print_buffers(read_t[3])

        ####################################
        # END - CODE FOR SHOWING THE PROBLEM
        ####################################

        self.assertEqual(df["missing"].array, read_t[1].to_pylist())
        self.assertEqual(df["missingList"].array, read_t[2].to_pylist())
        self.assertEqual(df["strings"].array, read_t[3].to_pylist())


if __name__ == "__main__":
    unittest.main()
