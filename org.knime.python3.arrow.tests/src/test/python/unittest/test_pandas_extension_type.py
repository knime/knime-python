from os import pardir
from typing import Type, Union
import unittest
import pandas as pd
import pandas.api.extensions as pdext
from pandas.core.dtypes.dtypes import register_extension_dtype
import pyarrow as pa
import numpy as np


class MyArrowExtType(pa.ExtensionType):
    def __init__(self, storage_type, logical_type):
        self._logical_type = logical_type
        pa.ExtensionType.__init__(self, storage_type, "test.ext_type")

    def __arrow_ext_serialize__(self):
        return self._logical_type.encode()

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        logical_type = serialized.decode()
        return MyArrowExtType(storage_type, logical_type)

    def __arrow_ext_class__(self):
        return MyExtensionArray

    def decode(self, storage):
        return storage

    def encode(self, value):
        return value

    @property
    def logical_type(self):
        return self._logical_type

    def to_pandas_dtype(self):
        return MyPandasExtType(str(self.storage_type), self._logical_type)


class MyExtensionArray(pa.ExtensionArray):
    def __getitem__(self, idx):
        storage_scalar = self.storage[idx]
        return MyExtensionScalar(self.type, storage_scalar)

    def to_pylist(self):
        return [self.type.decode(x) for x in self.storage.to_pylist()]

    def to_pandas(self):
        # TODO use super method and pass through arguments (i.e. essentially decorate the super implementation)
        series = self.storage.to_pandas()
        return series.apply(self.type.decode, convert_dtype=False)

    def to_numpy(self):
        import numpy as np

        # TODO same as for to_pandas
        ndarray = self.storage.to_numpy(zero_copy_only=False)
        # TODO we might need different converters for different libraries
        return np.array([self.type.decode(x) for x in ndarray])


class MyExtensionScalar:
    def __init__(self, ext_type: MyArrowExtType, storage_scalar: pa.Scalar):
        self.ext_type = ext_type
        self.storage_scalar = storage_scalar

    @property
    def type(self):
        return self.ext_type

    @property
    def is_valid(self):
        return self.storage_scalar.is_valid

    def cast(self, target_type):
        """
        Attempts a safe cast to target data type.
        If target_type is the same as this instances type, returns this instance, if it's a different
        KnimeArrowExtensionType a ValueError is raised and if it is something else entirely, we attempt to cast
        it via the storage type.
        """
        if target_type == self.ext_type:
            return self
        else:
            return self.storage_scalar.cast(target_type)

    def __repr__(self):
        return f"test.ext_scalar: {self.as_py()!r}"

    def __str__(self):
        return str(self.as_py())

    def equals(self, other):
        return self.ext_type == other.ext_type and self.storage_scalar.equals(
            other.storage_scalar
        )

    def __eq__(self, other):
        try:
            return self.equals(other)
        except:
            return NotImplemented

    def __reduce__(self):
        return unpickle_knime_extension_scalar, (self.ext_type, self.storage_scalar)

    def as_py(self):
        return self.ext_type.decode(self.storage_scalar.as_py())


def unpickle_knime_extension_scalar(ext_type, storage_scalar):
    return MyExtensionScalar(ext_type, storage_scalar)


pa.register_extension_type(MyArrowExtType(pa.int64(), "foo"))


def _apply_to_array(array, func):
    if isinstance(array, pa.ChunkedArray):
        return pa.chunked_array([func(chunk) for chunk in array.chunks])
    else:
        return func(array)


@register_extension_dtype
class MyPandasExtType(pdext.ExtensionDtype):
    def __init__(self, storage_type_str: str, logical_type: str):
        self._storage_type_str = storage_type_str
        self._logical_type = logical_type
        self._metadata = (
            self._storage_type_str,
            self._logical_type,
        )

    na_value = pd.NA

    def type(self):
        # We just say that this is raw data?! No need to be interpreted
        return bytes

    def name(self):
        return f"MyPandasExtType({self._storage_type_str}, {self._logical_type})"

    def construct_array_type(self):
        return MyPandasExtArray

    def construct_from_string(cls: Type[pdext.ExtensionDtype], string: str):
        # TODO implement this?
        return MyPandasExtType("missing", "missing")

    def __from_arrow__(self, arrow_array):
        return MyPandasExtArray(self._storage_type_str, self._logical_type, arrow_array)

    def __str__(self):
        return f"MyPandasExtType({self._storage_type_str}, {self._logical_type})"


class MyPandasExtArray(pdext.ExtensionArray):
    def __init__(
        self,
        storage_type_str,
        logical_type,
        data: Union[pa.Array, pa.ChunkedArray] = None,
    ):
        self._data = data
        self._storage_type_str = storage_type_str
        self._logical_type = logical_type

    def __arrow_array__(self, type=None):
        return self._data

    @classmethod
    def _from_sequence(cls, scalars, dtype=None, copy=None):
        arrow_type = MyPandasExtType("test", "sample")
        return MyPandasExtArray(pa.array(scalars, type=arrow_type))

    def _from_factorized(self):
        raise NotImplementedError("Cannot be created from factors")

    def __getitem__(self, item):
        if isinstance(item, int):
            return self._data[item].as_py()
        elif isinstance(item, slice):
            # todo: handle stride
            return self._data.slice(item.start, length=item.stop - item.start)
        elif isinstance(item, list):
            # fetch objects at the individual indices
            return self._data.take(item)
        elif isinstance(item, np.ndarray):
            # masked array
            raise NotImplementedError("Cannot index using masked array from numpy yet")

    def __len__(self):
        return len(self._data)

    def __eq__(self, other):
        if not isinstance(other, MyPandasExtArray):
            return False
        return other._data == self._data

    @property
    def dtype(self):
        return MyPandasExtType(self._storage_type_str, self._logical_type)

    @property
    def nbytes(self):
        return self._data.nbytes

    def isna(self):
        return self._data.is_null().to_numpy()

    def take(self, indices, *args, **kwargs) -> "MyPandasExtArray":
        # TODO: handle allow_fill and fill_value kwargs
        arrow_scalars = self._data.take(indices)
        return self._from_sequence(arrow_scalars)

    def _as_pandas_value(self, arrow_scalar: MyExtensionScalar):
        if isinstance(arrow_scalar, MyExtensionScalar):
            # return bytes? or how does that work now
            raise NotImplementedError(
                "Cannot convert MyExtensionScalar to a Pandas Value"
            )
        else:
            return pd.NA

    def copy(self):
        # TODO: do we really want to copy the data? This thing is read only anyways... Unless we implement setitem and concat
        return self

    @classmethod
    def _concat_same_type(cls, to_concat):
        raise NotImplementedError("Need to concat underlying pyarrow arrays")


class PyArrowExtensionTypeTest(unittest.TestCase):
    def _create_test_table(self):
        d = {"test_data": [0, 1, 2, 3, 4], "reference": [0, 1, 2, 3, 4]}
        plain = pa.Table.from_pydict(d)
        columns = plain.columns
        dtype = MyArrowExtType(pa.int64(), "foo")
        columns[0] = _apply_to_array(
            columns[0], lambda a: pa.ExtensionArray.from_storage(dtype, a)
        )
        return pa.Table.from_arrays(columns, names=list(d.keys()))

    def test_create_extension(self):
        t = self._create_test_table()
        reference_schema = pa.schema(
            [
                ("test_data", MyArrowExtType(pa.int64(), "foo")),
                ("reference", pa.int64()),
            ]
        )
        self.assertEqual(reference_schema, t.schema)
        self.assertTrue(
            isinstance(t[0][0].type, MyArrowExtType), msg=f"type was {t[0][0].type}"
        )

    def test_convert_to_pandas(self):
        t = self._create_test_table()
        df = t.to_pandas()
        self.assertTrue("test_data" in df)
        self.assertTrue(isinstance(df["test_data"].dtype, MyPandasExtType))
        print(df["test_data"][0])
        out = pa.Table.from_pandas(df)
        self.assertEqual(t.schema, out.schema)

    def test_wrap_list_of_null_pyarrow_6(self):
        """
        Experiment how we can create a PyArrow list of null array with extension type wrapping.
        """
        try:
            import packaging.version

            if packaging.version.parse(pa.__version__) < packaging.version.parse(
                "6.0.0"
            ):
                pass

            import knime_arrow_types as katy

            df = pd.DataFrame(
                {
                    "missingList": [
                        [None, None],
                        [None, None, None],
                        None,
                        [None, None],
                    ],
                }
            )
            raw_t = pa.Table.from_pandas(df)
            array = raw_t.columns[0].chunks[0]
            self.assertEqual(pa.list_(pa.null()), array.type)
            self.assertEqual(7, len(array.values))
            inner_type = MyArrowExtType(pa.null(), "VoidType")
            outer_type = MyArrowExtType(pa.list_(inner_type), "ListType")

            inner_data = pa.nulls(len(array.values), type=inner_type)
            null_mask = array.is_null().to_pylist() + [False]
            offsets = pa.array(
                array.offsets.to_pylist(), mask=null_mask, type=array.offsets.type
            )
            self.assertEqual(len(offsets), len(array.offsets))
            list_data = katy._create_list_array(offsets, inner_data)
            outer_wrapped = pa.ExtensionArray.from_storage(outer_type, list_data)
            self.assertEqual(outer_type, outer_wrapped.type)
            self.assertTrue(outer_wrapped[0].is_valid)
            self.assertFalse(outer_wrapped[2].is_valid)
        except:
            # test did not run because we don't have the packaging module, but we need that to test for the pyarrow version
            pass

    def test_wrap_list_of_null_pyarrow_5(self):
        """
        Experiment how we can create a PyArrow list of null array with extension type wrapping.
        This is the PyArrow 5 conformal way of doing so, see the method above for PyArrow 6.
        In PyArrow 7 the problem should be gone.
        """
        import knime_arrow_types as katy

        df = pd.DataFrame(
            {
                "missingList": [[None, None], [None, None, None], None, [None, None]],
            }
        )
        raw_t = pa.Table.from_pandas(df)
        array = raw_t.columns[0].chunks[0]
        self.assertEqual(pa.list_(pa.null()), array.type)
        self.assertEqual(7, len(array.values))
        inner_type = MyArrowExtType(pa.null(), "VoidType")
        outer_type = MyArrowExtType(pa.list_(inner_type), "ListType")

        validbits = np.packbits(
            np.ones(len(array.values), dtype=np.uint8), bitorder="little"
        )
        inner_data = pa.Array.from_buffers(
            inner_type,
            len(array.values),
            [pa.py_buffer(validbits)],
            null_count=len(array.values),
        )
        null_mask = np.array(array.is_null().to_pylist() + [False])
        offsets = pa.array(
            array.offsets.to_pylist(), mask=null_mask, type=array.offsets.type
        )
        self.assertEqual(len(offsets), len(array.offsets))
        list_data = katy._create_list_array(offsets, inner_data)
        outer_wrapped = pa.ExtensionArray.from_storage(outer_type, list_data)
        self.assertEqual(outer_type, outer_wrapped.type)
        self.assertTrue(outer_wrapped[0].is_valid)
        self.assertFalse(outer_wrapped[2].is_valid)


if __name__ == "__main__":
    unittest.main()
