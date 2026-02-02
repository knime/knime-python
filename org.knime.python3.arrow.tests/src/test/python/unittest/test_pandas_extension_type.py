# ------------------------------------------------------------------------
#  Copyright by KNIME AG, Zurich, Switzerland
#  Website: http://www.knime.com; Email: contact@knime.com
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License, Version 3, as
#  published by the Free Software Foundation.
#
#  This program is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, see <http://www.gnu.org/licenses>.
#
#  Additional permission under GNU GPL version 3 section 7:
#
#  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
#  Hence, KNIME and ECLIPSE are both independent programs and are not
#  derived from each other. Should, however, the interpretation of the
#  GNU GPL Version 3 ("License") under any applicable laws result in
#  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
#  you the additional permission to use and propagate KNIME together with
#  ECLIPSE with only the license terms in place for ECLIPSE applying to
#  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
#  license terms of ECLIPSE themselves allow for the respective use and
#  propagation of ECLIPSE together with KNIME.
#
#  Additional permission relating to nodes for KNIME that extend the Node
#  Extension (and in particular that are based on subclasses of NodeModel,
#  NodeDialog, and NodeView) and that only interoperate with KNIME through
#  standard APIs ("Nodes"):
#  Nodes are deemed to be separate and independent programs and to not be
#  covered works.  Notwithstanding anything to the contrary in the
#  License, the License does not apply to Nodes, you are not required to
#  license Nodes under the License, and you are granted a license to
#  prepare and propagate Nodes, in each case even if such Nodes are
#  propagated with or for interoperation with KNIME.  The owner of a Node
#  may freely choose the license terms applicable to such Node, including
#  when such Node is propagated with or for interoperation with KNIME.
# ------------------------------------------------------------------------

"""
@author Jonas Klotz, KNIME GmbH, Berlin, Germany
"""

import os
import unittest
from typing import Type, Union
import knime.types.builtin as et  # import to register column converters

import numpy as np
import pandas as pd
import pandas.api.extensions as pdext
import pyarrow as pa
from pandas.core.dtypes.dtypes import register_extension_dtype

import knime._arrow._backend as knar
import knime._arrow._pandas as kap
import knime._arrow._types as katy
import knime.scripting._deprecated._arrow_table as kat
import knime_node_arrow_table as knat
import knime._arrow._dictencoding as kasde
import knime.api.schema as ks
import knime.api.types as kt

from testing_utility import (
    DummyJavaDataSink,
    DummyWriter,
    DummyJavaDataSinkFactory,
    TestDataSource,
    DummyConverter,
    _generate_test_data_frame,
    _apply_to_array,
    _register_extension_types,
    _generate_arrow_table,
)


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

    def to_pylist(self, maps_as_pydicts=None):
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

    def as_py(self, maps_as_pydicts=None):
        return self.ext_type.decode(self.storage_scalar.as_py())


def unpickle_knime_extension_scalar(ext_type, storage_scalar):
    return MyExtensionScalar(ext_type, storage_scalar)


pa.register_extension_type(MyArrowExtType(pa.int64(), "foo"))


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

    @property
    def type(self):
        # We just say that this is raw data?! No need to be interpreted
        return bytes

    @property
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
    def _from_sequence(
        cls,
        scalars,
        dtype=None,
        copy=None,
        storage_type=None,
        logical_type=None,
    ):
        if scalars is None:
            raise ValueError("Cannot create MyPandasExtArray from empty data")

        # easy case
        if isinstance(scalars, pa.Array) or isinstance(scalars, pa.ChunkedArray):
            if not isinstance(scalars.type, MyArrowExtType):
                raise ValueError(
                    "MyPandasExtArray must be backed by MyArrowExtType values"
                )
            return MyPandasExtArray(
                scalars.type.storage_type,
                scalars.type.logical_type,
                scalars,
            )

        if isinstance(dtype, MyPandasExtType):
            # in this case we can extract storage, logical_type and converter
            storage_type = dtype._storage_type
            logical_type = dtype._logical_type

        if storage_type is None:
            raise ValueError(
                "Can only create MyPandasExtArray from a sequence if the storage type is given."
            )

        # needed for pandas ExtensionArray API
        arrow_type = MyArrowExtType(storage_type, logical_type)

        a = pa.array(scalars, type=storage_type)
        extension_array = pa.ExtensionArray.from_storage(arrow_type, a)
        return MyPandasExtArray(storage_type, logical_type, extension_array)

    def _from_factorized(self):
        raise NotImplementedError("Cannot be created from factors")

    def __getitem__(self, item):
        if isinstance(item, int):
            return self._data[item].as_py()
        elif isinstance(item, slice):
            (start, stop, step) = item.indices(len(self._data))

            indices = list(range(start, stop, step))
            return self.take(indices)
        elif isinstance(item, list):
            # fetch objects at the individual indices
            return self.take(item)
        elif isinstance(item, np.ndarray):
            # masked array
            raise NotImplementedError("Cannot index using masked array from numpy yet.")

    def __setitem__(self, item, value):
        def _set_data_from_input(inp: Union[list, np.ndarray]):
            an_arr = pa.array(inp)
            an_arr = _apply_to_array(
                an_arr,
                lambda a: MyPandasExtArray(
                    self._storage_type_str, self._logical_type, a
                ),
            )
            self._data = MyPandasExtArray(
                self._storage_type_str, self._logical_type, an_arr
            )._data

        tmp_list = self._data.to_pylist()  # convert immutable data to mutable list
        if isinstance(item, int):
            tmp_list[item] = value
            _set_data_from_input(tmp_list)

        elif isinstance(item, slice):
            (start, stop, step) = item.indices(len(self._data))
            for i in range(len(value)):
                tmp_list[start] = value[i].as_py()
                start += step
            _set_data_from_input(tmp_list)

        elif isinstance(item, list):
            # "This is only reachable from knime side"
            tmp_arr = np.asarray(tmp_list)
            tmp_indices = np.asarray(item)
            tmp_values = np.asarray(value)
            tmp_arr[tmp_indices] = tmp_values
            _set_data_from_input(tmp_arr)

        elif isinstance(item, np.ndarray):
            # masked array
            # panda converts all set queries with lists as indices to np.ndarrays
            tmp_arr = np.asarray(tmp_list)
            tmp_arr[item] = value
            _set_data_from_input(tmp_arr)

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
    _TEST_TABLE_COLUMNS = [
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

            import knime._arrow._types as katy

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
        import knime._arrow._types as katy

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

    def test_complicated_setitem_in_pandas(self):
        # loads a table with all knime extension types
        _register_extension_types()

        df = _generate_test_data_frame(
            file_name="generatedTestData.zip",
            columns=self._TEST_TABLE_COLUMNS,
            lists=False,
            sets=False,
        )

        # These extension types are not registered, thus still saved as dict. They would only work with at indexing.
        dict_columns = ["TimestampCol", "URICol", "Period"]
        df.drop(dict_columns, axis=1, inplace=True)  # remove all dicts
        df.reset_index(inplace=True, drop=True)  # drop index as it messes up equality

        original_df = df.copy(deep=True)
        df.loc[1, lambda dfu: [df.columns[0]]] = df.loc[2, lambda dfu: [df.columns[0]]]
        df = original_df  # reset to original df
        # test single item setting with int index for all columns
        for col_key in df.columns:
            col_index = df.columns.get_loc(col_key)
            df.iloc[1, col_index] = df.iloc[2, col_index]  # test iloc
            df.loc[1, col_key] = df.loc[2, col_key]

        self.assertTrue(df.iloc[1].equals(df.iloc[2]), msg="The rows are not equal")
        df = original_df  # reset to original df
        # test slice setting
        for col_key in df.columns:
            col_index = df.columns.get_loc(col_key)
            df.iloc[:3, col_index] = df.iloc[3:6, col_index]
            df.loc[:3, col_key] = df.loc[3:6, col_key]

        self.assertTrue(df.iloc[0].equals(df.iloc[2]), msg="The rows are not equal")
        df = original_df  # reset to original df
        # test slice broadcasting
        for col_key in df.columns:
            col_index = df.columns.get_loc(col_key)
            df.iloc[:6, col_index] = df.iloc[6, col_index]
            df.loc[:6, col_key] = df.loc[6, col_key]

        self.assertTrue(df.iloc[0].equals(df.iloc[6]), msg="The rows are not equal")
        df = original_df  # reset to original df
        # test a weird case of loc list setting, where the left values are overwritten with N/A value
        for col_key in df.columns:
            col_index = df.columns.get_loc(col_key)
            df.loc[[1, 2], col_key] = df.loc[[5, 6], col_key]
            if isinstance(
                df.loc[[5], col_key].dtype,
                kap.PandasLogicalTypeExtensionType,
            ):
                n_type = df.loc[[5], col_key].dtype.na_value
                self.assertTrue(df.iloc[1, col_index] == n_type)

        for col_key in df.columns:
            col_index = df.columns.get_loc(col_key)
            df.iloc[[1, 2], col_index] = df.iloc[[5, 6], col_index]

        self.assertTrue(df.iloc[1].equals(df.iloc[6]), msg="The rows are not equal")
        df = original_df  # reset to original df

        index_arr = np.arange(7)
        # test a weird case of loc np-arr setting, where the left values are overwritten with N/A value
        for col_key in df.columns:
            df.loc[index_arr, col_key] = df.loc[(index_arr + 7), col_key]
            col_index = df.columns.get_loc(col_key)
            if isinstance(
                df.loc[[10], col_key].dtype,
                kap.PandasLogicalTypeExtensionType,
            ):
                n_type = df.loc[[10], col_key].dtype.na_value

                self.assertTrue(df.iloc[1, col_index] == n_type)

        # test np arr setting
        for col_key in df.columns:
            col_index = df.columns.get_loc(col_key)
            df.iloc[index_arr, col_index] = df.iloc[(index_arr + 7), col_index]
        self.assertTrue(df.iloc[2].equals(df.iloc[9]), msg="The rows are not equal")
        df = original_df  # reset to original df
        # older pandas versions cast missing values NA to NaN thus the equality check fails
        df.drop("MissingValStringCol", axis=1, inplace=True)
        df = pd.concat([df, df.iloc[2].to_frame().T])
        self.assertTrue(df.iloc[2].equals(df.iloc[-1]))

    def test_complicated_setitem_in_pandas2(self):
        _register_extension_types()

        # Test setting for dict encoded values
        df = _generate_test_data_frame("DictEncString.zip", columns=["Name"])
        df.reset_index(inplace=True, drop=True)  # drop index as it messes up equality
        df.loc[1, "Name"] = df.loc[3, "Name"]
        self.assertTrue(df.loc[1, "Name"] == df.loc[3, "Name"])

        # here is a bug -> maybe a separate ticket?

        # dtype = df.dtypes["Name"]
        # col = df["Name"]
        # wrong_dtype = col.values._data.type
        # # struct dict enc logical type != struct dict encoded
        # self.assertTrue(dtype == wrong_dtype)
        # row1 = df.iloc[1]
        # row3 = df.iloc[3]
        # self.assertTrue(row1.equals(row3), msg="The rows are not equal")

    def test_complicated_setitem_in_pandas_with_lists_and_sets(self):
        """Test complicated setitem in pandas with lists and sets.
        As lists cannot be set properly with iloc, we use at and loc.
        """

        _register_extension_types()

        df = _generate_test_data_frame(
            file_name="generatedTestData.zip",
            columns=self._TEST_TABLE_COLUMNS,
            lists=True,
            sets=True,
        )

        df.reset_index(inplace=True, drop=True)  # drop index as it messes up equality
        df = df.replace({np.nan: None, pd.NA: None})
        original_df = df.copy(deep=True)

        # test single item setting with int index for all columns
        for col_key in df.columns:
            col_index = df.columns.get_loc(col_key)
            item = df.iloc[1, col_index]
            df.at[3, col_key] = item
        self.assertTrue(df.iloc[1].equals(df.iloc[3]), msg="The rows are not equal")
        df = original_df  # reset to original df

        # test slice setting
        for col_key in df.columns:
            col_index = df.columns.get_loc(col_key)
            iloc_val = df.iloc[3:6, col_index]
            df.iloc[:3, col_index] = iloc_val

        self.assertTrue(df.iloc[0].equals(df.iloc[3]), msg="The rows are not equal")
        df = original_df  # reset to original df

        for col_key in df.columns:
            col_index = df.columns.get_loc(col_key)
            value = df.iloc[[5, 6], col_index]
            df.iloc[[1, 2], col_index] = value
        self.assertTrue(df.iloc[1].equals(df.iloc[5]), msg="The rows are not equal")
        df = original_df  # reset to original df

        index_arr = np.arange(7)
        # test np arr setting
        for col_key in df.columns:
            col_index = df.columns.get_loc(col_key)
            value = df.iloc[(index_arr + 7), col_index]
            df.iloc[index_arr, col_index] = value
        self.assertTrue(df.iloc[2].equals(df.iloc[9]), msg="The rows are not equal")
        df = original_df  # reset to original df

        df = pd.concat([df, df.iloc[2].to_frame().T])
        self.assertTrue(df.iloc[2].equals(df.iloc[-1]))

    def test_append_sets_lists_2(self):
        df = _generate_test_data_frame(
            file_name="generatedTestData.zip",
            columns=self._TEST_TABLE_COLUMNS,
            lists=True,
            sets=True,
        )
        _register_extension_types()

        # These extension types are not registered, thus still saved as dict. They would only work with at indexing.
        dict_columns = ["TimestampCol", "URICol", "Period"]
        # df.drop(dict_columns, axis=1, inplace=True)  # remove all dicts
        df.reset_index(inplace=True, drop=True)  # drop index as it messes up equality

        with DummyJavaDataSinkFactory(49) as sink_creator:
            backend = kat.ArrowBackend(sink_creator)
            t = backend.batch_write_table()

            mid = int(len(df) / 2)
            df1 = df[:mid]
            df2 = df[mid:]
            # Create batch write table, fill it with batches
            t.append(df1)
            t.append(df2)
            backend.close()

    def test_send_timestamp_to_knime(self):
        """
        This Testcase creates a pandas dataframe containing pandas timestamps and sends this timestamps to knime.
        If the pandas timestamp is not handled correctly this test should fail by throwing an exception.
        As this tests the KNIME communication from the python side, and therefore the function
        :func:`~ka.ArrowDataSink.write_table` thats why the assertion is just looking at the table result.

        """
        # Setup
        _register_extension_types()

        with DummyJavaDataSinkFactory(5) as sink_creator:
            arrow_backend = kat.ArrowBackend(sink_creator)

            # Create table
            rng = pd.date_range("2015-02-24", periods=5e5, freq="s")
            df = pd.DataFrame({"Date": rng[:5], "Val": np.random.randn(len(rng[:5]))})

            A = arrow_backend.write_table(df)
            self.assertEqual(
                "<class 'knime.scripting._deprecated._arrow_table.ArrowWriteTable'>",
                str(type(A)),
            )

            import knime.api.schema as ks

            self.assertEqual(
                str(
                    ks.LogicalType(
                        '{"value_factory_class":"org.knime.core.data.v2.time.LocalDateTimeValueFactory"}',
                        ks.struct(ks.int64(), ks.int64()),
                    )
                ),
                str(knat._convert_arrow_schema_to_knime(A._schema)[0].ktype),
            )

    def test_timestamp_columns(self):
        """
        This test tests the conversion of a dict encoded KNIME timestamp from KNIME to python and back to KNIME.
        Currently, the dict representation of timestamps on the python side is not working properly. This can be
        reproduced in the test by readding the outcommented line in the test.
        """
        with DummyJavaDataSinkFactory(98) as sink_creator:
            arrow_backend = kat.ArrowBackend(sink_creator)

            df = _generate_test_data_frame(
                file_name="generatedTestData.zip",
                columns=self._TEST_TABLE_COLUMNS,
                lists=False,
                sets=False,
            )

            # currently, it does not work for lists, sets and dicts
            wrong_cols = [
                "StringCol",
                "IntCol",
                "LongCol",
                "DoubleCol",
                "BooleanCol",
                "URICol",
                "MissingValStringCol",
                "LongStringColumnName",
                "LongDoubleColumnName",
                "Local Date",
                "Local Time",
                "Local Date Time",
                "Zoned Date Time",
                "Period",
                "Duration",
            ]
            df.drop(wrong_cols, axis=1, inplace=True)  # remove all dicts
            df.reset_index(
                inplace=True, drop=True
            )  # drop index as it messes up equality

            arrow_table = arrow_backend.write_table(df)
            knime_ts_ext_str = "Timestamp"

            self.assertEqual(
                "<class 'knime.scripting._deprecated._arrow_table.ArrowWriteTable'>",
                str(type(arrow_table)),
            )
            self.assertEqual(
                knime_ts_ext_str,
                str(knat._convert_arrow_schema_to_knime(arrow_table._schema)[0].ktype),
            )

    def test_lists_with_missing_values(self):
        """
        Tests if list extensiontypes can handle missing values
        @return:
        """
        with DummyJavaDataSinkFactory(49) as sink_creator:
            backend = kat.ArrowBackend(sink_creator)
            t = backend.batch_write_table()

            # Create table

            df = _generate_test_data_frame(
                file_name="generatedTestData.zip",
                columns=self._TEST_TABLE_COLUMNS,
                lists=True,
                sets=True,
            )

            remove_cols = [
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
                "TimestampCol",
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

            df.drop(remove_cols, axis=1, inplace=True)
            df.reset_index(
                inplace=True, drop=True
            )  # drop index as it messes up equality

            # Slice into two dfs which we will use as batches
            mid = int(len(df) / 2)
            df1 = df[:mid]
            # actually here the null value gets replaced by a list
            df2 = df[mid:]

            # Create batch write table, fill it with batches
            t.append(df1, sentinel="min")
            t.append(df2, sentinel="min")

            self.assertEqual(
                "<class 'knime.scripting._deprecated._arrow_table.ArrowBatchWriteTable'>",
                str(type(t)),
            )
            backend.close()

    def test_dict_encoding_extension_array(self):
        """
        Test for a simple Dict Encoded Extension Array
        """
        conv = DummyConverter()
        # what is the order, extension array -> dict encode
        array = ["foo", "bar", "foo", "car", "foo", "bar", "foo"]

        struct_dict_enc_type = kasde.StructDictEncodedType(pa.string())
        dict_enc_storage_array = kasde.create_storage_for_struct_dict_encoded_array(
            pa.array(array), kasde.DictKeyGenerator()
        )
        dtype = katy.LogicalTypeExtensionType(conv, pa.string(), "java_value_factory")
        decoded_dtype = katy.StructDictEncodedLogicalTypeExtensionType(
            dtype, struct_dict_enc_type
        )
        array = _apply_to_array(
            dict_enc_storage_array,
            lambda a: pa.ExtensionArray.from_storage(decoded_dtype, a),
        )

        self.assertEqual(array[2].as_py(), "foo")
        self.assertEqual(array[3].as_py(), "car")

    def test_double_dict_encoded_extension_array(self):
        """
        Test for a dict encoded array nested in a struct array
        """
        conv = DummyConverter()

        # create dict encoded array
        array = ["foo", "bar", "foo", "car", "foo", "bar", "foo"]
        struct_dict_enc_type = kasde.StructDictEncodedType(pa.string())
        dict_enc_storage_array = kasde.create_storage_for_struct_dict_encoded_array(
            pa.array(array), kasde.DictKeyGenerator()
        )
        dict_enc_array = _apply_to_array(
            dict_enc_storage_array,
            lambda a: pa.ExtensionArray.from_storage(struct_dict_enc_type, a),
        )

        # combine a simple int array with our dict encoded array to created nested array
        double_structed = pa.StructArray.from_arrays(
            [pa.array([i for i in range(len(array))]), dict_enc_array], names=["0", "1"]
        )
        combined_storage_type = pa.struct(
            [
                pa.field("0", pa.int64()),
                pa.field("1", kasde.StructDictEncodedType(pa.string())),
            ]
        )
        dtype = katy.LogicalTypeExtensionType(
            conv, combined_storage_type, "java_value_factory"
        )
        complex_array = _apply_to_array(
            double_structed, lambda a: pa.ExtensionArray.from_storage(dtype, a)
        )

        self.assertEqual(complex_array[2].as_py(), {"0": 2, "1": "foo"})
        self.assertEqual(complex_array[3].as_py(), {"0": 3, "1": "car"})

    def test_struct_dict_encoded_logical_type_extension_type(self):
        # tests the usage of StructDictEncodedLogicalTypeExtensionType for dict decoded strings
        # the type is not used yet anywhere else but in this test

        df = _generate_test_data_frame("DictEncString.zip", columns=["Name"])
        self.assertEqual(df["Name"].iloc[0], "LINESTRING (30 10, 10 30, 40 40)")
        self.assertEqual(df["Name"].iloc[4], "POINT (30 10)")
        self.assertEqual(df["Name"].iloc[5], "LINESTRING (40 20, 10 30, 35 40)")
        self.assertEqual(df["Name"].iloc[6], "LINESTRING (30 10, 10 30, 40 40)")

    def test_struct_dict_encoding_with_chunks_regression(self):
        """
        Regression test for struct dict encoded array corruption during round-trip conversion.

        Bug Context:
        KNIME uses struct dict encoding to compress data: values are stored only on first
        occurrence in a struct {key: uint, value: T}, with subsequent occurrences referencing
        them by key. This creates an encoded array where keys point to indices in the data
        array, and the data array contains actual values at those indices.

        The Problem:
        In pandas 2.1.0+, pd.concat changed and calls KnimePandasExtensionArray.take with indices
        re-batches ChunkedArrays at different boundaries. When an already-encoded struct dict
        array gets split incorrectly:
        - Chunk 1 might contain data array with actual values at indices [0,1,2...]
        - Chunk 2 gets keys [0,0,0...] referencing index 0, but chunk 2's data array has
          null at index 0 (the actual value is in chunk 1)

        This causes reads to fail with "Cannot read DataCell with empty type information"
        because the dictionary structure is broken across chunks.

        What This Test Does:
        1. Loads structDictEncodedDataCellsWithBatches.zip which
           - has 3 batches
           - contains a struct dict encoded column for generic data cells
        2. Performs arrow → pandas → arrow round-trip conversion
        3. Asserts all columns remain equal after round-trip

        This was fixed by shortcutting taking indices from the storage array in KnimePandasExtensionArray.take if
        the indices cover the full array with the lines:
        ```
        if len(indices) == len(self) and np.all(indices == np.arange(len(self))):
            return self.copy()
        ```
        """
        arrow_table = _generate_arrow_table("structDictEncodedDataCellsWithBatches.zip")
        df = kap.arrow_data_to_pandas_df(arrow_table)
        arrow_table_2 = kap.pandas_df_to_arrow(df)

        self.assertEqual(len(arrow_table_2.columns), 2)
        self.assertEqual(arrow_table.column(0), arrow_table_2.column(0))
        self.assertEqual(arrow_table.column(1), arrow_table_2.column(1))

    def test_chunk_calculation(self):
        def _get_chunked_array_for_start_indices(chunk_start_indices):
            chunk_list = []
            for i in range(len(chunk_start_indices) - 1):
                tmp = chunk_start_indices[i]
                nxt = chunk_start_indices[i + 1]
                chunk_list.append([tmp] * (nxt - tmp))
            chunk_list.append([chunk_start_indices[-1]])
            chunked_array = pa.chunked_array(chunk_list)
            return chunked_array

        # power 2 chunks
        correct_chunk_start_indices = [0] + [2**i for i in range(16)]
        chunked_array = _get_chunked_array_for_start_indices(
            correct_chunk_start_indices
        )
        calc = katy._get_all_chunk_start_indices(chunked_array)
        self.assertEqual(correct_chunk_start_indices, calc)

        # random chunks
        correct_chunk_start_indices = [0, 3, 5, 23, 123, 250]
        chunked_array = _get_chunked_array_for_start_indices(
            correct_chunk_start_indices
        )
        calc = katy._get_all_chunk_start_indices(chunked_array)
        self.assertEqual(correct_chunk_start_indices, calc)

        # equal size chunks
        correct_chunk_start_indices = [30 * i for i in range(11)]
        chunked_array = _get_chunked_array_for_start_indices(
            correct_chunk_start_indices
        )
        calc = katy._get_all_chunk_start_indices(chunked_array)
        self.assertEqual(correct_chunk_start_indices, calc)

    def test_categorical_types(self):
        with DummyJavaDataSinkFactory(4) as sink_creator:
            arrow_backend = kat.ArrowBackend(sink_creator)

            # Create table
            df = pd.DataFrame({"A": ["a", "b", "c", "d"]})
            df["B"] = df["A"].astype("category")
            raw_cat = pd.Categorical(
                ["a", "b", "c", "a"], categories=["b", "c", "d"], ordered=False
            )
            df["C"] = pd.Series(raw_cat)
            A = arrow_backend.write_table(df)
            self.assertEqual(str(A.column_names), "['<RowID>', 'A', 'B', 'C']")
            # check if categorical columns where converted to strings
            self.assertEqual(
                str(A._schema.types),
                "[DataType(string), DataType(string), DataType(string), DataType(string)]",
            )
            arrow_backend.close()

    def test_construct_pandas_logical_type_ext_type_from_string(self):
        # test for the static method that constructs a pandas logical type extension type from a string
        _register_extension_types()
        types = (
            kt.get_python_type_list()
        )  # get all types that are registered in the python side
        # test if these types can be constructed via string

        for dtype_strings in types:
            # get the python type from the string
            mod, name = dtype_strings.rsplit(".", 1)
            try:
                mod = kt._get_module(mod)
                dtype = getattr(mod, name)
                logical_type = ks.logical(dtype)
                pandas_type = logical_type.to_pandas()
            except (ValueError, ModuleNotFoundError):
                continue
            # test pandas type
            string_type = str(pandas_type)
            constructed_type = kap.PandasLogicalTypeExtensionType.construct_from_string(
                string=string_type
            )
            self.assertEqual(pandas_type._logical_type, constructed_type._logical_type)
            self.assertEqual(pandas_type._storage_type, constructed_type._storage_type)

    def test_construct_nested_pandas_logical_type_ext_type_from_string(self):
        """Test for the static method that constructs a pandas logical type extension type from a string, but with
        nested types.
        """
        # create a nested extension type
        conv = DummyConverter()
        struct_dict_enc_type = kasde.StructDictEncodedType(pa.string())
        combined_storage_type = pa.struct(
            [
                pa.field("0", pa.int64()),
                pa.field("1", struct_dict_enc_type),
            ]
        )
        dtype = katy.LogicalTypeExtensionType(
            conv, combined_storage_type, "some_logical_type"
        )
        # re-create it from its string representation
        pandas_dtype = dtype.to_pandas_dtype()
        string_type = str(pandas_dtype)
        constructed_type = kap.PandasLogicalTypeExtensionType.construct_from_string(
            string=string_type
        )
        self.assertEqual(pandas_dtype._logical_type, constructed_type._logical_type)
        self.assertEqual(
            str(pandas_dtype._storage_type), str(constructed_type._storage_type)
        )

    def test_list_setitem(self):
        _register_extension_types()
        with DummyJavaDataSinkFactory(98) as sink_creator:
            arrow_backend = kat.ArrowBackend(sink_creator)

            list_col_name = "List(date)"
            set_col_name = "Set(date)"

            df = _generate_test_data_frame(
                file_name="Lists.zip",
                columns=["Group", list_col_name, set_col_name],
                lists=True,
                sets=True,
            )

            val = df.loc["Row0", set_col_name]
            df.at["Row1", set_col_name] = val

            val = df.loc["Row0", list_col_name]
            df.at["Row1", list_col_name] = val

            # assert that the values are equal
            self.assertEqual(df.loc["Row0", set_col_name], df.loc["Row1", set_col_name])
            self.assertEqual(
                df.loc["Row0", list_col_name], df.loc["Row1", list_col_name]
            )


if __name__ == "__main__":
    unittest.main()
