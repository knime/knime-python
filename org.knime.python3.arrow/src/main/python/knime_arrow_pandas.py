# -*- coding: utf-8 -*-
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
from typing import Type, Union
import bisect

import numpy as np
import pandas as pd
import pandas.api.extensions as pdext
import pyarrow as pa
from pandas.core.dtypes.dtypes import register_extension_dtype

import knime_arrow_struct_dict_encoding as kasde
import knime_arrow_types as kat
import knime_types as kt


def pandas_df_to_arrow(data_frame: pd.DataFrame) -> pa.Table:
    if data_frame.shape == (
        0,
        0,
    ):
        return pa.table([])

    if not issubclass(type(data_frame), pd.DataFrame):
        raise TypeError(
            f"Input must be subclass of a Pandas Dataframe, but is {type(data_frame)}"
        )

    # if we change the columns we have to make a shallow copy of the df
    # otherwise changes would be reflected in the original dataframe
    # to avoid copying when not necessary the copy is only made when we actually convert
    df = None
    for col_name, col_type in zip(data_frame.columns, data_frame.dtypes):
        col_converter = kt.get_first_matching_from_pandas_col_converter(col_type)
        if col_converter is not None:
            if df is None:
                # create a shallow copy of the dataframe
                df = data_frame.copy(deep=False)
            with col_converter.warning_manager():
                df[col_name] = col_converter.convert_column(df, col_name)

    if df is None:
        df = data_frame
    # change to dataframe, for instance if GeoDataFrame
    if type(df) != pd.DataFrame:
        df = pd.DataFrame(df)

    # Convert the index to a str series and prepend to the data_frame:
    # extract and drop index from DF
    row_keys = df.index.to_series().astype(str)
    row_keys.name = "<Row Key>"  # TODO what is the right string?
    df = pd.concat(
        [row_keys.reset_index(drop=True), df.reset_index(drop=True)],
        axis=1,
    )

    # Convert all column names to string or PyArrow might complain
    df.columns = [str(c) for c in df.columns]

    return pa.Table.from_pandas(df)


def arrow_data_to_pandas_df(data: Union[pa.Table, pa.RecordBatch]) -> pd.DataFrame:
    # Use Pandas' String data type if available instead of "object" if we're using a
    # Pandas version that is new enough. Gives better type safety and preserves its
    # type even if all values are missing in a column.

    if hasattr(pd, "StringDtype"):

        def mapper(dtype):
            # TODO: maybe also try the nullable integer types of Pandas so we don't need sentinels any more?
            if dtype == pa.string():
                return pd.StringDtype()
            else:
                return None

        data_frame = data.to_pandas(types_mapper=mapper)
    else:
        data_frame = data.to_pandas()
    for col_name, col_type in zip(data.schema.names, data.schema.types):
        col_converter = kt.get_first_matching_to_pandas_col_converter(col_type)
        if col_converter is not None:
            with col_converter.warning_manager():
                data_frame[col_name] = col_converter.convert_column(
                    data_frame, col_name
                )
    # The first column is interpreted as the index (row keys)
    data_frame.set_index(data_frame.columns[0], inplace=True)

    return data_frame


@register_extension_dtype
class PandasLogicalTypeExtensionType(pdext.ExtensionDtype):
    def __init__(self, storage_type: pa.DataType, logical_type: str, converter):
        self._storage_type = storage_type
        self._logical_type = logical_type
        self._converter = converter

        # used by pandas to query all attributes of this ExtensionType
        self._metadata = ("_storage_type", "_logical_type", "_converter")

    na_value = None
    type = bytes  # We just say that this is raw data?! No need to be interpreted :)

    @property
    def name(self):
        return f"PandasLogicalTypeExtensionType({self._storage_type}, {self._logical_type})"

    def construct_array_type(self):
        return KnimePandasExtensionArray

    def construct_from_string(cls: Type[pdext.ExtensionDtype], string: str):
        # TODO implement this?
        raise NotImplementedError("construct from string not available yet")

    def __from_arrow__(self, arrow_array):
        return KnimePandasExtensionArray(
            self._storage_type, self._logical_type, self._converter, arrow_array
        )

    def __str__(self):
        return f"PandasLogicalTypeExtensionType({self._storage_type}, {self._logical_type})"


def _struct_type_from_values(*args):
    """Utility method to create a pyarrow.struct type object for structs coming from KNIME.

    Structs coming from KNIME have no names for the children. Instead the children are named
    "0", "1", "2", ...

    Arguments:
        args: The pyarrow values for the children.
    """
    return pa.struct([pa.field(f"{i}", t.type) for i, t in enumerate(args)])


class KnimePandasExtensionArray(pdext.ExtensionArray):
    chunk_start_list = None
    def __init__(
        self,
        storage_type: pa.DataType,
        logical_type: str,
        converter,
        data: Union[pa.Array, pa.ChunkedArray],
    ):
        if data is None:
            raise ValueError("Cannot create empty KnimePandasExtensionArray")

        self._data = data
        self._converter = converter
        self._storage_type = storage_type
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
        converter=None,
    ):
        """
        Construct a new ExtensionArray from a sequence of scalars.
        Parameters. This is similar to the default docstring of extension arrays, except for the additional parameters:
        storage_type, logical_type and converter, which are added by KNIME and only used internally.
        ----------
        scalars : Sequence
            Each element will be an instance of the scalar type for this
            array, ``cls.dtype.type``.
        dtype : dtype, optional
            Construct for this particular dtype. This should be a Dtype
            compatible with the ExtensionArray.
        copy : bool, default False
            If True, copy the underlying data.
        storage_type: pa.DataType,
            Dtype in which the data is stored on disk.
        logical_type: str,
            Dtype that is used in KNIME
        converter: any,
            Valuefactory to decode and encode the datatype. For an example see :class: `LocalDateTimeValueFactory`
        Returns
        -------
        ExtensionArray
        """
        if scalars is None:
            raise ValueError("Cannot create KnimePandasExtensionArray from empty data")
            # easy case
        if isinstance(scalars, pa.Array) or isinstance(scalars, pa.ChunkedArray):
            if isinstance(scalars.type, kat.LogicalTypeExtensionType):
                return KnimePandasExtensionArray(
                    scalars.type.storage_type,
                    scalars.type.logical_type,
                    scalars.type._converter,
                    scalars,
                )
            elif isinstance(scalars.type, kat.StructDictEncodedLogicalTypeExtensionType):
                return KnimePandasExtensionArray(
                    scalars.type.struct_dict_encoded_type,
                    scalars.type.value_factory_type.logical_type,
                    scalars.type.value_factory_type._converter,
                    scalars
                )

            else:
                raise ValueError(
                    "KnimePandasExtensionArray must be backed by LogicalTypeExtensionType values"
                )

        if isinstance(dtype, PandasLogicalTypeExtensionType):
            # in this case we can extract storage, logical_type and converter
            storage_type = dtype._storage_type
            logical_type = dtype._logical_type
            converter = dtype._converter
            if converter is not None and converter.needs_conversion():
                scalars = [converter.encode(s) for s in scalars]

        if storage_type is None:
            raise ValueError(
                "Can only create KnimePandasExtensionArray from a sequence if the storage type is given."
            )

        # needed for pandas ExtensionArray API
        arrow_type = kat.LogicalTypeExtensionType(converter, storage_type, logical_type)

        a = pa.array(scalars, type=storage_type)
        extension_array = pa.ExtensionArray.from_storage(arrow_type, a)
        return KnimePandasExtensionArray(
            storage_type, logical_type, converter, extension_array
        )

    @classmethod
    def _from_factorized(cls, values, original):
        # needed for pandas ExtensionArray API
        raise NotImplementedError(
            "KnimePandasExtensionArray cannot be created from factorized yet."
        )

    def _get_int_item_from_struct_arr(self, storage: pa.StructArray, item: int):
        """ This Method unpacks nested struct arrays and takes the value at index: item

        it recursively goes into all sub struct arrays.
        :param storage: Struct array, which needs to be unpacked
        :param item: index to be searched
        :return: Storage Scalar for the value at item
        """
        storage_scalar_list = []
        for field_idx in range(storage.type.num_fields): # we unpack each sub-array
            field = storage.field(field_idx)
            # if we have a nested struct array and not the final dict encoded array we recursively access its fields
            if isinstance(field, pa.StructArray) and not isinstance(field.type, kasde.StructDictEncodedType):
                storage_scalar_list.append(self._get_int_item_from_struct_arr(field, item))
            else:
                storage_scalar_list.append(field[item])
        storage_scalar_type = _struct_type_from_values(*storage_scalar_list)
        storage_scalar = pa.scalar(tuple(i.as_py() for i in storage_scalar_list),
                                   type=storage_scalar_type)
        return storage_scalar

    @staticmethod
    def _get_all_chunk_start_indices(chunked_arr: pa.ChunkedArray) -> list:
        """ iterate over chunks to find their start indices

        :param chunked_arr: chunked array from which we calculate the chunk indices
        :return: list containing the end indices for each chunk
        """
        if not isinstance(chunked_arr, pa.ChunkedArray):
            raise TypeError(f"The input array must be of type pa.ChunkedArray not type {type(chunked_arr)}")
        chunk_start = 0
        chunk_start_list = []

        for chunk in chunked_arr.chunks:
            chunk_start_list.append(chunk_start)
            chunk_start += len(chunk) # chunks end at len - 1
        return chunk_start_list


    def __getitem__(self, item):
        if isinstance(item, int):
            if isinstance(self._storage_type, pa.StructType) or isinstance(self._storage_type, kasde.StructDictEncodedType):
                # if the storage is a struct type, the unpacking only works for top layer
                # thus, we have to manually access the chunks
                if isinstance(self._data, pa.ChunkedArray):
                    if self.chunk_start_list is None:
                        self.chunk_start_list = self._get_all_chunk_start_indices(self._data)
                    # use a right bisection to locate the closest chunk index
                    chunk_idx = bisect.bisect_right(self.chunk_start_list, item) - 1
                    item = item - self.chunk_start_list[chunk_idx]  # get the index inside the chunk
                    chunk = self._data.chunk(chunk_idx)  # get the correct chunk
                    # if we would use the StructDictEncodedLogicalTypeExtensionType this access is necessary
                    if isinstance(self._storage_type, kasde.StructDictEncodedType):
                        return chunk[item]
                    storage = chunk.storage
                elif isinstance(self._data, pa.StructArray):
                    # else we just access the struct
                    storage = self._data
                else:
                    raise TypeError("Data can't be of type pa.StructType and not a Chunked or Struct Array")
                # we recursively unpack the struct arrays and finally return the decoded value
                value = self._get_int_item_from_struct_arr(storage, item)
                decoded = self._converter.decode(value.as_py())
                return decoded

            return self._data[item].as_py()
        elif isinstance(item, slice):
            (start, stop, step) = item.indices(len(self._data))
            indices = list(range(start, stop, step))
            return self.take(indices)
        elif isinstance(item, list):
            return self.take(item)
        elif isinstance(item, np.ndarray):
            # masked array
            raise NotImplementedError("Cannot index using masked array from numpy yet.")

    def __setitem__(self, item, value):
        """
        Set one or more values inplace.

        This method is not required to satisfy the pandas extension array
        interface.

        Parameters
        ----------
        item : int, ndarray, or slice
            When called from, e.g. ``Series.__setitem__``, ``item`` will be
            one of

            * scalar int
            * ndarray of integers.
            * boolean ndarray
            * slice object

        value : ExtensionDtype.type, Sequence[ExtensionDtype.type], or object
            value or values to be set of ``key``.

        Returns
        -------
        None
        """

        def _apply_to_array(array, func):
            if isinstance(array, pa.ChunkedArray):
                return pa.chunked_array([func(chunk) for chunk in array.chunks])
            else:
                return func(array)

        def _set_data_from_input(inp: Union[list, np.ndarray]):
            # todo check if data contains lists > recursive mapping
            our_ext_type = self._data.type
            try:
                tmp = list(map(our_ext_type.encode, inp))
            except TypeError:
                raise TypeError(
                    "Encoding of the new value is not possible, maybe its not the right dtype?"
                )

            an_arr = pa.array(tmp, type=our_ext_type.storage_type)

            an_arr = _apply_to_array(
                an_arr, lambda a: pa.ExtensionArray.from_storage(our_ext_type, a)
            )
            self._data = an_arr

        tmp_list = self._data.to_pylist()  # convert immutable data to mutable list

        if isinstance(item, int):
            tmp_list[item] = value
            _set_data_from_input(tmp_list)

        elif isinstance(item, slice):

            (start, stop, step) = item.indices(len(self._data))

            if hasattr(value, "__len__") and 1 < len(value) != len(
                range(start, stop, step)
            ):
                raise ValueError(
                    "Must have equal len keys and value when setting with an iterable"
                )

            val_i = 0  # index for the value
            for i in range(start, stop, step):  # todo: improve speed with np.array
                if isinstance(value, pd.Series):  # no broadcasting necessary
                    try:
                        tmp_list[i] = value.iloc[val_i]
                    except IndexError:
                        raise ValueError(
                            "Must have equal len keys and value when setting with an iterable"
                        )
                    val_i += 1
                elif isinstance(
                    value, KnimePandasExtensionArray
                ):  # value is an extension array
                    try:
                        tmp_list[i] = value[val_i]
                    except IndexError:
                        raise ValueError(
                            "Must have equal len keys and value when setting with an iterable"
                        )
                    val_i += 1
                else:  # value needs to be broadcasted
                    tmp_list[i] = value

            _set_data_from_input(tmp_list)

        elif isinstance(item, list):
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

        else:
            raise NotImplementedError(
                f"Setting a value with an indexer of type {type(item)} is not (yet) possible"
            )

    def __len__(self):
        return len(self._data)

    def __eq__(self, other) -> bool:
        if not isinstance(other, KnimePandasExtensionArray):
            return False
        return (
            other._storage_type == self._storage_type
            and other._logical_type == self._logical_type
            and other._converter == self._converter
            and other._data == self._data
        )

    @property
    def dtype(self):
        # needed for pandas ExtensionArray API
        return PandasLogicalTypeExtensionType(
            self._storage_type, self._logical_type, self._converter
        )

    @property
    def nbytes(self):
        # needed for pandas ExtensionArray API
        return self._data.nbytes

    def isna(self):
        # needed for pandas ExtensionArray API
        if isinstance(self._data, pa.ChunkedArray):
            return np.concatenate(
                [c.to_numpy(zero_copy_only=False) for c in self._data.is_null().chunks]
            )

        return self._data.is_null().to_numpy(zero_copy_only=False)

    def take(
        self, indices, allow_fill: bool = False, fill_value=None
    ) -> "KnimePandasExtensionArray":
        """
        Take elements from an array.
        Parameters
        ----------
        indices : sequence of int
            Indices to be taken.
        allow_fill : bool, default False
            How to handle negative values in `indices`.
            * False: negative values in `indices` indicate positional indices
              from the right (the default). This is similar to
              :func:`numpy.take`.
            * True: negative values in `indices` indicate
              missing values. These values are set to `fill_value`. Any other
              other negative values raise a ``ValueError``.
        fill_value : any, optional
            Fill value to use for NA-indices when `allow_fill` is True.
            This may be ``None``, in which case the default NA value for
            the type, ``self.dtype.na_value``, is used.
            For many ExtensionArrays, there will be two representations of
            `fill_value`: a user-facing "boxed" scalar, and a low-level
            physical NA value. `fill_value` should be the user-facing version,
            and the implementation should handle translating that to the
            physical version for processing the take if necessary.
        Returns
        -------
        ExtensionArray
        Raises
        ------
        IndexError
            When the indices are out of bounds for the array.
        ValueError
            When `indices` contains negative values other than ``-1``
            and `allow_fill` is True.
        See Also
        --------
        numpy.take
        api.extensions.take
        """
        storage = kat._to_storage_array(
            self._data
        )  # decodes the data puts it in storage array

        if allow_fill and fill_value is None:  # ensures the right fill value
            fill_value = self.dtype.na_value

        if allow_fill:  # in this case -1's can be included in indices
            from pandas.core.algorithms import take

            taken = take(
                storage, indices, fill_value=fill_value, allow_fill=allow_fill
            )  # returns nparray
            # to convert to pa.array, the correct null values have to be provided
            # missing values in storage have to be located and their indexes have to be consistent with the taken values
            # thus the take function is also used to get missing values
            bool_nulls = pd.Series(self._data.is_null())
            null_mask = take(
                bool_nulls.to_numpy(), indices, fill_value=True, allow_fill=allow_fill
            )  # returns nparray

            taken = pa.array(taken, type=self._storage_type, mask=null_mask)

        else:
            taken = storage.take(indices)
        wrapped = kat._to_extension_array(taken, self._data.type)

        return self._from_sequence(
            wrapped,
            storage_type=self._storage_type,
            logical_type=self._logical_type,
            converter=self._converter,
        )

    def copy(self):
        # needed for pandas ExtensionArray API
        # TODO: do we really want to copy the data? This thing is read only anyways... Unless we implement __setitem__ and concat
        return self

    @classmethod
    def _concat_same_type(cls, to_concat):
        # needed for pandas ExtensionArray API
        if len(to_concat) < 1:
            raise ValueError("Nothing to concatenate")
        elif len(to_concat) == 1:
            return to_concat[0]

        chunks = []
        for pandas_ext_array in to_concat:
            d = pandas_ext_array._data
            if isinstance(d, pa.ChunkedArray):
                chunks += d.chunks
            else:
                chunks.append(d)

        combined_data = pa.chunked_array(chunks)
        first = to_concat[0]
        return KnimePandasExtensionArray(
            first._storage_type, first._logical_type, first._converter, combined_data
        )
