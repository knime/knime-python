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
import warnings
from typing import Type, Union
import bisect
import re

import numpy as np
import pandas as pd
import pandas.api.extensions as pdext
import pyarrow as pa
from pandas.core.dtypes.dtypes import register_extension_dtype

import knime._arrow._dictencoding as kasde
import knime._arrow._types as katy
import knime.api.types as kt
import knime.api.schema as ks
import logging

LOGGER = logging.getLogger(__name__)


def pandas_df_to_arrow(data_frame: pd.DataFrame, row_ids: str = "auto") -> pa.Table:
    if data_frame.shape == (
        0,
        0,
    ):
        return pa.table([])

    if not issubclass(type(data_frame), pd.DataFrame):
        raise TypeError(
            f"Input must be subclass of a Pandas Dataframe, but is {type(data_frame)}"
        )
    # extract the schema of the df to convert object type columns to logical types
    schema = extract_knime_schema_from_df(data_frame)
    # convert the df via registered column converters and by parsing the objects
    df = convert_df_to_ktypes_from_schema(data_frame, schema)

    # change to dataframe, for instance if it was a GeoDataFrame
    if type(df) != pd.DataFrame:
        df = pd.DataFrame(df)

    # Convert the index to a string series based on the row_ids argument
    if row_ids in ["auto", "keep"]:
        row_ids_series = df.index.to_series()
        if row_ids == "auto" and row_ids_series.dtype.kind in "iu":  # int or uint
            # Add "Row" prefix
            row_ids_series = row_ids_series.apply(lambda i: f"Row{i}")
        else:
            # Just make sure that the RowID column are strings
            row_ids_series = row_ids_series.astype(str)

        # Prepend the index to the data_frame:
        row_ids_series.name = "<RowID>"
        df = pd.concat(
            [row_ids_series.reset_index(drop=True), df.reset_index(drop=True)],
            axis=1,
        )
    elif row_ids == "none":
        df = df.reset_index(drop=True)
    else:
        raise ValueError('row_ids must be one of ["auto", "keep", "none"]')

    # Convert all column names to string or PyArrow might complain
    df.columns = [str(c) for c in df.columns]

    return pa.Table.from_pandas(df)


def extract_knime_schema_from_df(df: pd.DataFrame):
    """This method extracts a knime.schema from a dataframe.

    It finds the correct logical type for 'object' columns by using the first type of the first non-empty element in
    that column


    Args:
        df: dataframe to parse

    Returns: Schema dictionary of the correct types

    """
    schema = {}
    # extract schema
    for col_name, col_type in zip(df.columns, df.dtypes):
        if isinstance(col_type, object):
            try:
                cleaned = df[col_name].dropna()
                if (
                    cleaned.size == 0
                ):  # if the column only contains empty elements we keep object type
                    schema[col_name] = col_type
                    continue
                dtype = type(cleaned.iloc[0])
                if _check_if_local_dt(cleaned, dtype):
                    # as we map all pandas ts and dt objects on the ZonedDT ValFac
                    # we have to manually determine if it is a local dt object
                    col_type = _create_local_dt_type()
                else:
                    col_type = ks.logical(dtype).to_pandas()
            except TypeError:  # if we do not have the type we continue
                pass

        schema[col_name] = col_type
    return schema


def convert_df_to_ktypes_from_schema(df, schema: dict):
    """Converts a df with a schema containing the correct PandasLogicalTypeExtensionTypes.

        This method can be used to convert columns in a dataframe to KNIME types. It iterates over the schema keys to
        convert the columns from different types e.g. object to PandasLogicalTypeExtensionType. Furthermore, it tries to
        convert with the registered column converters. If the conversion fails, the column type remains as it is and
        the function throws a warning.

    Args:
        df: dataframe to convert
        schema: schema dictionary {col: dtype, …}, where col is a column label and dtype is a numpy.dtype,
                Python type or PandasLogicalTypeExtensionType to cast one or more of the DataFrame’s columns
                to column-specific types.

    Returns:the converted dataframe

    """
    # if we change the columns we have to make a shallow copy of the df
    # otherwise changes would be reflected in the original dataframe
    # to avoid copying when not necessary the copy is only made when we actually convert
    is_converted = False
    for col_name in schema.keys():
        dtype = schema[col_name]
        column = df[col_name]
        col_converter = kt.get_first_matching_from_pandas_col_converter(dtype)
        try:
            # convert via column converter
            if col_converter is not None:
                if not is_converted:
                    # create a shallow copy of the dataframe
                    df = df.copy(deep=False)
                    is_converted = True
                with col_converter.warning_manager():
                    df[col_name] = col_converter.convert_column(df, col_name)

            # we have an object type, convert our logical type
            elif isinstance(dtype, PandasLogicalTypeExtensionType) and not isinstance(
                column.dtype, PandasLogicalTypeExtensionType
            ):
                if not is_converted:
                    # create a shallow copy of the dataframe
                    df = df.copy(deep=False)
                    is_converted = True
                # replace all pd.NA or np.NaN Values with the correct na_value and save it as logical type
                # column need to be cast as object so that the NA, NaN and NaT are not automatically converted back
                df[col_name] = (
                    column.astype(object).where(pd.notnull(column), None).astype(dtype)
                )
        except ImportError as e:
            warnings.warn(
                f"Could not convert the column {col_name}; an import error occured: {e}."
            )
        except TypeError:
            warnings.warn(
                f"Automatic type detection assumed type '{dtype}' in column '{col_name}'"
                f" but conversion failed. Please assign a type to the Pandas series using"
                f" knime.schema.logical(correct_dtype).to_pandas()"
            )

    return df


def _create_local_dt_type():
    """

    Returns:manually created Logical Type for a Local dt

    """
    logical = katy._knime_datetime_type("LocalDateTimeValueFactory")
    logical = katy.LogicalTypeExtensionType(
        converter=kt.get_converter(logical),
        storage_type=katy._knime_datetime_logical_to_storage[logical],
        java_value_factory=logical,
    ).to_pandas_dtype()
    return logical


def _check_if_local_dt(column: pd.Series, dtype):
    """Checks if the column contains a dt parse-able type

    Args:
        column: column to check
        dtype: type of the first element in the column

    Returns:true if the column contains a dt parse-able type

    """
    if not (
        str(dtype) == "<class 'pandas._libs.tslibs.timestamps.Timestamp'>"
        or str(dtype) == "<class 'datetime.datetime'>"
    ):
        return False
    # parse timezone
    for elem in column:
        if hasattr(elem, "tzinfo") and elem.tzinfo is not None:
            return False
    return True


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
        try:
            col_converter = kt.get_first_matching_to_pandas_col_converter(col_type)
            if col_converter is not None:
                with col_converter.warning_manager():
                    data_frame[col_name] = col_converter.convert_column(
                        data_frame, col_name
                    )
        except ImportError as e:
            LOGGER.info(
                f"Could not convert the column {col_name}; an import error occured: {e}."
            )

    # The first column is interpreted as the index (RowID)
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

    @classmethod
    def construct_from_string(cls: Type[pdext.ExtensionDtype], string: str):
        # compile a regex to parse the string
        regex = re.compile(
            r"knime.pandas_type<(?P<storage_type>.+), (?P<logical_type>.+)>"
        )
        match = regex.match(string)
        if match is None:
            raise TypeError(f"Cannot construct knime.pandas_type from string {string}")
        storage_type_string = match.group("storage_type")
        storage_type = katy.extract_pa_dtype_from_string(storage_type_string)

        logical_type = match.group("logical_type")
        # get converter
        converter = kt.get_converter(logical_type)
        return PandasLogicalTypeExtensionType(
            storage_type=storage_type, logical_type=logical_type, converter=converter
        )

    def __from_arrow__(self, arrow_array):
        return KnimePandasExtensionArray(
            self._storage_type, self._logical_type, self._converter, arrow_array
        )

    def __str__(self):
        storage_type_string = katy.extract_string_from_pa_dtype(self._storage_type)
        return f"knime.pandas_type<{storage_type_string}, {self._logical_type}>"


def _apply_to_array(array, func):
    if isinstance(array, pa.ChunkedArray):
        return pa.chunked_array([func(chunk) for chunk in array.chunks])
    else:
        return func(array)


class KnimePandasExtensionArray(pdext.ExtensionArray):
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
        self._chunk_start_list = None

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
            if isinstance(scalars.type, katy.LogicalTypeExtensionType):
                return KnimePandasExtensionArray(
                    scalars.type.storage_type,
                    scalars.type.logical_type,
                    scalars.type._converter,
                    scalars,
                )
            elif isinstance(
                scalars.type, katy.StructDictEncodedLogicalTypeExtensionType
            ):
                return KnimePandasExtensionArray(
                    scalars.type.struct_dict_encoded_type,
                    scalars.type.value_factory_type.logical_type,
                    scalars.type.value_factory_type._converter,
                    scalars,
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

            if isinstance(scalars, KnimePandasExtensionArray):
                return cls._as_ext_type(scalars, logical_type, storage_type, converter)

            elif converter is not None and converter.needs_conversion():
                scalars = [converter.encode(s) for s in scalars]

        if storage_type is None:
            raise ValueError(
                "Can only create KnimePandasExtensionArray from a sequence if the storage type is given."
            )

        arrow_type = katy.LogicalTypeExtensionType(
            converter, storage_type, logical_type
        )
        if converter and type(converter) != type(kt.get_converter(logical_type)):
            # If we slice a Pandas series based on a ProxyExtensionType, we should return
            # the appropriate type as well:
            proxy_value_factories = [
                v[1] for v in kt._python_proxy_type_to_factory_info.values()
            ]
            converter_name = type(converter).__qualname__
            if converter_name not in proxy_value_factories:
                raise TypeError(
                    f"""
                    The given configuration is not a valid ProxyExtensionType, converter {converter} was not 
                    found in list of registered proxy converters: {proxy_value_factories}
                    """
                )

            arrow_type = katy.ProxyExtensionType(converter, storage_type, logical_type)

        storage_array = pa.array(scalars, type=storage_type)
        extension_array = pa.ExtensionArray.from_storage(arrow_type, storage_array)
        return KnimePandasExtensionArray(
            storage_type, logical_type, converter, extension_array
        )

    @classmethod
    def _as_ext_type(cls, scalars, logical_type, storage_type, converter):
        """
        Create a new ExtensionArray from the given scalars using the provided type,
        can be used for type casting.
        """

        # We are casting between different extension types, but then the
        # storage and logical types must match
        if (
            scalars._storage_type != storage_type
            or scalars._logical_type != logical_type
        ):
            raise TypeError(
                f"""
                Cannot cast array of type {scalars.dtype} to ({logical_type}, {storage_type}, {converter}). 
                Types are incompatible.
                """
            )

        # we just "reinterpret" the storage data as different dtype -> uses a different converter
        try:
            # casting to "proxy" logical type
            new_data_dtype = katy.ProxyExtensionType(
                converter, storage_type, logical_type
            )
        except TypeError:
            # casting to "primary" logical type
            new_data_dtype = katy.LogicalTypeExtensionType(
                converter, storage_type, logical_type
            )

        def astype(a):
            b = pa.ExtensionArray.from_storage(new_data_dtype, a.storage)
            return b

        casted_data = _apply_to_array(
            scalars._data,
            astype,
        )
        assert casted_data.type == new_data_dtype
        arr = cls(
            storage_type,
            logical_type,
            converter,
            casted_data,
        )

        return arr

    @classmethod
    def _from_factorized(cls, values, original):
        # needed for pandas ExtensionArray API
        raise NotImplementedError(
            "KnimePandasExtensionArray cannot be created from factorized yet."
        )

    def _get_int_item_from_struct_arr(self, storage: pa.StructArray, item: int):
        """This Method unpacks nested struct arrays and takes the value at index: item

        it recursively goes into all sub struct arrays and collects the value at item.
        :param storage: Struct array, which needs to be unpacked
        :param item: index to be searched
        :return: Storage Scalar for the value at item
        """
        storage_scalar_list = []
        for field_idx in range(storage.type.num_fields):  # we unpack each sub-array
            field = storage.field(field_idx)
            # if we have a nested struct array and not the final dict encoded array we recursively access its fields
            if isinstance(field, pa.StructArray) and not isinstance(
                field.type, kasde.StructDictEncodedType
            ):
                storage_scalar_list.append(
                    self._get_int_item_from_struct_arr(field, item)
                )
            else:
                storage_scalar_list.append(field[item])
        storage_scalar_type = katy._struct_type_from_values(*storage_scalar_list)
        storage_scalar = pa.scalar(
            tuple(i.as_py() for i in storage_scalar_list), type=storage_scalar_type
        )
        return storage_scalar

    @staticmethod
    def _get_all_chunk_start_indices(chunked_arr: pa.ChunkedArray) -> list:
        """iterate over chunks to find their start indices

        :param chunked_arr: chunked array from which we calculate the chunk indices
        :return: list containing the end indices for each chunk
        """
        if not isinstance(chunked_arr, pa.ChunkedArray):
            raise TypeError(
                f"The input array must be of type pa.ChunkedArray not type {type(chunked_arr)}"
            )
        chunk_start = 0
        chunk_start_list = []

        for chunk in chunked_arr.chunks:
            chunk_start_list.append(chunk_start)
            chunk_start += len(chunk)  # chunks end at len - 1
        return chunk_start_list

    def __getitem__(self, item):
        if isinstance(item, int):
            if isinstance(self._storage_type, pa.StructType) or isinstance(
                self._storage_type, kasde.StructDictEncodedType
            ):
                # if the storage is a struct type, the unpacking only works for top layer
                # thus, we have to manually access the chunks
                if isinstance(self._data, pa.ChunkedArray):
                    chunk, item = self.get_the_correct_chunk(item)
                    # if we would use the StructDictEncodedLogicalTypeExtensionType this access is necessary
                    if isinstance(self._storage_type, kasde.StructDictEncodedType):
                        return chunk[item]
                    storage = chunk.storage
                elif isinstance(self._data, pa.StructArray):
                    # else we just access the struct
                    storage = self._data
                elif isinstance(self._data, katy.KnimeExtensionArray):
                    storage = self._data.storage
                else:
                    raise TypeError(
                        f"Data can't be of type pa.StructType and not a Chunked, Extension or Struct Array, "
                        f"but is of type {type(self._data)}"
                    )
                # we recursively unpack the struct arrays and finally return the decoded value
                value = self._get_int_item_from_struct_arr(storage, item)
                return self._converter.decode(value.as_py())
            elif isinstance(self._storage_type, pa.ListType):  # list of extension types
                if isinstance(self._data, pa.ChunkedArray):
                    chunk, item = self.get_the_correct_chunk(item)
                    # if we would use the StructDictEncodedLogicalTypeExtensionType this access is necessary
                    if isinstance(self._storage_type, kasde.StructDictEncodedType):
                        return chunk[item]
                    storage = chunk.storage.values
                else:  # we just take from the list
                    storage = self._data.values
                value = storage[item]
                return self._converter.decode(value.as_py())
            else:
                return self._data[item].as_py()
        elif isinstance(item, slice):
            (start, stop, step) = item.indices(len(self._data))
            indices = list(range(start, stop, step))
            return self.take(indices)
        elif isinstance(item, list):
            return self.take(item)
        elif isinstance(item, np.ndarray):
            shape = item.shape
            if len(shape) != 1:
                raise IndexError(
                    f"Only one dimensional np.arrays can be used as indexer, but the given array has shape"
                    f"'{shape}'"
                )
            if item.dtype is np.dtype(np.bool_):
                # the indexer is a mask array
                if item.shape[0] != len(self):
                    raise IndexError(
                        f"The len of the masked array '{item.shape[0]}' is not equal to the length "
                        f"of the data '{len(self)}'"
                    )
                index_list = np.where(item)[0].tolist()
            else:
                # the indexer is an integer access array
                index_list = item.tolist()
            return self.take(index_list)

    def get_the_correct_chunk(self, item):
        if self._chunk_start_list is None:
            self._chunk_start_list = self._get_all_chunk_start_indices(self._data)
        if item < 0:  # if we have a negative index access
            item = len(self) + item
        # use a right bisection to locate the closest chunk index
        chunk_idx = bisect.bisect_right(self._chunk_start_list, item) - 1
        item = (
            item - self._chunk_start_list[chunk_idx]
        )  # get the index inside the chunk
        chunk = self._data.chunk(chunk_idx)  # get the correct chunk
        return chunk, item

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

        def _set_data_from_input(inp: Union[list, np.ndarray]):
            """Set the backbone data array with new values.

            Parameters
            ----------
            inp : input list which is used as new data.
            Returns
            -------
            None
            """
            ext_type = self._data.type
            try:
                # encode the python value to the correct storage type
                encoded_pylist = list(map(ext_type.encode, inp))
            except TypeError:
                raise TypeError(
                    f"Encoding of the new value is not possible, the array has the type '{ext_type}',"
                    f" maybe its not the right dtype?"
                )
            if katy.contains_knime_extension_type(ext_type):
                storage_type = katy.get_storage_type(ext_type)
                arr = pa.array(encoded_pylist, type=storage_type)
            else:
                arr = pa.array(encoded_pylist, type=ext_type.storage_type)

            self._data = katy._to_extension_array(arr, ext_type)

        tmp_list = self._data.to_pylist()  # convert immutable data to mutable list

        if isinstance(item, tuple) and len(item) == 1:
            # unwrap single-tuples which can arrive here since Pandas 1.5
            item = item[0]

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
                f"Setting a value with an indexer {item} of type {type(item)} is not (yet) possible"
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
        if (isinstance(indices, list) and indices == [None] * len(indices)) or (
            isinstance(indices, np.ndarray) and (indices == None).all()
        ):
            return self._from_sequence(
                [None] * len(indices),
                storage_type=self._storage_type,
                logical_type=self._logical_type,
                converter=self._converter,
            )
        storage = katy._to_storage_array(
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
            storage_type = katy.get_storage_type(self._storage_type)
            taken = pa.array(taken, type=storage_type, mask=null_mask)

        else:
            taken = storage.take(indices)
        wrapped = katy._to_extension_array(taken, self._data.type)

        return self._from_sequence(
            wrapped,
            storage_type=self._storage_type,
            logical_type=self._logical_type,
            converter=self._converter,
        )

    def copy(self):
        # needed for pandas ExtensionArray API

        # A copy should have its own copy of data, but as plain arrow arrays are immutable,
        # it suffices to hand over the array. Chunked arrays however behave like lists and
        # changes to their underlying data would be reflected in the copy, too.
        data_copy = self._data
        if isinstance(data_copy, pa.ChunkedArray):
            data_copy = pa.chunked_array(self._data.chunks)

        return KnimePandasExtensionArray(
            self._storage_type, self._logical_type, self._converter, data_copy
        )

    def astype(self, dtype, copy: bool = True):
        if not isinstance(dtype, PandasLogicalTypeExtensionType):
            # fall back to default
            return super().astype(dtype, copy)

        cls = dtype.construct_array_type()
        return cls._from_sequence(self, dtype=dtype, copy=copy)

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

    def __repr__(self):
        return f"KnimePandasExtArray({self._converter}, {self._storage_type}, {self._logical_type})"

    def __array__(self, dtype=None):
        # we cannot use the pyarrow to_pylist method as this it to use the scalar representation for dictionary decoding
        return np.array(
            [self[i] for i in range(len(self))],
            dtype=np.dtype("object") if dtype is None else dtype,
        )
