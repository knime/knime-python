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

"""
Arrow implementation of the knime_types.

@author Adrian Nembach, KNIME GmbH, Konstanz, Germany
"""

from typing import Optional, Union

import knime_types as kt
import knime_arrow_struct_dict_encoding as kas
import pyarrow as pa
import pyarrow.types as pat
import numpy as np
import json


def _pretty_type_string(dtype: pa.DataType):
    if is_dict_encoded_value_factory_type(dtype):
        return f"StructDictEncoded<key={dtype.key_type}>[{_pretty_type_string(dtype.value_type)}]"
    elif is_value_factory_type(dtype):
        # return f"LogicalType<{dtype.logical_type}>[{_pretty_type_string(dtype.storage_type)}]"
        return f"LogicalType[{_pretty_type_string(dtype.storage_type)}]"
    elif kas.is_struct_dict_encoded(dtype):
        return f"StructDictEncoded<key={dtype.key_type}>[{_pretty_type_string(dtype.value_type)}]"
    elif isinstance(dtype, pa.ExtensionType):
        return f"{str(dtype)}[{_pretty_type_string(dtype.storage_type)}]"
    elif pat.is_struct(dtype):
        return (
            f"struct[{', '.join([_pretty_type_string(inner.type) for inner in dtype])}]"
        )
    elif is_list_type(dtype):
        return f"list[{_pretty_type_string(dtype.value_type)}]"
    else:
        return str(dtype)


def is_list_type(dtype: pa.DataType):
    return pat.is_large_list(dtype) or pat.is_list(dtype)


def _get_arrow_storage_to_ext_fn(dtype):
    if is_dict_encoded_value_factory_type(dtype):
        key_gen = kas.DictKeyGenerator()
        storage_fn = _get_arrow_storage_to_ext_fn(dtype.storage_type) or _identity

        def wrap_and_struct_dict_encode(a):
            unencoded_storage = storage_fn(a)
            encoded_storage = kas.create_storage_for_struct_dict_encoded_array(
                unencoded_storage,
                key_gen,
                value_type=dtype.value_type,
                key_type=dtype.key_type,
            )
            return pa.ExtensionArray.from_storage(dtype, encoded_storage)

        return wrap_and_struct_dict_encode
    elif kas.is_struct_dict_encoded(dtype):
        key_gen = kas.DictKeyGenerator()
        return lambda a: kas.struct_dict_encode(a, key_gen, key_type=dtype.key_type)
    elif is_value_factory_type(dtype):
        storage_fn = _get_arrow_storage_to_ext_fn(dtype.storage_type) or _identity
        return lambda a: pa.ExtensionArray.from_storage(dtype, storage_fn(a))
    elif is_list_type(dtype):
        if not contains_knime_extension_type(dtype):
            return _identity

        value_fn = _get_arrow_storage_to_ext_fn(dtype.value_type) or _identity
        # We have to cast the returned list to the expected type because
        # otherwise some internal field will have a different name (item != $data$)
        # and make PyArrow's type conversion state that the types differ...
        return lambda a: _create_list_array(a.offsets, value_fn(a.values)).cast(dtype)
    elif pat.is_struct(dtype):
        if not contains_knime_extension_type(dtype):
            return _identity

        inner_fns = [_get_arrow_storage_to_ext_fn(inner.type) for inner in dtype]
        return lambda a: pa.StructArray.from_arrays(
            [fn(inner) for fn, inner in zip(inner_fns, a.flatten())],
            names=[t.name for t in dtype],
        )
    else:
        return _identity


def _identity(x):
    return x


def _create_list_array(offsets, values):
    offset_type = offsets.type
    if pat.is_int64(offset_type):
        return pa.LargeListArray.from_arrays(offsets, values)
    elif pat.is_int32(offset_type):
        return pa.ListArray.from_arrays(offsets, values)


def _to_storage_array(array: pa.Array) -> pa.Array:
    compatibility_fn = _get_array_to_storage_fn(array.type)
    if compatibility_fn is None:
        return array
    else:
        return _apply_to_array(array, compatibility_fn)


def _get_array_to_storage_fn(dtype: pa.DataType):
    if is_dict_encoded_value_factory_type(dtype):
        storage_fn = _get_array_to_storage_fn(dtype.value_type) or _identity
        return lambda a: storage_fn(a.dictionary_decode())
    elif is_value_factory_type(dtype):
        storage_fn = _get_array_to_storage_fn(dtype.storage_type) or _identity
        return lambda a: storage_fn(a.storage)
    elif kas.is_struct_dict_encoded(dtype):
        return lambda a: a.dictionary_decode()
    elif is_list_type(dtype):
        value_fn = _get_array_to_storage_fn(dtype.value_type)
        if value_fn is None:
            return None
        else:
            return lambda a: _create_list_array(a.offsets, _to_storage_array(a.values))
    elif pat.is_struct(dtype):
        inner_fns = [_get_array_to_storage_fn(inner.type) for inner in dtype]
        if all(i is None for i in inner_fns):
            return None
        else:
            inner_fns = [_identity if fn is None else fn for fn in inner_fns]

            def _to_storage_struct(struct_array: pa.StructArray):
                inner = [fn(struct_array.field(i)) for i, fn in enumerate(inner_fns)]
                return pa.StructArray.from_arrays(
                    inner, names=[field.name for field in dtype]
                )

            return _to_storage_struct
    else:
        return None


def contains_knime_extension_type(dtype: pa.DataType):
    if (
        is_value_factory_type(dtype)
        or kas.is_struct_dict_encoded(dtype)
        or is_dict_encoded_value_factory_type(dtype)
    ):
        return True
    elif is_list_type(dtype):
        return contains_knime_extension_type(dtype.value_type)
    elif pat.is_struct(dtype):
        return any(contains_knime_extension_type(inner.type) for inner in dtype)
    else:
        return False


def needs_conversion(dtype: pa.DataType):
    if kas.is_struct_dict_encoded(dtype) or is_dict_encoded_value_factory_type(dtype):
        return True
    elif is_value_factory_type(dtype):
        return dtype.needs_conversion
    elif is_list_type(dtype):
        return needs_conversion(dtype.value_type)
    elif pat.is_struct(dtype):
        return any(needs_conversion(inner.type) for inner in dtype)
    else:
        return False


def get_object_to_storage_fn(dtype: pa.DataType):
    """
    Constructs a function that can be used to turn a Python object that corresponds to the provided Arrow type into a
    storage version of itself that Arrow can parse into an array.
    """
    if is_dict_encoded_value_factory_type(dtype):
        storage_fn = get_object_to_storage_fn(dtype.value_type)
        return lambda a: storage_fn(dtype.encode(a))
    elif is_value_factory_type(dtype):
        storage_fn = get_object_to_storage_fn(dtype.storage_type)
        return lambda a: storage_fn(dtype.encode(a))
    elif kas.is_struct_dict_encoded(dtype):
        return _identity
    elif is_list_type(dtype):
        inner_fn = get_object_to_storage_fn(dtype.value_type)
        return lambda l: [inner_fn(x) for x in l]
    elif pat.is_struct(dtype):
        inner_fns = [get_object_to_storage_fn(field.type) for field in dtype]
        return lambda d: {
            inner[0]: fn(inner[1]) for fn, inner in zip(inner_fns, d.items())
        }
    else:
        return _identity


def _to_extension_array(data, target_type):
    if data.type != target_type:
        assert target_type is not None
        return _apply_to_array(data, _get_arrow_storage_to_ext_fn(target_type))
    else:
        return data


def insert_sentinel_for_missing_values(
    data: Union[pa.RecordBatch, pa.Table], sentinel: Union[str, int]
) -> Union[pa.RecordBatch, pa.Table]:
    arrays = []
    for column in data:
        # TODO: list/struct of integral data
        if pa.types.is_integer(column.type) and column.null_count != 0:
            column = _insert_sentinels_for_missing_values_in_int_array(column, sentinel)
        arrays.append(column)

    if isinstance(data, pa.RecordBatch):
        return pa.RecordBatch.from_arrays(arrays, schema=data.schema)
    else:
        return pa.Table.from_arrays(arrays, schema=data.schema)


def _sentinel_value(dtype: pa.DataType, sentinel: Union[str, int]) -> int:
    if sentinel == "min":
        return -2147483648 if pa.types.is_int32(dtype) else -9223372036854775808
    elif sentinel == "max":
        return 2147483647 if pa.types.is_int32(dtype) else 9223372036854775807
    else:
        return int(sentinel)


def _insert_sentinels_for_missing_values_in_int_array(
    array: pa.Array, sentinel: Union[str, int]
) -> pa.Array:
    sentinel_value = _sentinel_value(array.type, sentinel)
    return array.fill_null(sentinel_value)


def sentinel_to_missing_value(
    data: Union[pa.RecordBatch, pa.Table], sentinel: Union[str, int]
) -> Union[pa.RecordBatch, pa.Table]:
    arrays = []
    for column in data:
        # TODO: list/struct of integral data
        if pa.types.is_integer(column.type):
            column = _sentinel_to_missing_value_in_int_array(column, sentinel)
        arrays.append(column)
    if isinstance(data, pa.RecordBatch):
        return pa.RecordBatch.from_arrays(arrays, schema=data.schema)
    else:
        return pa.Table.from_arrays(arrays, schema=data.schema)


def _sentinel_to_missing_value_in_int_array(
    array: pa.Array, sentinel: Union[str, int]
) -> pa.Array:
    sentinel_value = _sentinel_value(array.type, sentinel)
    mask = pa.compute.equal(array, sentinel_value)
    return pa.compute.if_else(mask, None, array)


def _apply_to_array(array, func):
    if isinstance(array, pa.ChunkedArray):
        return pa.chunked_array([func(chunk) for chunk in array.chunks])
    else:
        return func(array)


class LogicalTypeExtensionType(pa.ExtensionType):
    def __init__(self, converter, storage_type, java_value_factory):
        self._converter = converter
        self._logical_type = java_value_factory
        pa.ExtensionType.__init__(self, storage_type, "knime.logical_type")

    def __arrow_ext_serialize__(self):
        return self._logical_type.encode()

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        logical_type = serialized.decode()
        converter = kt.get_converter(logical_type)
        return LogicalTypeExtensionType(converter, storage_type, logical_type)

    def __arrow_ext_class__(self):
        return KnimeExtensionArray

    def decode(self, storage):
        return self._converter.decode(storage)

    def encode(self, value):
        return self._converter.encode(value)

    @property
    def needs_conversion(self):
        return False if self._converter is None else self._converter.needs_conversion()

    @property
    def logical_type(self):
        return self._logical_type

    @staticmethod
    def version():
        """The version is stored in the Arrow field's metadata and must match the Java version of the extension type"""
        return 0

    def to_pandas_dtype(self):
        from knime_arrow_pandas import PandasLogicalTypeExtensionType

        return PandasLogicalTypeExtensionType(
            self.storage_type, self._logical_type, self._converter
        )


# Register our extension type with
pa.register_extension_type(LogicalTypeExtensionType(None, pa.null(), ""))


class StructDictEncodedLogicalTypeExtensionType(pa.ExtensionType):
    def __init__(self, value_factory_type, struct_dict_encoded_type):
        self.struct_dict_encoded_type = struct_dict_encoded_type
        self.value_factory_type = value_factory_type
        # is none in the instance used for registration with pyarrow
        if value_factory_type is not None:
            self.encode = value_factory_type.encode
            self.decode = value_factory_type.decode
            self.needs_conversion = value_factory_type.needs_conversion
        pa.ExtensionType.__init__(
            self,
            struct_dict_encoded_type.storage_type,
            "knime.struct_dict_encoded_logical_type",
        )

    @property
    def value_type(self):
        return self.struct_dict_encoded_type.value_type

    @property
    def key_type(self):
        return self.struct_dict_encoded_type.key_type

    @staticmethod
    def version():
        return kas.StructDictEncodedType.version()

    def __arrow_ext_serialize__(self):
        # StructDictEncodedType doesn't have any meta data
        return self.value_factory_type.__arrow_ext_serialize__()

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        struct_dict_encoded_type = kas.StructDictEncodedType.__arrow_ext_deserialize__(
            storage_type, b""
        )
        value_factory_type = LogicalTypeExtensionType.__arrow_ext_deserialize__(
            storage_type, serialized
        )
        return StructDictEncodedLogicalTypeExtensionType(
            value_factory_type=value_factory_type,
            struct_dict_encoded_type=struct_dict_encoded_type,
        )

    def __arrow_ext_class__(self):
        return StructDictEncodedLogicalTypeArray


pa.register_extension_type(
    StructDictEncodedLogicalTypeExtensionType(
        LogicalTypeExtensionType(None, pa.null(), ""),
        kas.StructDictEncodedType(pa.null()),
    )
)


class StructDictEncodedLogicalTypeArray(kas.StructDictEncodedArray):
    """
    An array of logical values where the underlying data is struct-dict-encoded.
    """

    def _getitem(self, index):
        storage = super()._getitem(index)
        return KnimeExtensionScalar(self.type.value_factory_type, storage)


def is_dict_encoded_value_factory_type(dtype: pa.DataType):
    return isinstance(dtype, StructDictEncodedLogicalTypeExtensionType)


def is_value_factory_type(dtype: pa.DataType):
    return isinstance(dtype, LogicalTypeExtensionType)


def _data_spec_json_to_arrow(data_spec_json):
    data_spec = json.loads(data_spec_json)
    return _data_spec_to_arrow(data_spec)


_primitive_type_map = {
    "string": pa.string(),
    "variable_width_binary": pa.large_binary(),
    "boolean": pa.bool_(),
    "byte": pa.int8(),
    "double": pa.float64(),
    "float": pa.float32(),
    "int": pa.int32(),
    "long": pa.int64(),
    "void": pa.null(),
}


def _data_spec_to_arrow(data_spec):
    if isinstance(data_spec, str):
        return _primitive_type_map[data_spec]
    else:
        type_id = data_spec["type"]
        if type_id == "list":
            value_type = data_spec["inner_type"]
            return pa.large_list(value_type)
        elif type_id == "struct":
            fields = [
                (str(i), _data_spec_to_arrow(t))
                for (i, t) in enumerate(data_spec["inner_types"])
            ]
            return pa.struct(fields)
        else:
            raise ValueError("Invalid data_spec: " + str(data_spec))


class KnimeExtensionArray(pa.ExtensionArray):
    def __getitem__(self, idx):
        storage_scalar = self.storage[idx]
        # TODO return Scalar once there is a customizable ExtensionScalar in pyarrow
        #  to be consistent with other pa.Arrays
        return KnimeExtensionScalar(self.type, storage_scalar)

    def to_pylist(self):
        return [self.type.decode(x) for x in self.storage.to_pylist()]

    def to_pandas(self):
        # TODO use super method and pass through arguments (i.e. essentially decorate the super implementation)
        series = self.storage.to_pandas()
        return series.apply(self.type.decode, convert_dtype=False)

    def to_numpy(self):
        # TODO same as for to_pandas
        ndarray = self.storage.to_numpy(zero_copy_only=False)
        # TODO we might need different converters for different libraries
        return np.array([self.type.decode(x) for x in ndarray])


class KnimeExtensionScalar:
    """
    Mimics the behavior of an Arrow Scalar.
    TODO Replace with an ExtensionScalar once pyarrow has proper support (AP-17422)
    """

    def __init__(self, ext_type: LogicalTypeExtensionType, storage_scalar: pa.Scalar):
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
        elif isinstance(target_type, LogicalTypeExtensionType):
            raise ValueError(
                "Casting to different KnimeArrowExtensionTypes is not supported"
            )
        else:
            return self.storage_scalar.cast(target_type)

    def __repr__(self):
        return f"knime_arrow_types.KnimeExtensionScalar: {self.as_py()!r}"

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
    return KnimeExtensionScalar(ext_type, storage_scalar)


def _knime_primitive_type(name):
    return '{"value_factory_class":"org.knime.core.data.v2.value.' + name + '"}'


_arrow_to_knime_primitive_types = {
    pa.int32(): _knime_primitive_type("IntValueFactory"),
    pa.int64(): _knime_primitive_type("LongValueFactory"),
    pa.string(): _knime_primitive_type("StringValueFactory"),
    pa.bool_(): _knime_primitive_type("BooleanValueFactory"),
    pa.float64(): _knime_primitive_type("DoubleValueFactory"),
    pa.null(): _knime_primitive_type("VoidValueFactory"),
}

_arrow_to_knime_primitive_list_types = {
    pa.int32(): _knime_primitive_type("IntListValueFactory"),
    pa.int64(): _knime_primitive_type("LongListValueFactory"),
    pa.string(): _knime_primitive_type("StringListValueFactory"),
    pa.bool_(): _knime_primitive_type("BooleanListValueFactory"),
    pa.float64(): _knime_primitive_type("DoubleListValueFactory"),
}

_arrow_to_knime_list_type = _knime_primitive_type("ListValueFactory")
_row_key_type = _knime_primitive_type("DefaultRowKeyValueFactory")


def _is_primitive_type(dtype):
    return is_value_factory_type(dtype) and (
        dtype.logical_type == _row_key_type
        or dtype.logical_type in _arrow_to_knime_primitive_types.values()
        or dtype.logical_type in _arrow_to_knime_primitive_list_types.values()
    )


def _unwrap_primitive_knime_extension_array(array: pa.Array) -> pa.Array:
    """
    Unpacks array if it holds primitive types (int, double, string and so on) or a
    list of primitive types. Otherwise returns the unchanged array.

    Args:
        array: A pa.Array
    """
    if (
        is_value_factory_type(array.type)
        and array.type.logical_type == _arrow_to_knime_list_type
        and _is_primitive_type(array.type.storage_type.value_type)
    ):
        # special handling for unspecific list types: we unwrap the values
        # and maintain the offsets and validity mask
        offsets = _get_offsets_with_nulls(array.storage)
        values = _unwrap_primitive_knime_extension_array(array.storage.values)
        return _create_list_array(offsets, values)
    elif _is_primitive_type(array.type):
        return array.storage
    else:
        return array


def unwrap_primitive_arrays(
    table: Union[pa.Table, pa.RecordBatch]
) -> Union[pa.Table, pa.RecordBatch]:
    arrays = [
        _apply_to_array(column, _unwrap_primitive_knime_extension_array)
        for column in table.columns
    ]

    if isinstance(table, pa.Table):
        return pa.Table.from_arrays(arrays, names=table.column_names)
    else:
        return pa.RecordBatch.from_arrays(arrays, names=table.schema.names)


def _get_wrapped_type(dtype, is_row_key):
    if is_row_key and dtype == pa.string():
        return LogicalTypeExtensionType(
            kt.get_converter(_row_key_type), dtype, _row_key_type
        )
    elif (
        not isinstance(dtype, pa.ExtensionType)
        and dtype in _arrow_to_knime_primitive_types
    ):
        logical_type = _arrow_to_knime_primitive_types[dtype]
        return LogicalTypeExtensionType(
            kt.get_converter(logical_type), dtype, logical_type
        )
    elif (
        not isinstance(dtype, pa.ExtensionType)
        and is_list_type(dtype)
        and (dtype.value_type in _arrow_to_knime_primitive_list_types)
    ):
        # We have to treat lists differently here because arrow's comparison of list types is
        # extremely strict and fails due to a mismatch in field names (which we have no control over).
        logical_type = _arrow_to_knime_primitive_list_types[dtype.value_type]
        return LogicalTypeExtensionType(
            kt.get_converter(logical_type), dtype, logical_type
        )
    elif (
        not isinstance(dtype, pa.ExtensionType)
        and is_list_type(dtype)
        and dtype.value_type == pa.null()
    ):
        # There is no special VoidList type in KNIME, so we need two extension types,
        # one for the outer list, and one for the inner void type
        inner_logical_type = _arrow_to_knime_primitive_types[dtype.value_type]
        inner_ext_type = LogicalTypeExtensionType(
            kt.get_converter(inner_logical_type), dtype.value_type, inner_logical_type
        )
        outer_logical_type = _arrow_to_knime_list_type
        outer_ext_type = LogicalTypeExtensionType(
            kt.get_converter(outer_logical_type),
            pa.list_(inner_ext_type),
            outer_logical_type,
        )
        return outer_ext_type
    else:
        return None


def _get_offsets_with_nulls(a: Union[pa.ListArray, pa.LargeListArray]):
    # We need to manually add Nones to the offset vector where missing values
    # should be in the resulting list, because a.offsets will not contain None.
    # The offset vector contains an "end" element, however the validity
    # mask does not, so it needs to be extended. Not super efficient unfortunately.
    # Too bad Arrow does not offer to create a list with offsets, values, and mask.
    null_mask = a.is_null().to_pylist() + [False]
    return pa.array(a.offsets.to_pylist(), mask=null_mask, type=a.offsets.type)


def _wrap_primitive_array(
    array: Union[pa.Array, pa.ChunkedArray], is_row_key: bool
) -> Union[pa.Array, pa.ChunkedArray]:
    wrapped_type = _get_wrapped_type(array.type, is_row_key)

    if wrapped_type is None:
        return array
    elif array.type == pa.null():
        # We would like to wrap a null vector in an extension type, but there's a bug
        # that null arrays cannot be wrapped immediately, so we create a new null vector
        # with the appropriate type. See
        # https://issues.apache.org/jira/browse/ARROW-14522
        return _apply_to_array(array, lambda a: pa.nulls(len(a), type=wrapped_type))
    elif is_list_type(array.type) and array.type.value_type == pa.null():

        def to_list_of_nulls(a):
            inner_data = pa.nulls(
                len(a.values), type=wrapped_type.storage_type.value_type
            )
            offsets = _get_offsets_with_nulls(a)
            list_data = _create_list_array(offsets, inner_data)
            return pa.ExtensionArray.from_storage(wrapped_type, list_data)

        return _apply_to_array(
            array,
            to_list_of_nulls,
        )
    else:
        return _apply_to_array(
            array, lambda a: pa.ExtensionArray.from_storage(wrapped_type, a)
        )


def wrap_primitive_arrays(
    table: Union[pa.Table, pa.RecordBatch]
) -> Union[pa.Table, pa.RecordBatch]:
    arrays = [_wrap_primitive_array(column, i == 0) for i, column in enumerate(table)]
    if isinstance(table, pa.Table):
        return pa.Table.from_arrays(arrays, names=table.column_names)
    else:
        return pa.RecordBatch.from_arrays(arrays, names=table.schema.names)
