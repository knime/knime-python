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

from typing import Union

import knime_types as kt
import knime_arrow_struct_dict_encoding as kasde
import pyarrow as pa
import pyarrow.types as pat
import numpy as np
import logging

if pa.__version__.split(".")[0] == "8":
    # due to a pyarrow bug the as_py method from an ExtensionScalar is used, which does not decode our Extension
    # Scalar (in pa >= 8). Therefore, the builtin method has to be overwritten using forbidden fruit curse magic.
    # This Fix should be removed if https://issues.apache.org/jira/browse/ARROW-13612 is resolved.
    try:
        # forbidden fruit can override types and methods built in C or cython
        from forbiddenfruit import curse

        _orig_ext_scalar_as_py = pa.lib.ExtensionScalar.as_py

        def as_py_fix(self):
            if hasattr(self, "type") and isinstance(
                self.type, LogicalTypeExtensionType
            ):  # if we have an extension type
                return self.type.decode(self.value.as_py())  # use our own decode
            return _orig_ext_scalar_as_py(
                self
            )  # else use the usual ExtensionScalar as_py

        curse(pa.lib.ExtensionScalar, "as_py", as_py_fix)  # swap methods

    except ImportError:
        logging.info(
            f"Using pyarrow with version {pa.__version__}  can result in errors when using pyarrow "
            f"Extension types."
        )


def _pretty_type_string(dtype: pa.DataType):
    if is_dict_encoded_value_factory_type(dtype):
        return f"StructDictEncoded<key={dtype.key_type}>[{_pretty_type_string(dtype.value_type)}]"
    elif is_value_factory_type(dtype):
        # return f"LogicalType<{dtype.logical_type}>[{_pretty_type_string(dtype.storage_type)}]"
        return f"LogicalType[{_pretty_type_string(dtype.storage_type)}]"
    elif kasde.is_struct_dict_encoded(dtype):
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
    """
    Finds and returns the specific function to convert a pa.array of the given datatype to an extension array. Handles
    nested types (like list value factories) and dictionary encoding. This includes finding the dict decode function
    for all inner types via recursion, meaning that it resolves all structures contained in the dtype (eg it can encode
    into a StructDictEncodedLogicalTypeExtensionType containing a list of structs which contains dict encoded data).

    Args:
        dtype: dtype of the target extensiontype
    Returns:
        converter function
    """
    if is_dict_encoded_value_factory_type(
        dtype
    ):  # if datatype is a StructDictEncodedLogicalTypeExtensionType
        key_gen = kasde.DictKeyGenerator()
        # gets conversion fct for nested dtypes
        storage_fn = _get_arrow_storage_to_ext_fn(dtype.storage_type) or _identity

        # dict encodes the data
        def wrap_and_struct_dict_encode(a):
            unencoded_storage = storage_fn(a)
            encoded_storage = kasde.create_storage_for_struct_dict_encoded_array(
                unencoded_storage,
                key_gen,
                value_type=dtype.value_type,
                key_type=dtype.key_type,
            )
            return pa.ExtensionArray.from_storage(dtype, encoded_storage)

        return wrap_and_struct_dict_encode

    elif kasde.is_struct_dict_encoded(
        dtype
    ):  # if datatype is a dict encoded pa.struct type
        # this is the base case: we found the dict encoded data and return the dict encoding function
        key_gen = kasde.DictKeyGenerator()
        return lambda a: kasde.struct_dict_encode(a, key_gen, key_type=dtype.key_type)

    elif is_value_factory_type(dtype):  # if datatype is a LogicalTypeExtensionType
        # finds nested encoding function
        storage_fn = _get_arrow_storage_to_ext_fn(dtype.storage_type) or _identity
        # returns an extension array with encoded data
        return lambda a: pa.ExtensionArray.from_storage(dtype, storage_fn(a))

    elif is_list_type(dtype):  # if datatype is a pa.list type
        if not contains_knime_extension_type(dtype):
            return _identity

        value_fn = _get_arrow_storage_to_ext_fn(dtype.value_type) or _identity
        # We have to cast the returned list to the expected type because
        # otherwise some internal field will have a different name (item != $data$)
        # and make PyArrow's type conversion state that the types differ...
        return lambda a: _create_list_array(
            _get_offsets_with_nulls(a), value_fn(a.values)
        ).cast(dtype)

    elif pat.is_struct(dtype):  # if dtype is pa.struct
        if not contains_knime_extension_type(dtype):
            return _identity
        # get encoding for all contained types
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
    """
    Calls the :func:`_get_array_to_storage_fn`.

    Args:
        array: pa.array to be converted to storage array
    Returns:
        Storage array
    """
    compatibility_fn = _get_array_to_storage_fn(array.type)
    if compatibility_fn is None:
        return array
    else:
        return _apply_to_array(array, compatibility_fn)


def _get_array_to_storage_fn(dtype: pa.DataType):
    """
    Finds and returns the specific function to convert a pa.array of the given datatype to a
    storage array. For instance, decoding a dict encoded pa.array. This includes finding the dict decode function
    for all inner types via recursion, meaning that it resolves all structures contained in the dtype (eg it can decode
    a StructDictEncodedLogicalTypeExtensionType containing a list of structs which contains dict encoded data)

    Args:
        dtype: dtype of the pa.array
    Returns:
        converter function
    """
    if is_dict_encoded_value_factory_type(
        dtype
    ):  # if datatype is a StructDictEncodedLogicalTypeExtensionType
        # in this case we recursively call function with the value type of the StructDictEncodedLogicalTypeExtensionType
        # resulting in the decode function for the storage type
        # which we return, called with the storage of the logicalTypeExtensionType
        storage_fn = _get_array_to_storage_fn(dtype.value_type) or _identity
        return lambda a: storage_fn(a.dictionary_decode())

    elif kasde.is_struct_dict_encoded(
        dtype
    ):  # if datatype is a dict encoded pa.struct type
        # this is the base case: we found the dict encoded data and return the dict decoding function
        return lambda a: a.dictionary_decode()

    elif is_value_factory_type(dtype):  # if datatype is a LogicalTypeExtensionType
        # in this case we recursively call function with the storage type of the LogicalTypeExtensionType
        # resulting in the decode function for the storage type
        # which we return, called with the storage of the logicalTypeExtensionType
        storage_fn = _get_array_to_storage_fn(dtype.storage_type) or _identity
        return lambda a: storage_fn(a.storage)

    elif is_list_type(dtype):  # if datatype is a pa.list type
        value_fn = _get_array_to_storage_fn(dtype.value_type)
        if value_fn is None:
            # in this case we have no way of decoding the content  of the list
            return None
        else:
            # as the list structure is decoded with offset ( start indices of all contained lists)
            # calculate the offsets with missing elements
            # create a pa.listArray from the offsets and decoded values
            return lambda a: _create_list_array(
                _get_offsets_with_nulls(a), value_fn(a.values)
            )

    elif pat.is_struct(dtype):  # if dtype is pa.struct
        # get decoding for all contained types
        inner_fns = [_get_array_to_storage_fn(inner.type) for inner in dtype]
        if all(i is None for i in inner_fns):
            return None
        else:
            inner_fns = [_identity if fn is None else fn for fn in inner_fns]

            def _to_storage_struct(struct_array: pa.StructArray):
                # we apply every inner function to every field in the struct array
                inner = [fn(struct_array.field(i)) for i, fn in enumerate(inner_fns)]
                # we convert the list back to a pa.StructArray
                return pa.StructArray.from_arrays(
                    inner, names=[field.name for field in dtype]
                )

            # return the function that decodes all contained datatypes
            return _to_storage_struct
    else:
        # else we do not support the given dtype
        return None


def contains_knime_extension_type(dtype: pa.DataType):
    if (
        is_value_factory_type(dtype)
        or kasde.is_struct_dict_encoded(dtype)
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
    if kasde.is_struct_dict_encoded(dtype) or is_dict_encoded_value_factory_type(dtype):
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
    elif kasde.is_struct_dict_encoded(dtype):
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
    """
    Wraps a pa.array in an extension array.

    Args:
        data: pa.array or pa.chunkedArray
        target_type: type of data
    Returns:
        wrapped data
    """
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
    """
    The LogicalTypeExtensionType encodes the KNIME logical type - given in form of a
    Java ValueFactory class name - so that it can be used as Arrow ExtensionType.
    The storage type is the type of the values stored in the "physical" Arrow table.

    A converter can be given to convert the value during read and write access,
    the converter is an instance of a PythonValueFactory.
    """

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
        """
        Specify which exension array class to use when creating pyarrow arrays of this type
        """
        return KnimeExtensionArray

    def __arrow_ext_scalar_class__(self):
        """
        Specify which scalar class to use when accessing individual values of this type.
        This feature was introduced in pyarrow 9. Before that, we were using
        KnimeExtensionScalar (see below), but that stopped working in pyarrow 8 due to
        their internal treatment of ExtensionArrays.

        We define a special scalar type that references this logical type when this method is
        called, the scalar invokes the decode method of this logical type in as_py().
        """

        class LogicalTypeExtensionScalar(pa.ExtensionScalar):
            _ext_type = self

            def as_py(self):
                return self._ext_type.decode(self.value.as_py())

        return LogicalTypeExtensionScalar

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


class ProxyExtensionType(LogicalTypeExtensionType):
    """
    A proxy extension type is similar to a LogicalTypeExtensionType, but has a PythonValueFactory as
    converter that is not the one that is registered for the data type by default.

    The only thing it needs to do differently from a LogicalTypeExtensionType is make sure this
    non-standard PythonValueFactory(=converter) is restored when the type is deserialized.
    """

    def __init__(self, converter, storage_type, java_value_factory):
        self._converter = converter
        self._logical_type = java_value_factory

        if type(converter) == type(kt.get_converter(java_value_factory)):
            raise TypeError(
                """
                ProxyExtensionTypes should only be created if their converter differs from the
                original value factory. This is an implementation error.
                """
            )

        pa.ExtensionType.__init__(self, storage_type, "knime.proxy_type")

    def __arrow_ext_serialize__(self):
        import pickle

        compatible_type = self._converter.compatible_type if self._converter else None
        return pickle.dumps((self._logical_type, compatible_type))

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        import pickle

        logical_type, compatible_type = pickle.loads(serialized)
        converter = kt.get_proxy_by_python_type(compatible_type)[0]

        return ProxyExtensionType(converter, storage_type, logical_type)

    @property
    def original_type(self):
        return LogicalTypeExtensionType(
            kt.get_converter(self._logical_type), self.storage_type, self._logical_type
        )


# Register our extension type with
pa.register_extension_type(LogicalTypeExtensionType(None, pa.null(), ""))
pa.register_extension_type(ProxyExtensionType(None, pa.null(), ""))


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
        return kasde.StructDictEncodedType.version()

    def __arrow_ext_serialize__(self):
        # StructDictEncodedType doesn't have any meta data
        return self.value_factory_type.__arrow_ext_serialize__()

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        struct_dict_encoded_type = (
            kasde.StructDictEncodedType.__arrow_ext_deserialize__(storage_type, b"")
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

    def __arrow_ext_scalar_class__(self):
        """
        Specify which scalar class to use when accessing individual values of this type.
        This feature was introduced in pyarrow 9. Before that, we were using
        KnimeExtensionScalar (see below), but that stopped working in pyarrow 8 due to
        their internal treatment of ExtensionArrays.

        We define a special scalar type that references this logical type when this method is
        called, the scalar invokes the decode method of this logical type in as_py().
        """

        class StructDictEncodedLogicalTypeExtensionScalar(pa.ExtensionScalar):
            def as_py(self):
                return self.value.as_py()

        return StructDictEncodedLogicalTypeExtensionScalar

    def to_pandas_dtype(self):
        from knime_arrow_pandas import PandasLogicalTypeExtensionType

        return PandasLogicalTypeExtensionType(
            self.struct_dict_encoded_type,
            self.value_factory_type.logical_type,
            self.value_factory_type._converter,
        )


pa.register_extension_type(
    StructDictEncodedLogicalTypeExtensionType(
        LogicalTypeExtensionType(None, pa.null(), ""),
        kasde.StructDictEncodedType(pa.null()),
    )
)


class StructDictEncodedLogicalTypeArray(kasde.StructDictEncodedArray):
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


def data_spec_to_arrow(data_spec):
    """
    Gives the pyarrow representation of a data spec.
    Used, for example, to get the storage_type of logical extension types.

    Args:
        data_spec:
            A dict containing the data specification.
    Returns:
        Wrapped pyarrow representation of KNIME data spec.
    """
    if isinstance(data_spec, str):
        return _primitive_type_map[data_spec]
    else:
        type_id = data_spec["type"]
        if type_id == "list":
            value_type = data_spec["inner_type"]
            return pa.large_list(data_spec_to_arrow(value_type))
        elif type_id == "struct":
            fields = [
                (str(i), data_spec_to_arrow(t))
                for (i, t) in enumerate(data_spec["inner_types"])
            ]
            return pa.struct(fields)
        else:
            raise ValueError("Invalid data_spec: " + str(data_spec))


def _struct_type_from_values(*args):
    """Utility method to create a pyarrow.struct type object for structs coming from KNIME.

    Structs coming from KNIME have no names for the children. Instead the children are named
    "0", "1", "2", ...

    Arguments:
        args: The pyarrow types for the children.
    """
    return pa.struct([pa.field(f"{i}", t.type) for i, t in enumerate(args)])


class KnimeExtensionArray(pa.ExtensionArray):
    def _get_int_item_from_struct_arr(self, storage: pa.StructArray, item: int):
        """This Method unpacks nested struct arrays and takes the value at index: item

        it recursively goes into all sub struct arrays and collects the value at item.
        :param storage: Struct array, which needs to be unpacked
        :param item: index to be searched
        :return: Storage Scalar for the value at item
        """
        storage_scalar_list = []
        for field in storage.flatten():  # we unpack each sub-array
            # if we have a nested struct array and not the final dict encoded array we recursively access its fields
            if isinstance(field, pa.StructArray) and not isinstance(
                field.type, kasde.StructDictEncodedType
            ):
                storage_scalar_list.append(
                    self._get_int_item_from_struct_arr(field, item)
                )
            else:
                storage_scalar_list.append(field[item])
        storage_scalar_type = _struct_type_from_values(*storage_scalar_list)
        storage_scalar = pa.scalar(
            tuple(i.as_py() for i in storage_scalar_list), type=storage_scalar_type
        )
        return storage_scalar

    def __getitem__(self, idx):
        if isinstance(self.storage, pa.StructArray):
            storage_scalar = self._get_int_item_from_struct_arr(self.storage, idx)
        else:
            storage_scalar = self.storage[idx]
        # TODO return Scalar once there is a customizable ExtensionScalar in pyarrow
        #  to be consistent with other pa.Arrays
        return KnimeExtensionScalar(self.type, storage_scalar)

    def __iter__(self):
        """Return a generator that iterates over extension scalars"""
        for idx in range(len(self)):
            yield KnimeExtensionScalar(self.type, self.storage[idx])

    def to_pylist(self):
        return [self.type.decode(x) for x in self.storage.to_pylist()]

    def to_pandas(self):
        # TODO use super method and pass through arguments (i.e. essentially decorate the super implementation)
        series = self.storage.to_pandas()
        return series.apply(self.type.decode, convert_dtype=False)

    def to_numpy(self, dtype=None):
        # TODO same as for to_pandas
        ndarray = self.storage.to_numpy(zero_copy_only=False)
        # TODO we might need different converters for different libraries
        return np.array([self.type.decode(x) for x in ndarray], dtype=dtype)


class KnimeExtensionScalar:
    """
    Mimics the behavior of an Arrow Scalar.

    Only used up to pyarrow 7, broken for our use case in pyarrow 8, and superceeded
    by LogicalTypeExtensionType.__arrow_ext_scalar_class__() from pyarrow 9 on.
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


def _is_knime_primitive_type(dtype):
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
        and _is_knime_primitive_type(array.type.storage_type.value_type)
    ):
        # special handling for unspecific list types: we unwrap the values
        # and maintain the offsets and validity mask
        offsets = _get_offsets_with_nulls(array.storage)
        values = _unwrap_primitive_knime_extension_array(array.storage.values)
        return _create_list_array(offsets, values)
    elif _is_knime_primitive_type(array.type):
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
    if is_row_key and dtype == pa.string():  # if we deal with the rowkey
        return LogicalTypeExtensionType(
            kt.get_converter(_row_key_type), dtype, _row_key_type
        )
    elif (
        not isinstance(dtype, pa.ExtensionType)
        and dtype in _arrow_to_knime_primitive_types
    ):
        # if it is a primitive extension type
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
    # for dictionary decoded types
    elif isinstance(dtype, pa.DictionaryType):
        logical_type = _arrow_to_knime_primitive_types[dtype.value_type]
        return LogicalTypeExtensionType(
            kt.get_converter(logical_type), dtype.value_type, logical_type
        )
    else:
        return None


def _get_offsets_with_nulls(a: Union[pa.ListArray, pa.LargeListArray]):
    # We need to manually add Nones to the offset vector where missing values
    # should be in the resulting list, because a.offsets will not contain None.
    # The offset vector contains an "end" element, however the validity
    # mask does not, so it needs to be extended. Not super efficient unfortunately.
    # Too bad Arrow does not offer to create a list with offsets, values, and mask.
    # NOTE: PyArrow 5.0 requires the mask to be a numpy array, newer PyArrow versions don't.
    null_mask = np.array(a.is_null().to_pylist() + [False])
    return pa.array(a.offsets.to_pylist(), mask=null_mask, type=a.offsets.type)


def _nulls(num_nulls: int, dtype: pa.DataType):
    # We would like to wrap a null vector in an extension type, but there's a bug
    # that null arrays cannot be wrapped immediately, so we create a new null vector
    # with the appropriate type. See
    # https://issues.apache.org/jira/browse/ARROW-14522, which is fixed in PyArrow 7.
    # We want to support PyArrow 5, which is the last version available for Python 3.6,
    # and thus have to resort to this tedious construction of a null vector with extension
    # type.
    validbits = np.packbits(np.ones(num_nulls, dtype=np.uint8), bitorder="little")
    return pa.Array.from_buffers(
        dtype,
        num_nulls,
        [pa.py_buffer(validbits)],
        null_count=num_nulls,
    )


def _wrap_primitive_array(
    array: Union[pa.Array, pa.ChunkedArray], is_row_key: bool, column_name: str
) -> Union[pa.Array, pa.ChunkedArray]:
    """
    Wraps the column array in the corresponding LogicalTypeExtensionType and returns it.

    Args:
        array:
            The pa.Array or pa.ChunkedArray which is to be wrapped.
        is_row_key:
            The wrapped_type differs if the array is_row_key.
        column_name:
            Used for error message.
    Returns:
        The wrapped array.

    Raises:
        ValueError:
            If the array.type is no LogicalTypeExtensionType / value factory type.

    """
    wrapped_type = _get_wrapped_type(array.type, is_row_key)

    if wrapped_type is None:
        if not is_value_factory_type(array.type):
            raise ValueError(
                f"Data type '{array.type}' in column '{column_name}' is not supported in KNIME Python."
                + " Please use a different data type."
            )

        if isinstance(array.type, ProxyExtensionType):
            # unpack to matching LogicalType because we don't want to save the proxy type to disk
            original_type = array.type.original_type
            return _apply_to_array(
                array,
                lambda a: pa.ExtensionArray.from_storage(original_type, a.storage),
            )
        else:
            # is already LogicalTypeExtensionType
            return array
    elif array.type == pa.null():
        return _apply_to_array(array, lambda a: _nulls(len(a), dtype=wrapped_type))
    elif is_list_type(array.type) and array.type.value_type == pa.null():

        def to_list_of_nulls(a):
            inner_data = _nulls(
                len(a.values), dtype=wrapped_type.storage_type.value_type
            )
            offsets = _get_offsets_with_nulls(a)
            list_data = _create_list_array(offsets, inner_data)
            return pa.ExtensionArray.from_storage(wrapped_type, list_data)

        return _apply_to_array(
            array,
            to_list_of_nulls,
        )
    # if we have dictionary encoding on the pyarrow site we use pa's decoding before wrapping
    elif isinstance(array.type, pa.DictionaryType):
        return _apply_to_array(
            array,
            lambda a: pa.ExtensionArray.from_storage(
                wrapped_type, a.dictionary_decode()
            ),
        )
    else:
        return _apply_to_array(
            array, lambda a: pa.ExtensionArray.from_storage(wrapped_type, a)
        )


def _check_is_rowkey(array: pa.Array):
    first_column_type = array.type
    if (
        not is_value_factory_type(first_column_type)
        or first_column_type.storage_type != pa.string()
        or first_column_type.logical_type != _row_key_type
    ):
        raise TypeError(
            "The first column must contain unique row identifiers of type 'string'"
        )


def wrap_primitive_arrays(
    table: Union[pa.Table, pa.RecordBatch]
) -> Union[pa.Table, pa.RecordBatch]:
    arrays = [
        _wrap_primitive_array(column, i == 0, table.schema.names[i])
        for i, column in enumerate(table)
    ]
    _check_is_rowkey(arrays[0])
    if isinstance(table, pa.Table):
        return pa.Table.from_arrays(arrays, names=table.column_names)
    else:
        return pa.RecordBatch.from_arrays(arrays, names=table.schema.names)
