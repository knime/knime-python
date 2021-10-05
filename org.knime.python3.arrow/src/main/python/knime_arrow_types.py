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

import knime_types as kt
import knime_arrow as ka
import pyarrow as pa
import pyarrow.types as pat
import numpy as np
import pandas as pd
import json


def pandas_df_to_arrow_table(data_frame):
    schema = _extract_schema(data_frame)
    # pyarrow doesn't allow to customize the conversion from pandas, so we convert the corresponding columns to storage
    # format in the pandas DataFrame
    storage_df, storage_schema = _to_storage(data_frame, schema)
    arrow_table = pa.table(storage_df, schema=storage_schema)
    return _reintroduce_extension_types(arrow_table, schema)


def _extract_schema(data_frame: pd.DataFrame):
    dtypes = data_frame.dtypes
    columns = [
        (column, _to_arrow_type(data_frame[column][0]))
        for (column, dtype) in dtypes.items()
    ]
    return pa.schema(columns)


def _to_arrow_type(first_value):
    t = type(first_value)
    if t == list or t == np.ndarray:
        inner = _to_arrow_type(first_value[0])
        return pa.list_(inner)
    elif t == np.int64:  # TODO implement remaining primitive types
        return pa.int64()
    elif t == np.int32:
        return pa.int32()
    elif t == int:
        return pa.int32()
    elif t == str:
        return pa.string()
    else:
        return _to_extension_type(t)


def _to_extension_type(dtype):
    # TODO consider more elaborate method for extension type matching e.g. based on value/structure not only type
    #  this would allow an extension type to claim a value for itself e.g. FSLocationValue could detect
    #  {'fs_category': 'foo', 'fs_specifier': 'bar', 'path': 'baz'}. Problem: What if multiple types match?
    try:
        factory_bundle = kt.get_value_factory_bundle_for_type(dtype)
        storage_type = _create_storage_type(factory_bundle)
        return ValueFactoryExtensionType(
            factory_bundle.value_factory,
            storage_type,
            factory_bundle.java_value_factory,
        )
    except KeyError:
        raise ValueError(f"The type {dtype} is unknown.")


def _create_storage_type(factory_bundle):
    arrow_type_without_dict_encoding = _data_spec_json_to_arrow(
        factory_bundle.data_spec_json
    )
    data_traits = json.loads(factory_bundle.data_traits)
    return _add_dict_encoding(arrow_type_without_dict_encoding, data_traits)


def _add_dict_encoding(arrow_type, data_traits):
    if pat.is_struct(arrow_type):
        inner = [
            (inner_field.name, _add_dict_encoding(inner_field.type, inner_traits))
            for inner_field, inner_traits in zip(arrow_type, data_traits["inner"])
        ]
        return pa.struct(inner)
    elif _is_list_type(arrow_type):
        value_type = _add_dict_encoding(arrow_type.value_type, data_traits["inner"])
        if pat.is_list(arrow_type):
            return pa.list_(value_type)
        elif pat.is_large_list(arrow_type):
            return pa.large_list(value_type)
        else:
            raise ValueError(f"Unsupported list type '{arrow_type}' encountered.")
    else:
        key_type = _get_dict_key_type(data_traits)
        if key_type is not None:
            return ka.StructDictEncodedType(arrow_type, key_type)
        else:
            return arrow_type


def _get_dict_key_type(data_traits):
    traits = data_traits["traits"]
    if "dict_encoding" in traits:
        key_type = traits["dict_encoding"]
        if key_type == "BYTE_KEY":
            return pa.uint8()
        elif key_type == "INT_KEY":
            return pa.uint32()
        elif key_type == "LONG_KEY":
            return pa.uint64()
        else:
            raise ValueError(f"Unsupported key type {key_type} encountered.")
    else:
        return None


def _to_storage(df: pd.DataFrame, schema: pa.Schema):
    storage_schema = []
    for name, arrow_type in zip(schema.names, schema.types):
        storage_series, storage_type = _series_to_storage(df[name], arrow_type)
        storage_schema.append((name, storage_type))
        df[name] = storage_series
    return df, pa.schema(storage_schema)


def _series_to_storage(series: pd.Series, arrow_type: pa.DataType):
    if _contains_ext_type(arrow_type):
        storage_type, storage_func = _storage_type_and_fn(arrow_type)
        storage_series = series.apply(storage_func)
        return storage_series, storage_type
    else:
        return series, arrow_type


def _pretty_type_string(type_: pa.DataType):
    if isinstance(type_, pa.ExtensionType):
        return f"{str(type_)}[{_pretty_type_string(type_.storage_type)}]"
    elif pat.is_struct(type_):
        return f"{[_pretty_type_string(inner.type) for inner in type_]}"
    elif _is_list_type(type_):
        return f"[{_pretty_type_string(type_.value_type)}]"
    else:
        return str(type_)


def _contains_ext_type(type_: pa.DataType):
    if is_value_factory_type(type_) or ka.is_struct_dict_encoded(type_):
        return True
    elif _is_list_type(type_):
        return _contains_ext_type(type_.value_type)
    elif pat.is_struct(type_):
        return any(_contains_ext_type(inner.type) for inner in type_)
    else:
        return False


def _storage_type_and_fn(type_: pa.DataType):
    """
    Finds the storage type and a function that converts a complex object in a pandas DataFrame into its storage
    representation
    """
    if is_value_factory_type(type_):
        storage_type, storage_fn = _storage_type_and_fn(type_.storage_type)
        return storage_type, lambda a: type_.encode(a)
    elif ka.is_struct_dict_encoded(type_):
        return type_.value_type, _identity
    elif _is_list_type(type_):
        inner_type, inner_fn = _storage_type_and_fn(type_.value_type)
        if pat.is_list(type_):
            return pa.list_(inner_type), lambda l: [inner_fn(x) for x in l]
        elif pat.is_large_list(type_):
            return pa.large_list(inner_type), lambda l: [inner_fn(x) for x in l]
    elif pat.is_struct(type_):
        inner_types, inner_fns = zip(
            *[_storage_type_and_fn(inner.type) for inner in type_]
        )
        inner_fields = [
            inner.with_type(new_type) for inner, new_type in zip(type_, inner_types)
        ]
        return pa.struct(inner_fields), lambda a: {
            inner.key: fn(inner.value) for fn, inner in zip(inner_fns, a.items())
        }
    else:
        return type_, _identity


def _reintroduce_extension_types(table: pa.Table, schema_with_ext_types: pa.Schema):
    for i, name in enumerate(schema_with_ext_types.names):
        potential_ext_type = schema_with_ext_types.types[i]
        if table.schema.types[i] != potential_ext_type:
            assert potential_ext_type is not None
            ext_array = _apply_to_array(table.column(i), _get_arrow_storage_to_ext_fn(potential_ext_type))
            table = table.set_column(i, schema_with_ext_types.field(i), ext_array)
    return table


def _get_arrow_storage_to_ext_fn(type_):
    if ka.is_struct_dict_encoded(type_):
        key_gen = ka.DictKeyGenerator()
        return lambda a: ka.struct_dict_encode(a, key_gen, key_type=type_.key_type)
    elif is_value_factory_type(type_):
        storage_fn = _get_arrow_storage_to_ext_fn(type_.storage_type) or _identity
        return lambda a: pa.ExtensionArray.from_storage(type_, storage_fn(a))
    elif _is_list_type(type_):
        value_fn = _get_arrow_storage_to_ext_fn(type_.value_type) or _identity
        return lambda a: _create_list_array(a.offsets, value_fn(a.values))
    elif pat.is_struct(type_):
        inner_fns = [_get_arrow_storage_to_ext_fn(inner.type) for inner in type_]
        return lambda a: pa.StructArray.from_arrays(
            [fn(inner) for fn, inner in zip(inner_fns, a.flatten())],
            names=[t.name for t in type_],
        )
    else:
        return _identity


def _apply_to_array(array, func):
    if isinstance(array, pa.ChunkedArray):
        return pa.chunked_array([func(chunk) for chunk in array.chunks])
    else:
        return func(array)


def arrow_table_to_pandas_df(table: pa.Table):
    logical_columns = [
        i for i, field in enumerate(table.schema) if _contains_logical_type(field.type)
    ]
    storage_table = _to_pandas_compatible_table(table)
    storage_df = storage_table.to_pandas()
    _encode_df(storage_df, logical_columns, table.schema)
    return storage_df


def _contains_logical_type(type_: pa.DataType):
    if is_value_factory_type(type_):
        return True
    elif pat.is_struct(type_):
        for inner in type_:
            if _contains_logical_type(inner):
                return True
        return False
    elif _is_list_type(type_):
        return _contains_logical_type(type_.value_type)
    else:
        return False


def _is_list_type(type_: pa.DataType):
    return pat.is_large_list(type_) or pat.is_list(type_)


def _to_pandas_compatible_table(table: pa.Table):
    arrays = []
    fields = []
    for i, field in enumerate(table.schema):
        compatible_array = _to_pandas_compatible_array(table.column(i))
        arrays.append(compatible_array)
        fields.append(field.with_type(compatible_array.type))
    schema = pa.schema(fields)
    return pa.table(arrays, schema=schema)


def _to_pandas_compatible_array(array: pa.Array):
    compatibility_fn = _get_compatibility_fn(array.type)
    if compatibility_fn is None:
        return array
    else:
        return _apply_to_array(array, compatibility_fn)


def _identity(x):
    return x


def _get_compatibility_fn(type_: pa.DataType):
    if is_value_factory_type(type_):
        storage_fn = _get_compatibility_fn(type_.storage_type) or _identity
        return lambda a: storage_fn(a.storage)
    elif ka.is_struct_dict_encoded(type_):
        return lambda a: a.dictionary_decode()
    elif _is_list_type(type_):
        value_fn = _get_compatibility_fn(type_.value_type)
        if value_fn is None:
            return None
        else:
            return lambda a: _create_list_array(
                a.offsets, _to_pandas_compatible_array(a.values)
            )
    elif pat.is_struct(type_):
        inner_fns = [_get_compatibility_fn(inner.type) for inner in type_]
        if all(i is None for i in inner_fns):
            return None
        else:
            inner_fns = [_identity if fn is None else fn for fn in inner_fns]

            def _to_pandas_compatible_struct(struct_array: pa.StructArray):
                inner = [fn(struct_array.field(i)) for i, fn in enumerate(inner_fns)]
                return pa.StructArray.from_arrays(
                    inner, names=[field.name for field in type_]
                )

            return _to_pandas_compatible_struct
    else:
        return None


def _create_list_array(offsets, values):
    offset_type = offsets.type
    if pat.is_int64(offset_type):
        return pa.LargeListArray.from_arrays(offsets, values)
    elif pat.is_int32(offset_type):
        return pa.ListArray.from_arrays(offsets, values)


def _to_storage_table(table: pa.Table, logical_columns):
    storage_table = table
    for i in logical_columns:
        field = table.schema.field(i)
        assert isinstance(
            field.type, ValueFactoryExtensionType
        ), f"Only extension type columns need encoding, not {field.type}."
        storage_field = field.with_type(field.type.storage_type)
        storage_array = _apply_to_array(storage_table.column(i), lambda c: c.storage)
        storage_table = table.set_column(i, storage_field, storage_array)
    return storage_table


def _encode_df(df: pd.DataFrame, logical_columns, schema: pa.Schema):
    for i in logical_columns:
        field = schema.field(i)
        assert isinstance(
            field.type, ValueFactoryExtensionType
        ), f"Only extension type columns need encoding, not {field.type}."
        df[field.name] = df[field.name].apply(field.type.decode)


class ValueFactoryExtensionType(pa.ExtensionType):
    def __init__(self, value_factory, storage_type, java_value_factory):
        self._value_factory = value_factory
        self._java_value_factory = java_value_factory
        pa.ExtensionType.__init__(self, storage_type, "knime.value_factory")

    def __arrow_ext_serialize__(self):
        return self._java_value_factory.encode()

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        # TODO we could avoid serializing the java_value_factory if we can get access to the extension type identifier
        java_value_factory = serialized.decode()
        value_factory = kt.get_value_factory(java_value_factory)
        return ValueFactoryExtensionType(
            value_factory, storage_type, java_value_factory
        )

    def __arrow_ext_class__(self):
        return KnimeExtensionArray

    def decode(self, storage):
        return self._value_factory.decode(storage)

    def encode(self, value):
        return self._value_factory.encode(value)

    @property
    def java_value_factory(self):
        return self._java_value_factory


# Register our extension type with
pa.register_extension_type(ValueFactoryExtensionType(None, pa.null(), ""))


def is_value_factory_type(type_: pa.DataType):
    return isinstance(type_, ValueFactoryExtensionType)


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
    "void": pa.null()
    # TODO add date & time dataspecs (see DataSpecSerializer on Java side)
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

    def __init__(self, ext_type: ValueFactoryExtensionType, storage_scalar: pa.Scalar):
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
        elif isinstance(target_type, ValueFactoryExtensionType):
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


def knime_extension_scalar(value):
    ext_type = _to_extension_type(type(value))
    py_storage = ext_type.encode(value)
    arrow_scalar = pa.scalar(py_storage)
    return KnimeExtensionScalar(ext_type, arrow_scalar)


def knime_extension_array(array):
    ext_type = _to_extension_type(type(array[0]))
    storage_type, storage_fn = _storage_type_and_fn(ext_type)
    py_list = [storage_fn(x) for x in array]
    storage_array = pa.array(py_list, type=storage_type)
    return _get_arrow_storage_to_ext_fn(ext_type)(storage_array)
