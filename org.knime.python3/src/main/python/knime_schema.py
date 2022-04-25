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
Type system and schema definition for KNIME tables.

@author Carsten Haubold, KNIME GmbH, Konstanz, Germany
"""

# --------------------------------------------------------------------
# Types
# --------------------------------------------------------------------
from abc import ABC, abstractmethod
from typing import Iterator, List, Sequence, Type, Union, Tuple
import logging

LOGGER = logging.getLogger(__name__)


class KnimeType:
    def __init__(self):
        if type(self) == KnimeType:
            raise TypeError(f"{type(self)} is not supposed to be created directly")

    def __repr__(self) -> str:
        return str(self)


class IntType(KnimeType):
    def __eq__(self, other: object) -> bool:
        return isinstance(other, IntType)

    def __str__(self) -> str:
        return "int32"

    def __hash__(self):
        return hash(str(self))


class LongType(KnimeType):
    def __eq__(self, other: object) -> bool:
        return isinstance(other, LongType)

    def __str__(self) -> str:
        return "int64"

    def __hash__(self):
        return hash(str(self))


class BoolType(KnimeType):
    def __eq__(self, other: object) -> bool:
        return isinstance(other, BoolType)

    def __str__(self) -> str:
        return "bool"

    def __hash__(self):
        return hash(str(self))


class DoubleType(KnimeType):
    def __eq__(self, other: object) -> bool:
        return isinstance(other, DoubleType)

    def __str__(self) -> str:
        return "double"

    def __hash__(self):
        return hash(str(self))


class SupportsDictEncoding:
    def __init__(self, key_type=None):
        self._key_type = key_type

    @property
    def dict_encoding_key_type(self):
        return self._key_type

    def __str__(self) -> str:
        if self._key_type is not None:
            return f"[dict_encoding={self._key_type}]"
        else:
            return ""

    def __eq__(self, other):
        return (
            isinstance(other, SupportsDictEncoding)
            and other._key_type == self._key_type
        )

    def reset_key(self):
        self._key_type = None


class StringType(SupportsDictEncoding, KnimeType):
    def __init__(self, dict_encoding_key_type=None):
        super(StringType, self).__init__(dict_encoding_key_type)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, StringType) and super(StringType, self).__eq__(other)

    def __str__(self) -> str:
        return "string" + super(StringType, self).__str__()

    def __hash__(self):
        return hash(str(self))


class BlobType(SupportsDictEncoding, KnimeType):
    def __init__(self, dict_encoding_key_type=None):
        super(BlobType, self).__init__(dict_encoding_key_type)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, BlobType) and super(BlobType, self).__eq__(other)

    def __str__(self) -> str:
        return "blob" + super(BlobType, self).__str__()

    def __hash__(self):
        return hash(str(self))


class ListType(KnimeType):
    def __init__(self, inner_type):
        if not isinstance(inner_type, KnimeType):
            raise TypeError(
                f"Cannot create list type with inner type {inner_type}, must be a KnimeType"
            )
        self._inner_type = inner_type

    @property
    def inner_type(self):
        return self._inner_type

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ListType) and self._inner_type == other._inner_type

    def __str__(self) -> str:
        return f"list<{str(self._inner_type)}>"

    def __hash__(self):
        return hash(str(self))


class StructType(KnimeType):
    def __init__(self, inner_types):
        for t in inner_types:
            if not isinstance(t, KnimeType):
                raise TypeError(
                    f"Cannot create struct type with inner type {t}, must be a KnimeType"
                )

        self._inner_types = inner_types

    @property
    def inner_types(self):
        return self._inner_types

    def __eq__(self, other: object) -> bool:
        return isinstance(other, StructType) and all(
            i == o for i, o in zip(self._inner_types, other._inner_types)
        )

    def __str__(self) -> str:
        return f"struct<{', '.join(str(t) for t in self._inner_types)}>"

    def __hash__(self):
        return hash(str(self))


class ExtensionType(KnimeType):
    """
    A KNIME extension type represents any type that is understood by KNIME but
    which has no Python KnimeType equivalent.

    An ExtensionType is linked to a type on the KNIME side through its logical_type.
    The data itself is stored using the storage_type.

    Some ExtensionTypes, such as date and time formats, are also implemented on the Python
    side. For these, the value_type property represents the Python type, and these
    extensions can be created using the helper method knime_schema.extension(value_type) below.
    """

    def __init__(self, logical_type, storage_type: KnimeType):
        self._logical_type = logical_type
        self._storage_type = storage_type

    @property
    def logical_type(self):
        """
        The JSON encoded definition of the type in KNIME
        """
        return self._logical_type

    @property
    def storage_type(self) -> KnimeType:
        """
        The KnimeType that is actually used to store the data of this extension type
        """
        return self._storage_type

    @property
    def value_type(self) -> Type:
        """
        The type of the values as they are represented in Python.
        This only returns a type if a PythonValueFactory has been registered
        for this extension type.
        """
        import knime_types as kt

        if self.logical_type not in kt._java_value_factory_to_python_type:
            raise TypeError()

        return kt._java_value_factory_to_python_type[self.logical_type]

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, ExtensionType)
            and other.logical_type == self.logical_type
            and other.storage_type == self.storage_type
        )

    def __str__(self) -> str:
        import knime_types as kt

        if self.logical_type in kt._java_value_factory_to_python_type:
            dtype = kt._java_value_factory_to_python_type[self.logical_type]
            return f"extension<{'.'.join([dtype.__module__, dtype.__name__])}>"
        else:
            return (
                f"extension<logical={self.logical_type}, storage={self.storage_type}>"
            )

    def __hash__(self):
        return hash(str(self))


# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
def int32():
    """
    Create a KNIME integer type with 32 bits
    """
    return IntType()


def int64():
    """
    Create a KNIME integer type with 64 bits
    """
    return LongType()


def double():
    """
    Create a KNIME floating point type with double precision (64 bits)
    """
    return DoubleType()


def string(key_type=None):
    """
    Create a KNIME string type.

    Args:
        key_type: 
    """
    return StringType(key_type)


def bool_():
    """
    Create a KNIME boolean type
    """
    return BoolType()


def blob(key_type=None):
    """
    Create a KNIME blob type for binary data of variable length
    """
    return BlobType(key_type)


def list_(inner_type: KnimeType):
    """
    Create a KNIME type that is a list of the given inner types

    Args:
        inner_type: The type of the elements in the list. Must be a KnimeType
    """
    return ListType(inner_type)


def struct_(*inner_types):
    """
    Create a KNIME structured data type where each given argument represents
    a field of the struct.

    Args:
        inner_types:
            The argument list of this method defines the fields 
            in this structured data type. Each inner type must be a
            KNIME type
    """
    return StructType(inner_types)


def extension(value_type):
    """
    Create a KNIME extension data type of the given Python value type.

    Args:
        value_type: 
            The type of the values inside this column. A knime_types.PythonValueFactory must be registered
            for this type.
    
    Raise:
        TypeError: if no PythonValueFactory has been registered for this value type with `knime_types.register_python_value_factory`
    """
    import knime_types as kt

    try:
        vf = kt._python_type_to_java_value_factory[value_type]

        # decode the storage type of this value_type from the info provided with the java value factory
        bundle = kt._java_value_factory_to_bundle[vf]
        specs = bundle.data_spec_json
        traits = bundle.data_traits
        return _dict_to_knime_type(specs, traits)
    except Exception as e:
        raise TypeError(
            f"Could not find registered KNIME extension type for Python type {value_type}",
            e,
        )


# --------------------------------------------------------------------
# Schema
# --------------------------------------------------------------------
class Column:
    """
    A column inside a table schema consists of the datatype, a column name
    and optional metadata.
    """

    type: KnimeType
    name: str
    metadata: str

    def __init__(self, type, name, metadata=None):
        self.type = type
        self.name = name
        self.metadata = metadata

    def __str__(self) -> str:
        metastr = "" if self.metadata is None else f", {self.metadata}"
        return f"Column<'{self.name}', {self.type}{metastr}>"

    def __eq__(self, other) -> bool:
        return (
            self.type == other.type
            and self.name == other.name
            and (
                (self.metadata is None and other.metadata is None)
                or (self.metadata == other.metadata)
            )
        )


class Schema:
    """
    A schema defines the data types and names of the columns inside a table.
    Additionally it can hold metadata for the individual columns of for the full schema.
    """

    @staticmethod
    def from_columns(columns: List[Column]):
        """Create a schema from a list of columns"""
        types, names, metadata = zip(*[(c.type, c.name, c.metadata) for c in columns])
        return Schema(types, names, metadata)

    @staticmethod
    def from_types(types: List[KnimeType], names: List[str], metadata: List = None):
        """Create a schema from a list of column data types, names and metadata"""
        return Schema(types, names, metadata)

    def __init__(self, types: List[KnimeType], names: List[str], metadata: List = None):
        """Create a schema from a list of column data types, names and metadata"""
        if not isinstance(types, Sequence) or not all(
            isinstance(t, KnimeType) for t in types
        ):
            raise TypeError(
                f"Schema expected types to be a sequence of KNIME types but got {type(types)}"
            )

        if (not isinstance(names, list) and not isinstance(names, tuple)) or not all(
            isinstance(n, str) for n in names
        ):
            raise TypeError(
                f"Schema expected names to be a sequence of strings, but got {type(names)}"
            )

        if len(types) != len(names):
            raise ValueError(
                f"Number of types must match number of names, but {len(types)} != {len(names)}"
            )

        if metadata is not None:
            if not isinstance(metadata, Sequence):
                # DOESNT WORK: or not all(m is None or isinstance(m, str) for m in metadata):
                raise TypeError(
                    "Schema expected Metadata to be None or a sequence of strings or Nones"
                )

            if len(types) != len(metadata):
                raise ValueError(
                    f"Number of types must match number of metadata fields, but {len(types)} != {len(metadata)}"
                )
        else:
            metadata = [None] * len(types)

        self._columns = [Column(t, n, m) for t, n, m in zip(types, names, metadata)]

    @property
    def column_names(self) -> List[str]:
        """Return the list of column names"""
        return [c.name for c in self._columns]

    @property
    def num_columns(self) -> int:
        """The number of columns in this schema"""
        return len(self._columns)

    def __len__(self) -> int:
        """The number of columns in this schema"""
        return len(self._columns)

    def __iter__(self) -> Iterator[Column]:
        """Allow iteration over columns"""
        for c in self._columns:
            yield c

    def __getitem__(self, index: Union[int, slice]) -> Union[Column, "Schema"]:
        """
        Select columns from this schema. If the index is a single integer, a single column will be
        returned. If the index is a slice, then a new schema is returned.
        
        Args:
            index:
                An integer index specifying a single column or a slice of columns.

        Returns:
            The selected column if the index is an integer, or a new schema with the columns 
            selected by the slice.

        Raises:
            IndexError: if the index is invalid
            TypeError: if the index is neither an int nor a slice
            
        **Examples:**

        Get a specific column:
        ``my_col = schema[3]``

        Get a slice of columns:
        ``sliced_schema = schema[1:4]``
        """
        # TODO: better slicing support will be added when the functional API prototype is finished,
        #       see https://knime-com.atlassian.net/browse/AP-18642 or test_functional_table_api.py
        if isinstance(index, int):
            if index < 0 or index > len(self._columns):
                return IndexError(
                    f"Index {index} does not exist in schema with {len(self)} columns"
                )

            return self._columns[index]
        elif isinstance(index, slice):
            return Schema.from_columns(self._columns[index])
        else:
            return TypeError(
                f"Schema can only be indexed by int or slice, not {type(index)}"
            )

    def __eq__(self, other) -> bool:
        if not isinstance(other, Schema):
            return False

        return all(a == b for a, b in zip(self._columns, other._columns))

    def append(self, other: Union["Schema", Column]) -> "Schema":
        """Create a new schema by adding another schema or a column to the end"""
        return self.insert(other, at=self.num_columns)

    def insert(self, other: Union["Schema", Column], at: int) -> "Schema":
        """Create a new schema by inserting another schema or a column right before the given position"""
        if isinstance(other, Schema):
            cols = self._columns.copy()
            # this fancy syntax inserts an expanded list at an index
            cols[at:at] = other._columns
            return Schema.from_columns(cols)
        elif isinstance(other, Column):
            cols = self._columns.copy()
            cols.insert(at, other)
            return Schema.from_columns(cols)
        else:
            raise ValueError("Can only append columns or schemas to this schema")

    def __str__(self) -> str:
        sep = ",\n\t"
        return f"Schema<\n\t{sep.join(str(c) for c in self._columns)}>"

    def __repr__(self) -> str:
        return str(self)

    def to_knime_dict(self) -> dict:
        """
        Convert this Schema into dict which can then be JSON encoded and sent to KNIME
        as result of a node's configure() method.

        Because KNIME expects a row key column as first column of the schema but we don't
        include this in the KNIME Python table schema, we insert a row key column here.
        """
        row_key_type = ExtensionType(_row_key_type, string())
        schema_with_row_key = _wrap_primitive_types(self).insert(
            Column(row_key_type, "RowKey"), at=0
        )
        return _schema_to_knime_dict(schema_with_row_key)

    @staticmethod
    def from_knime_dict(table_schema: dict) -> "Schema":
        """
        Construct a Schema from a dict that was retrieved from KNIME in JSON encoded form
        as the input to a node's configure() method.

        KNIME provides table information with a RowKey column at the beginning, which we drop before
        returning the created schema.
        """
        specs = table_schema["schema"]["specs"]
        traits = table_schema["schema"]["traits"]
        names = table_schema["columnNames"]
        metadata = table_schema["columnMetaData"]

        types = [_dict_to_knime_type(s, t) for s, t in zip(specs, traits)]
        row_key_type = ExtensionType(_row_key_type, string())
        if types[0] == row_key_type:
            schema_without_row_key = Schema(types[1:], names[1:], metadata[1:])
        else:
            LOGGER.warn(
                "Did not find RowKey column when creating Schema from KNIME dict"
            )
            schema_without_row_key = Schema(types, names, metadata)
        return _unwrap_primitive_types(schema_without_row_key)


# ---------------------------------------------------------------------------------
# Logical Type handling


def _knime_logical_type(name):
    return '{"value_factory_class":"org.knime.core.data.v2.value.' + name + '"}'


_row_key_type = _knime_logical_type("DefaultRowKeyValueFactory")

_knime_type_to_logical_type = {
    int32(): _knime_logical_type("IntValueFactory"),
    int64(): _knime_logical_type("LongValueFactory"),
    string(): _knime_logical_type("StringValueFactory"),
    bool_(): _knime_logical_type("BooleanValueFactory"),
    double(): _knime_logical_type("DoubleValueFactory"),
    # null(): _knime_logical_type("VoidValueFactory"),
    list_(int32()): _knime_logical_type("IntListValueFactory"),
    list_(int64()): _knime_logical_type("LongListValueFactory"),
    list_(string()): _knime_logical_type("StringListValueFactory"),
    list_(bool_()): _knime_logical_type("BooleanListValueFactory"),
    list_(double()): _knime_logical_type("DoubleListValueFactory"),
}
_logical_type_to_knime_type = dict(
    (l, k) for k, l in _knime_type_to_logical_type.items()
)
_logical_list_type = _knime_logical_type("ListValueFactory")


def _unwrap_primitive_type(dtype: KnimeType) -> KnimeType:
    # TODO: also unwrap struct types?
    # extension types don't need unwrapping, we use the PythonValueFactory in our ks.ExtensionType if available
    import knime_types as kt

    if (
        isinstance(dtype, ExtensionType)
        and dtype.logical_type in _logical_type_to_knime_type
    ):
        dtype = _logical_type_to_knime_type[dtype.logical_type]
    elif (
        isinstance(dtype, ExtensionType)
        and dtype.logical_type == _logical_list_type
        and isinstance(dtype.storage_type, ListType)
    ):
        dtype = list_(_unwrap_primitive_type(dtype.storage_type.inner_type))
    return dtype


def _unwrap_primitive_types(schema: Schema) -> Schema:
    """
    A table schema as it is coming from KNIME contains all columns as "extension types",
    because they have a logical type trait (and java_value_factory) attached to it. 
    Here we unwrap all extension types that are known to us and present them as
    primitive types to our users.
    """
    unwrapped_columns = []
    for c in schema:
        c.type = _unwrap_primitive_type(c.type)
        unwrapped_columns.append(c)
    return Schema.from_columns(unwrapped_columns)


def _wrap_primitive_type(dtype: KnimeType) -> KnimeType:
    # TODO: also wrap struct types?
    # no need to wrap extension types -> happens in ks.extension(value_type)
    import knime_types as kt

    if dtype in _knime_type_to_logical_type:
        dtype = ExtensionType(_knime_type_to_logical_type[dtype], dtype)
    elif isinstance(dtype, ListType):
        wrapped_inner = _wrap_primitive_type(dtype.inner_type)
        dtype = ExtensionType(_logical_list_type, list_(wrapped_inner))
    return dtype


def _wrap_primitive_types(schema: Schema) -> Schema:
    """
    Given a schema with primitive types we wrap all columns - with types that
    we understand - in extension types to attach a logical type (including a java_value_factory)
    to them. This is needed to be able to read the Schema on the KNIME side.
    """
    wrapped_columns = []
    for c in schema:
        c.type = _wrap_primitive_type(c.type)
        wrapped_columns.append(c)
    return Schema.from_columns(wrapped_columns)


# ---------------------------------------------------------------------------------
# Serialization helpers


def _create_knime_type_from_id(type_id):
    if type_id == "string":
        return string()
    elif type_id == "int":
        return int32()
    elif type_id == "long":
        return int64()
    elif type_id == "double":
        return double()
    elif type_id == "boolean":
        return bool_()
    elif type_id == "variable_width_binary":
        return blob()


def _dict_to_knime_type(spec, traits):
    if traits["type"] == "simple":
        if "traits" in traits and "dict_encoding" in traits["traits"]:
            # annotate types for dict encoding
            if spec == "string":
                return string(traits["traits"]["dict_encoding"])
            if spec == "variable_width_binary":
                return blob(traits["traits"]["dict_encoding"])
        storage_type = _create_knime_type_from_id(spec)
    elif traits["type"] == "list":
        assert spec["type"] == "list"
        inner_spec = spec["inner_type"]
        inner_traits = traits["inner"]
        storage_type = list_(_dict_to_knime_type(inner_spec, inner_traits))
    elif traits["type"] == "struct":
        assert spec["type"] == "struct"
        inner_specs = spec["inner_types"]
        inner_traits = traits["inner"]
        storage_type = struct_(
            *(_dict_to_knime_type(s, t) for s, t in zip(inner_specs, inner_traits))
        )

    if "traits" in traits and "logical_type" in traits["traits"]:
        logical_type = traits["traits"]["logical_type"]
        return ExtensionType(logical_type, storage_type)

    return storage_type


_knime_to_type_str = {
    int32(): "int",
    int64(): "long",
    string(): "string",
    bool_(): "boolean",
    double(): "double",
    blob(): "variable_width_binary",
}


def _knime_type_to_dict(dtype):
    traits = {}

    if isinstance(dtype, ExtensionType):
        traits["traits"] = {"logical_type": dtype.logical_type}
        dtype = dtype.storage_type
    else:
        traits["traits"] = {}

    if isinstance(dtype, ListType):
        inner_spec, inner_traits = _knime_type_to_dict(dtype.inner_type)
        traits["type"] = "list"
        traits["inner"] = inner_traits
        spec = {"type": "list", "inner_type": inner_spec}
    elif isinstance(dtype, StructType):
        inner_specs, inner_traits = zip(
            *[_knime_type_to_dict(i) for i in dtype.inner_types]
        )
        traits["type"] = "struct"
        traits["inner"] = list(inner_traits)
        spec = {"type": "struct", "inner_types": list(inner_specs)}
    else:
        traits["type"] = "simple"
        if (
            isinstance(dtype, StringType) or isinstance(dtype, BlobType)
        ) and dtype.dict_encoding_key_type is not None:
            traits["traits"]["dict_encoding"] = dtype.dict_encoding_key_type
            dtype.reset_key()  # to look up the data type without key in the list]

        try:
            spec = _knime_to_type_str[dtype]
        except:
            raise TypeError(f"Could not find spec for type: {dtype}")

    return spec, traits


def _schema_to_knime_dict(schema):
    specs, traits = zip(*[_knime_type_to_dict(c.type) for c in schema])
    return {
        "schema": {"specs": specs, "traits": traits},
        "columnNames": [c.name for c in schema],
        "columnMetaData": [c.metadata for c in schema],
    }
