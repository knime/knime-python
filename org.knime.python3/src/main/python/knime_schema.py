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
from abc import ABC, abstractmethod, abstractclassmethod
from typing import Dict, Iterator, List, Sequence, Type, Union, Tuple
import logging
from enum import Enum, unique

import knime_types as kt

LOGGER = logging.getLogger(__name__)


class KnimeType:
    def __init__(self):
        if type(self) == KnimeType:
            raise TypeError(f"{type(self)} is not supposed to be created directly")

    def __repr__(self) -> str:
        # needed for nice list-of-type printing. __str__ should be implemented by subclasses
        return str(self)


@unique
class PrimitiveTypeId(Enum):
    """
    Primitive data types known to KNIME
    """

    INT = "int32"
    LONG = "int64"
    STRING = "string"
    DOUBLE = "double"
    BOOL = "bool"
    BLOB = "blob"


@unique
class DictEncodingKeyType(Enum):
    """
    Key types that can be used for dictionary encoding
    """

    INT = "INT_KEY"
    LONG = "LONG_KEY"
    BYTE = "BYTE_KEY"


class _PrimitiveTypeSingletonsMetaclass(type):
    """
    The metaclass for PrimitiveType makes sure that we only ever create a single instance per type
    """

    _instances_per_type = {}

    def __call__(
        cls,
        dtype: PrimitiveTypeId,
        dict_encoding_key: DictEncodingKeyType = None,
        *args,
        **kwargs,
    ):
        key = (dtype, dict_encoding_key)
        if key in cls._instances_per_type:
            return cls._instances_per_type[key]

        obj = cls.__new__(cls)
        obj.__init__(dtype, dict_encoding_key, *args, **kwargs)
        cls._instances_per_type[key] = obj
        return obj


class PrimitiveType(KnimeType, metaclass=_PrimitiveTypeSingletonsMetaclass):
    """
    Each data type is an instance of PrimitiveType. Due to the metaclass, we only
    ever create a single instance per type, so each data type is also a singleton.

    Primitive types have a type id (INT, LONG, BOOL, ...) and a dictionary encoding key
    type which is not None if the type is stored using dictionary encoding. This is currently
    only allowed for STRING and BLOB types.
    """

    def __init__(self, type_id: PrimitiveTypeId, key_type: DictEncodingKeyType = None):
        """
        Construct a PrimitiveType from a type_id and its dictionary encoding key_type.
        Multiple invocations of this constructor with the same arguments will return the
        same instance instead of a new object with the same configuration.

        Args:
            type_id: A primitive type identifier
            key_type: The key_type for dictionary encoding or None to disable dict encoding
        """
        if not isinstance(type_id, PrimitiveTypeId):
            raise TypeError(
                f"{self.__class__.__name__} expected a {PrimitiveTypeId} instance but got {type_id}"
            )
        if key_type is not None and not isinstance(key_type, DictEncodingKeyType):
            raise TypeError(
                f"key_type must be a valid DictEncodingKeyType, got {key_type}"
            )
        if (
            key_type is not None
            and type_id != PrimitiveTypeId.STRING
            and type_id != PrimitiveTypeId.BLOB
        ):
            raise TypeError(
                f"Dictionary only works for strings and binary blobs, not {type_id.value}"
            )
        self._type_id = type_id
        self._key_type = key_type

    @property
    def dict_encoding_key_type(self):
        return self._key_type

    def __str__(self) -> str:
        if self._key_type is not None:
            return f"{self._type_id.value}[dict_encoding={self._key_type.value}]"
        else:
            return self._type_id.value

    @property
    def plain_type(self):
        """
        Returns a PrimitiveType with disabled dict encoding
        """
        return PrimitiveType(self._type_id)


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
        return (
            other.__class__ == self.__class__ and self._inner_type == other._inner_type
        )

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
        return other.__class__ == self.__class__ and all(
            i == o for i, o in zip(self._inner_types, other._inner_types)
        )

    def __str__(self) -> str:
        return f"struct<{', '.join(str(t) for t in self._inner_types)}>"

    def __hash__(self):
        return hash(str(self))


class LogicalType(KnimeType):
    """
    A KNIME LogicalType allows to attach a logical meaning to an underlying physical storage_type
    type. This could e.g. be that a Python datetime object is stored as int64, so the logical_type
    is a date but the storage_type is int64.

    The logical_type attribute contains a JSON encoded description of the Java class in KNIME that
    can understand this kind of value. It is used to specify how KNIME reads data coming from Python.

    Some LogicalTypes, such as date and time formats, are also implemented on the Python
    side. For these, the value_type property represents the Python type, and these
    logical types can be created using the helper method knime_schema.logical(value_type) below.
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
        if self.logical_type not in kt._java_value_factory_to_python_type:
            raise TypeError()

        return kt._java_value_factory_to_python_type[self.logical_type]

    def __eq__(self, other: object) -> bool:
        return (
            other.__class__ == self.__class__
            and other.logical_type == self.logical_type
            and other.storage_type == self.storage_type
        )

    def __str__(self) -> str:
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
    return PrimitiveType(PrimitiveTypeId.INT)


def int64():
    """
    Create a KNIME integer type with 64 bits
    """
    return PrimitiveType(PrimitiveTypeId.LONG)


def double():
    """
    Create a KNIME floating point type with double precision (64 bits)
    """
    return PrimitiveType(PrimitiveTypeId.DOUBLE)


def bool_():
    """
    Create a KNIME boolean type
    """
    return PrimitiveType(PrimitiveTypeId.BOOL)


def string(dict_encoding_key_type: DictEncodingKeyType = None):
    """
    Create a KNIME string type.

    Args:
        dict_encoding_key_type: 
            The key type to use for dictionary encoding. If this is
            None (the default), no dictionary encoding will be used.
            Dictionary encoding helps to reduce storage space and read/write
            performance for columns with repeating values such as categorical data. 
    """
    return PrimitiveType(PrimitiveTypeId.STRING, dict_encoding_key_type)


def blob(dict_encoding_key_type: DictEncodingKeyType = None):
    """
    Create a KNIME blob type for binary data of variable length

    Args:
        dict_encoding_key_type: 
            The key type to use for dictionary encoding. If this is
            None (the default), no dictionary encoding will be used.
            Dictionary encoding helps to reduce storage space and read/write
            performance for columns with repeating values such as categorical data. 
    """
    return PrimitiveType(PrimitiveTypeId.BLOB, dict_encoding_key_type)


def list_(inner_type: KnimeType):
    """
    Create a KNIME type that is a list of the given inner types

    Args:
        inner_type: The type of the elements in the list. Must be a KnimeType
    """
    return ListType(inner_type)


def struct(*inner_types):
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


def logical(value_type):
    """
    Create a KNIME logical data type of the given Python value type.

    Args:
        value_type: 
            The type of the values inside this column. A knime_types.PythonValueFactory 
            must be registered for this type.
    
    Raise:
        TypeError: 
            if no PythonValueFactory has been registered for this value type 
            with `knime_types.register_python_value_factory`
    """
    try:
        vf = kt._python_type_to_java_value_factory[value_type]

        # decode the storage type of this value_type from the info provided with the java value factory
        bundle = kt._java_value_factory_to_bundle[vf]
        specs = bundle.data_spec_json
        traits = bundle.data_traits
        return _dict_to_knime_type(specs, traits)
    except Exception as e:
        raise TypeError(
            f"Could not find registered KNIME extension type for Python logical type {value_type}",
            e,
        )


class PortObjectSpec(ABC):
    """
    Base protocol for PortObjectSpecs.

    A PortObjectSpec must support conversion from/to a dictionary which is then
    encoded as JSON and sent to/from KNIME.
    """

    @abstractmethod
    def to_knime_dict():
        pass

    @abstractclassmethod
    def from_knime_dict(cls, data: Dict):
        pass


class BinaryPortObjectSpec(PortObjectSpec):
    """
    Port object spec for simple binary port objects.
    
    BinaryPortObjectSpecs have an ID that is used to ensure
    that only ports with equal ID can be connected.
    """

    def __init__(self, id):
        """
        Create a BinaryPortObjectSpec

        Args:
            id: The id of this binary port.
        """
        self._id = id

    @property
    def id(self):
        return self._id

    def to_knime_dict(self):
        return {"id": self._id}

    @classmethod
    def from_knime_dict(cls, data):
        return cls(data["id"])


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
        return f"{self.__class__.__name__}<'{self.name}', {self.type}{metastr}>"

    def __eq__(self, other) -> bool:
        return (
            self.type == other.type
            and self.name == other.name
            and (
                (self.metadata is None and other.metadata is None)
                or (self.metadata == other.metadata)
            )
        )


class Schema(PortObjectSpec):
    """
    A schema defines the data types and names of the columns inside a table.
    Additionally it can hold metadata for the individual columns.
    """

    @classmethod
    def from_columns(cls, columns: List[Column]):
        """Create a schema from a list of columns"""
        types, names, metadata = zip(*[(c.type, c.name, c.metadata) for c in columns])
        return cls(types, names, metadata)

    @classmethod
    def from_types(
        cls, types: List[KnimeType], names: List[str], metadata: List = None
    ):
        """Create a schema from a list of column data types, names and metadata"""
        return cls(types, names, metadata)

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

    def __len__(self) -> int:
        """The number of columns in this schema"""
        return len(self._columns)

    def __iter__(self) -> Iterator[Column]:
        """Allow iteration over columns"""
        yield from self._columns

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
            return self.__class__.from_columns(self._columns[index])
        else:
            return TypeError(
                f"Schema can only be indexed by int or slice, not {type(index)}"
            )

    def __eq__(self, other) -> bool:
        if not other.__class__ == self.__class__:
            return False

        return all(a == b for a, b in zip(self._columns, other._columns))

    def append(self, other: Union["Schema", Column]) -> "Schema":
        """Create a new schema by adding another schema or a column to the end"""
        return self.insert(other, at=len(self))

    def insert(self, other: Union["Schema", Column], at: int) -> "Schema":
        """Create a new schema by inserting another schema or a column right before the given position"""
        if isinstance(other, self.__class__):
            cols = self._columns.copy()
            # this fancy syntax inserts an expanded list at an index
            cols[at:at] = other._columns
            return self.__class__.from_columns(cols)
        elif isinstance(other, Column):
            cols = self._columns.copy()
            cols.insert(at, other)
            return self.__class__.from_columns(cols)
        else:
            raise ValueError("Can only append columns or schemas to this schema")

    def __str__(self) -> str:
        sep = ",\n\t"
        return (
            f"{self.__class__.__name__}<\n\t{sep.join(str(c) for c in self._columns)}>"
        )

    def to_knime_dict(self) -> Dict:
        """
        Convert this Schema into dict which can then be JSON encoded and sent to KNIME
        as result of a node's configure() method.

        Because KNIME expects a row key column as first column of the schema but we don't
        include this in the KNIME Python table schema, we insert a row key column here.
        """
        row_key_type = LogicalType(_row_key_type, string())
        schema_with_row_key = _wrap_primitive_types(self).insert(
            Column(row_key_type, "RowKey"), at=0
        )
        return _schema_to_knime_dict(schema_with_row_key)

    @classmethod
    def from_knime_dict(cls, table_schema: dict) -> "Schema":
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
        row_key_type = LogicalType(_row_key_type, string())
        if types[0] == row_key_type:
            schema_without_row_key = cls(types[1:], names[1:], metadata[1:])
        else:
            LOGGER.warning(
                "Did not find RowKey column when creating Schema from KNIME dict"
            )
            schema_without_row_key = cls(types, names, metadata)
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
    """
    Removes all known logical types to simplify the type.
    If the type is unknown or does not contain a logical type, it
    will be returned unmodified.
    """
    # extension types don't need unwrapping, we use the PythonValueFactory in our ks.LogicalType if available
    if (
        isinstance(dtype, LogicalType)
        and dtype.logical_type in _logical_type_to_knime_type
    ):
        dtype = _logical_type_to_knime_type[dtype.logical_type]
    elif (
        isinstance(dtype, LogicalType)
        and dtype.logical_type == _logical_list_type
        and isinstance(dtype.storage_type, ListType)
    ):
        dtype = list_(_unwrap_primitive_type(dtype.storage_type.inner_type))
    return dtype


def _unwrap_primitive_types(schema: Schema) -> Schema:
    """
    A table schema as it is coming from KNIME contains all columns as "logical types",
    because they have a logical type trait (and java_value_factory) attached to it. 
    Here we unwrap all logical types that are known to us and present them as
    primitive types to our users.
    """
    unwrapped_columns = []
    for c in schema:
        c.type = _unwrap_primitive_type(c.type)
        unwrapped_columns.append(c)
    return Schema.from_columns(unwrapped_columns)


def _wrap_primitive_type(dtype: KnimeType) -> KnimeType:
    """
    Wraps all primitive types in their according KNIME logical type.
    If the type is unknown, it will be returned unmodified.
    """
    # no need to wrap extension types -> happens in ks.logical(value_type)
    import knime_types as kt

    if dtype in _knime_type_to_logical_type:
        dtype = LogicalType(_knime_type_to_logical_type[dtype], dtype)
    elif isinstance(dtype, ListType):
        wrapped_inner = _wrap_primitive_type(dtype.inner_type)
        dtype = LogicalType(_logical_list_type, list_(wrapped_inner))
    return dtype


def _wrap_primitive_types(schema: Schema) -> Schema:
    """
    Given a schema with primitive types we wrap all columns - with types that
    we understand - in logical types to attach a "java_value_factory"
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
            key = DictEncodingKeyType(traits["traits"]["dict_encoding"])
            if spec == "string":
                return string(key)
            elif spec == "variable_width_binary":
                return blob(key)
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
        storage_type = struct(
            *(_dict_to_knime_type(s, t) for s, t in zip(inner_specs, inner_traits))
        )

    if "traits" in traits and "logical_type" in traits["traits"]:
        logical_type = traits["traits"]["logical_type"]
        return LogicalType(logical_type, storage_type)

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

    if isinstance(dtype, LogicalType):
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
            isinstance(dtype, PrimitiveType)
            and (
                dtype._type_id == PrimitiveTypeId.STRING
                or dtype._type_id == PrimitiveTypeId.BLOB
            )
            and dtype.dict_encoding_key_type is not None
        ):
            traits["traits"]["dict_encoding"] = dtype.dict_encoding_key_type.value
            dtype = (
                dtype.plain_type
            )  # to look up the data type without key in the _knime_to_type_str dict

        try:
            spec = _knime_to_type_str[dtype]
        except KeyError:
            raise KeyError(f"Could not find spec for type: {dtype}")

    return spec, traits


def _schema_to_knime_dict(schema):
    specs, traits = zip(*[_knime_type_to_dict(c.type) for c in schema])
    return {
        "schema": {"specs": specs, "traits": traits},
        "columnNames": [c.name for c in schema],
        "columnMetaData": [c.metadata for c in schema],
    }
