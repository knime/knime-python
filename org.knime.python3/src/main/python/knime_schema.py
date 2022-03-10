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
from dataclasses import dataclass
from typing import List, Sequence, Union, Tuple


class KnimeType:
    # How to make sure this is not instanciated directly?
    # def __init__(self):
    #     raise RuntimeError(f"{type(self)} is not supposed to be created directly")

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


class StringType(KnimeType, SupportsDictEncoding):
    def __init__(self, dict_encoding_key_type=None):
        super(StringType, self).__init__(dict_encoding_key_type)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, StringType) and super(StringType, self).__eq__(other)

    def __str__(self) -> str:
        return "string" + super(StringType, self).__str__()

    def __hash__(self):
        return hash(str(self))


class BlobType(KnimeType, SupportsDictEncoding):
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
    def __init__(self, logical_type, storage_type):
        self._logical_type = logical_type
        self._storage_type = storage_type

    @property
    def logical_type(self):
        return self._logical_type

    @property
    def storage_type(self) -> KnimeType:
        return self._storage_type

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, ExtensionType)
            and other.logical_type == self.logical_type
            and other.storage_type == self.storage_type
        )

    def __str__(self) -> str:
        return f"extension<logical={self.logical_type}, storage={self.storage_type}>"

    def __hash__(self):
        return hash(str(self))


# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
def int32():
    return IntType()


def int64():
    return LongType()


def double():
    return DoubleType()


def string(key_type=None):
    return StringType(key_type)


def bool_():
    return BoolType()


def blob(key_type=None):
    return BlobType(key_type)


def list_(inner_type):
    return ListType(inner_type)


def struct_(*inner_types):
    return StructType(inner_types)


def logical(logical_type, storage_type):
    return ExtensionType(logical_type, storage_type)


# --------------------------------------------------------------------------------------------------------------------------------------------


class Columnar(ABC):
    """
    Defines some operations that work on Columnar structured data.
    However, a Columnar object does not hold the data itself but uses a delegate for that.
    A Columnar object can also be backed by a Schema instead of a Table, then the operations
    are only applied to create a Schema of the appropriate output type.
    """

    # TODO: make Table, Batch and Schema implement the Columnar protocol?
    # Use a protocol for this? (only supported by Python >= 3.8 though).
    # https://www.python.org/dev/peps/pep-0544/#defining-a-protocol

    def __init__(self, delegate=None, slicing=None):
        self._slicing = slicing
        self._delegate = None

    @property
    @abstractmethod
    def num_columns(self) -> int:
        """
        Returns the number of columns in the table.
        """
        pass

    @property
    @abstractmethod
    def column_names(self) -> Tuple[str, ...]:
        """
        Returns the list of column names.
        """
        pass

    @abstractmethod
    def __getitem__(self, slicing: Union[slice, List[int], List[str]]) -> "Columnar":
        """
        Creates a view of this Columnar data by slicing columns. The slicing syntax is similar to
        that of numpy arrays, but columns can also be addressed as index lists or via a list of column names.

        Args:
            column_slice
                A slice object, a list of column indices, or a list of column names.

        Returns:
            A Columnar with the remaining/reordered columns

        **Examples:**

        """
        return Columnar(self, slicing)

    def append(self, other: "Columnar") -> "Columnar":
        """
        Appends the columns of the 'other' table at the end of this table
        """
        return self.insert(other, at=self.num_columns)

    @abstractmethod
    def insert(self, other: "Columnar", at: int) -> "Columnar":
        """
        Inserts all columns of the 'other' table directly before the specified column index in this table
        """
        # TODO: create append operation? Or implement in ArrowTable, ArrowBatch and Schema directly?
        pass

    def map(
        self, func, output_col_name: str, output_type: KnimeType = None
    ) -> "Columnar":
        """
        Apply the function to each row of this table. 
        The function `func` is expected to take as many parameters as this table has columns, with the respective data types, 
        and to provide one value as output. 

        We try to parse the type annotations of the provided function. It must return a `ks.KnimeType`. If there are no type annotations,
        then the output_type of this map function must be provided.
        """
        if output_type is None:
            import inspect

            output_type = inspect.signature(func).return_annotation

        if output_type is None:
            raise ValueError("Cannot determine output type of map function")

        if not issubclass(output_type, KnimeType):
            raise TypeError(
                "Output type of a map operation must be a subclass of KnimeType"
            )

        if isinstance(self, Schema):
            return Schema(output_type)
        else:
            return self._map(func, output_col_name, output_type)

    def _map(
        self, func, output_col_name: str, output_type: KnimeType = None
    ) -> "Columnar":
        pass
        # Implement _map in ArrowReadBatch and ArrowReadTable:
        # if isinstance(self, kt.ReadBatch):
        #     # todo: apply row by row
        #     pass
        # elif isinstance(self, kt.ReadTable):
        #     out_table = kt.batch_write_table()
        #     for batch in self.batches():
        #         out_batch = batch.map(func, output_col_name, output_type)
        #         out_table.append(out_batch)
        #     return out_table


# --------------------------------------------------------------------
# Schema
# --------------------------------------------------------------------
@dataclass
class Column:
    type: KnimeType
    name: str
    metadata: str

    def __str__(self) -> str:
        metastr = "" if self.metadata is None else f", {self.metadata}"
        return f"Column<'{self.name}', {self.type}{metastr}>"


class Schema(Columnar):
    @staticmethod
    def from_columns(columns):
        types, names, metadata = zip(*[(c.type, c.name, c.metadata) for c in columns])
        return Schema(types, names, metadata)

    @staticmethod
    def from_types(types, names, metadata=None):
        return Schema(types, names, metadata)

    def __init__(self, types, names, metadata=None):
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
    def column_names(self):
        return [c.name for c in self._columns]

    @property
    def num_columns(self):
        return len(self._columns)

    def __len__(self):
        return len(self._columns)

    def __getitem__(self, index: Union[int, slice]) -> Union[Column, "Schema"]:
        if isinstance(index, int):
            if index < 0 or index > len(self._columns):
                return IndexError(
                    f"Index {index} does not exist in schema with {len(self)} columns"
                )

            # which is better, returning a column or a schema? Always returning a schema makes concatenation of operations much simpler
            return self._columns[index]
            # return Schema.from_columns([self._columns[index]])
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
        return super().append(other)

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

    def __add__(self, other: Union["Schema", Column]) -> "Schema":
        return self.append(other)

    def __str__(self) -> str:
        sep = ",\n\t"
        return f"Schema<\n\t{sep.join(str(c) for c in self._columns)}>"

    def __repr__(self) -> str:
        return str(self)

    def to_knime_dict(self) -> dict:
        return _schema_to_knime_dict(self)

    @staticmethod
    def from_knime_dict(table_schema: dict) -> "Schema":
        specs = table_schema["schema"]["specs"]
        traits = table_schema["schema"]["traits"]
        names = table_schema["columnNames"]
        metadata = table_schema["columnMetaData"]

        types = [_dict_to_knime_type(s, t) for s, t in zip(specs, traits)]
        return Schema(types, names, metadata)


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
        return logical(logical_type, storage_type)
    return storage_type


def _knime_logical_type(name):
    return '{"value_factory_class":"org.knime.core.data.v2.value.' + name + '"}'


# TODO: automatic logical_type wrapping/unwrapping?!
_knime_to_logical_type = {
    int32(): _knime_logical_type("IntValueFactory"),
    int64(): _knime_logical_type("LongValueFactory"),
    string(): _knime_logical_type("StringValueFactory"),
    bool_(): _knime_logical_type("BooleanValueFactory"),
    double(): _knime_logical_type("DoubleValueFactory"),
    # null(): _knime_logical_type("VoidValueFactory"),
}

_knime_to_logical_list_type = {
    int32(): _knime_logical_type("IntListValueFactory"),
    int64(): _knime_logical_type("LongListValueFactory"),
    string(): _knime_logical_type("StringListValueFactory"),
    bool_(): _knime_logical_type("BooleanListValueFactory"),
    double(): _knime_logical_type("DoubleListValueFactory"),
}

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
            dtype.reset_key()  # to look up the data type without key in the list
        spec = _knime_to_type_str[dtype]

    return spec, traits


def _schema_to_knime_dict(schema):
    specs, traits = zip(*[_knime_type_to_dict(c.type) for c in schema])
    return {
        "schema": {"specs": specs, "traits": traits},
        "columnNames": [c.name for c in schema],
        "columnMetaData": [c.metadata for c in schema],
    }
