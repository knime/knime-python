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
@author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
"""
import abc
import functools
from collections import OrderedDict
from typing import Any, Callable
import pyarrow as pa
import numpy as np
import pyarrow.compute as pc
from knime_arrow_utils import normalize_index


###############################################################################
# HELPERS FOR CREATING CLASSES THAT ACT LIKE PYARROW.ARRAY
###############################################################################


def knime_struct_type(*args):
    """Utility method to create a pyarrow.struct type object for structs coming from KNIME.

    Structs coming from KNIME have no names for the children. Instead the children are named
    "0", "1", "2", ...

    Arguments:
        args: The pyarrow types for the children.
    """
    return pa.struct([pa.field(f"{i}", t) for i, t in enumerate(args)])


def knime_struct(*args):
    """Create an unnamed KNIME struct for the values."""
    return {f"{i}": v for i, v in enumerate(args)}


class _AbstractArray(abc.ABC):
    """Implementation of some methods of a pyarrow.Array.

    Implements:
    * #__getitem__: Delegates to #_getitem and #slice
    * #slice and #take: Create _LazyArraySlice and _LazyArrayTake
    * #__iter__: Uses #_getitem
    * #to_pylist, #to_numpy, #__array__: Uses list comprehension with iteration to get a Python/Numpy list
    * #to_string: Pretty string representation
    """

    @abc.abstractmethod
    def _null_value(self):
        raise NotImplementedError("Must be implemented by the subclass")

    @abc.abstractmethod
    def _getitem(self, index):
        raise NotImplementedError("Must be implemented by the subclass")

    # Item access

    def __getitem__(self, key):
        l = len(self)
        # Get a single element
        if isinstance(key, (int, np.integer)):
            index = normalize_index(key, l)
            return self._getitem(index)

        # Get a slice
        elif isinstance(key, slice):
            start = 0 if key.start is None else _normalize_slice_idx(key.start, l)
            stop = l if key.stop is None else _normalize_slice_idx(key.stop, l)

            if key.step is None or key.step == 1:
                # We can use #slice
                return self.slice(start, max(stop - start, 0))
            else:
                # We have to use #take
                return self.take(np.arange(start, stop, key.step))

        # Neither slice nor single element
        raise TypeError(f"key must be int or slice. Got {type(key)}.")

    def _check_slice_offset(self, offset):
        if offset < 0:
            raise IndexError("Offset must be non-negative")

    def slice(self, offset=0, length=None):
        self._check_slice_offset(offset)
        return _LazyArraySlice(self, offset, length)

    def _convert_to_pa_indices(self, indices):
        if isinstance(indices, pa.Array):
            return indices
        if len(indices) == 0:
            return pa.array([], type=pa.uint8())
        return pa.array(indices)

    def take(self, indices):
        indices = self._convert_to_pa_indices(indices)
        return _LazyArrayTake(self, indices)

    def __iter__(self):
        for i in range(len(self)):
            yield self._getitem(i)

    # Converters

    def to_pylist(self):
        return [v.as_py() for v in self]

    def to_numpy(self, zero_copy_only=True, writable=False):
        if zero_copy_only:
            raise ValueError(
                "Need to resolve dictionary encoded data by copying the data, but zero_copy_only was True"
            )
        return np.array(self.to_pylist())

    def __array__(self, dtype=None):
        values = self.to_numpy(zero_copy_only=False)
        if dtype is None:
            return values
        return values.astype(dtype)

    # String representation

    def to_string(self, indent: int = 0, window: int = 10) -> str:
        ind = " " * indent  # indent string
        lb = "\n" + ind  # linebreak + indent for next line

        def values_to_string(a):
            return "  " + f",{lb}  ".join([str(v) for v in a])

        # Print everything
        if len(self) <= window * 2:
            return f"{ind}[{lb}{values_to_string(self)}{lb}]\n"

        # Print only slices at the beginning and end
        return (
            f"{ind}[{lb}"
            + f"{values_to_string(self[:window])},"
            + f"{lb}  ...{lb}"
            + f"{values_to_string(self[-window:])}{lb}"
            + "]\n"
        )

    def __str__(self) -> str:
        return self.to_string()

    def __repr__(self) -> str:
        return f"{object.__repr__(self)}\n{str(self)}"


class _LazyArraySlice(_AbstractArray):
    """A lazy slice of an array. Shifts indices and delegates access to a delegate."""

    def __init__(self, delegate, offset=0, length=None):
        self._delegate = delegate
        self._offset = offset
        if length is None:
            self._length = len(delegate) - offset
        else:
            self._length = length

    @property
    def type(self):
        return self._delegate.type

    # NOTE: In Python >= 3.8 functools.cached_property could be used
    @property
    @functools.lru_cache()
    def null_count(self):
        is_null = self.is_null()
        if len(is_null) == 0:
            return 0
        return pc.sum(is_null).as_py()

    def _getitem(self, index):
        return self._delegate[index + self._offset]

    def _null_value(self):
        return self._delegate._null_value()

    def slice(self, offset=0, length=None):
        self._check_slice_offset(offset)
        if length is None:
            length = self._length - offset
        return _LazyArraySlice(self._delegate, self._offset + offset, length)

    def take(self, indices):
        indices = self._convert_to_pa_indices(indices)
        return _LazyArrayTake(self._delegate, pc.add(indices, self._offset))

    def __len__(self):
        return self._length

    def is_null(self):
        return self._delegate.is_null().slice(self._offset, self._length)


class _LazyArrayTake(_AbstractArray):
    """A lazy selection (#take) of an array. Maps indices and delegates access to a delegate."""

    def __init__(self, parent, indices):
        self._delegate = parent
        self._indices = indices

    @property
    def type(self):
        return self._delegate.type

    # NOTE: In Python >= 3.8 functools.cached_property could be used
    @property
    @functools.lru_cache()
    def null_count(self):
        is_null = self.is_null()
        if len(is_null) == 0:
            return 0
        return pc.sum(is_null).as_py()

    def _getitem(self, index):
        if not self._indices[index].is_valid:
            return self._null_value()
        return self._delegate[self._indices[index].as_py()]

    def _null_value(self):
        return self._delegate._null_value()

    def slice(self, offset=0, length=None):
        self._check_slice_offset(offset)
        if length is None:
            length = len(self._indices) - offset
        return _LazyArrayTake(self._delegate, self._indices[offset : (offset + length)])

    def take(self, indices):
        indices = self._convert_to_pa_indices(indices)
        return _LazyArrayTake(self._delegate, self._indices.take(indices))

    def __len__(self):
        return len(self._indices)

    def is_null(self):
        return self._delegate.is_null().take(self._indices).fill_null(True)


###############################################################################
# STRUCT BASED DICTIONARY ENCODING
###############################################################################


class DictKeyGenerator:
    """A generator for keys of a dictionary encoded Array.

    Calling the generator with values will return increasing numbers.
    """

    def __init__(self) -> None:
        self.next_key = 0

    def __call__(self, v):
        key = self.next_key
        self.next_key += 1
        return key


# TODO benchmark and make this faster
def struct_dict_encode(
    array,
    key_generator: Callable[[Any], int],
    value_type: pa.DataType = None,
    key_type: pa.DataType = None,
):
    """Create a struct based dictionary encoded array from the given data.

    Args:
        array (ArrayLike): The data which should be dictionary encoded.
        key_generator: A callable which returns keys for individual values.
            Only equal values can get the same key.
        value_type (pa.DataType, optional): The type of the values in the array.
            Can be omitted if array is a pa.Array.
        key_type (pa.DataType, optional): The type of keys to use. One of uint8, uint32 and uint64, defaults to uint64

    Returns:
        Dictionary encoded (StructDictEncodedArray)
    """
    storage = create_storage_for_struct_dict_encoded_array(
        array, key_generator, value_type, key_type
    )
    key_type = storage.field(0).type
    value_type = storage.field(1).type
    return pa.ExtensionArray.from_storage(
        StructDictEncodedType(value_type, key_type=key_type), storage
    )


def create_storage_for_struct_dict_encoded_array(
    array,
    key_generator: Callable[[Any], int],
    value_type: pa.DataType = None,
    key_type: pa.DataType = None,
):
    # Encode
    entry_to_key = {}

    mask = []
    keys = []

    # Use specialized implementation for pyarrow arrays:
    # This saves only the indices of the values and uses pc.take
    if isinstance(array, pa.Array):
        # Check that the type fits or is not given
        if value_type is not None and value_type != array.type:
            raise ValueError(
                f"The type ({value_type}) does not match the type of the array ({array.type})."
            )
        value_type = array.type  # NOSONAR: "type" is the default naming in pyarrow

        # Loop and encode
        entry_indices = []
        for idx, v in enumerate(array):
            if not v.is_valid:
                keys.append(None)
                entry_indices.append(None)
                mask.append(True)
            elif v in entry_to_key:
                # Already in this batch
                key = entry_to_key[v]

                keys.append(key)
                entry_indices.append(None)
                mask.append(False)
            else:
                # Not yet in this batch
                key = key_generator(v)
                entry_to_key[v] = key

                keys.append(key)
                entry_indices.append(idx)
                mask.append(False)

        entries_array = array.take(entry_indices)

    # Use simple implementation for other types of arrays
    else:
        # Loop and encode
        entries = []
        for v in array:
            if v is None:
                keys.append(None)
                entries.append(None)
                mask.append(True)
            elif v in entry_to_key:
                # Already in this batch
                key = entry_to_key[v]

                keys.append(key)
                entries.append(None)
                mask.append(False)
            else:
                # Not yet in this batch
                key = key_generator(v)
                entry_to_key[v] = key

                keys.append(key)
                entries.append(v)
                mask.append(False)

        if value_type is None:
            entries_array = pa.array(entries)
            value_type = entries_array.type
        else:
            entries_array = pa.array(entries, type=value_type)

    mask_array = pa.array(mask)
    if key_type is None:
        key_type = pa.uint64()
    keys_array = pa.array(keys, type=key_type)

    # Create the storage
    # NOTE pyarrow >= 5 is needed for the mask argument
    return pa.StructArray.from_arrays(
        [keys_array, entries_array], names=["0", "1"], mask=mask_array
    )


class StructDictEncodedType(pa.ExtensionType):
    def __init__(self, inner_type, key_type=None):
        if key_type is None:
            key_type = pa.uint64()
        self._key_type = key_type
        self._inner_type = inner_type
        pa.ExtensionType.__init__(
            self, knime_struct_type(key_type, inner_type), "knime.struct_dict_encoded"
        )

    @property
    def value_type(self):
        return self._inner_type

    @property
    def key_type(self):
        return self._key_type

    @staticmethod
    def version():
        """The version is stored in the Arrow field's metadata and must match the Java version of the extension type"""
        return 0

    def __arrow_ext_serialize__(self):
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        # TODO assert the storage type is as expected?
        return StructDictEncodedType(
            storage_type[1].type, key_type=storage_type[0].type
        )

    def __arrow_ext_class__(self):
        return StructDictEncodedArray

    def __arrow_ext_scalar_class__(self):
        """
        If we are using PyArrow 9 and try to access a struct dict encoded array that is wrapped inside
        a StructArray or a ChunkedArray, then PyArrow returns the type of ExtensionScalar that we provide here.

        Unfortunately, this bypasses our StructDictEncodedArray that we defined below, so we have no
        way to access other values inside the array and cannot look up the dictionary encoded value.

        For now, we raise an error in that case to make it obvious that the values cannot be accessed.
        """
        raise NotImplementedError(
            """
            Since PyArrow 8, accessing values that are dictionary encoded by KNIME is no longer supported.
            Please use PyArrow 7 or refrain from accessing values in this column.
            """
        )


def is_struct_dict_encoded(dtype: pa.DataType):
    return isinstance(dtype, StructDictEncodedType)


pa.register_extension_type(StructDictEncodedType(pa.null()))


def get_unique_array_key(array: pa.Array):
    """ extracts and combines the memory address of the buffer of a given array as key

    :param array: array for which the key is needed
    :return: tuple containing each address
    """
    buffers = array.buffers()
    keys = []
    for buffer in buffers:
        if buffer:
            keys.append(buffer.address)
        else:
            keys.append(None)
    return tuple(keys)


class LRUCacheDict(OrderedDict):
    """Dictionary with a limited length utilizing a LRU cache strategy."""

    def __init__(self, *args, cache_len: int = 128, **kwargs):
        self.cache_len = cache_len

        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        super().move_to_end(key)

        while len(self) > self.cache_len:
            oldkey = next(iter(self))
            super().__delitem__(oldkey)

    def __getitem__(self, key):
        val = super().__getitem__(key)
        super().move_to_end(key)
        return val


value_indices_per_array = LRUCacheDict()


class StructDictEncodedArray(_AbstractArray, pa.ExtensionArray):
    """A struct dictionary encoded array.

    Saves struct<dictKey: uint, dictValue>. The dictValue is only non-null if the dictKey appears first for this array.
    dictKey can be one of [uint8, uint32, uint64]
    """

    # NOTE:
    # This type cannot be used directly with pyarrow.compute.
    # However, this is not a big issue because
    # * StructDictEncoding will be used for binary data that cannot be interpreted by
    #   pyarrow.compute
    #   -> No usage for pyarrow.compute
    # * String data will use pyarrow dictionaries that work with pyarrow.compute

    # TODO(AP-17517)
    # There are some more methods on pyarrow.Array that usually use Arrow compute.
    # We should implement some or all of them.

    # TODO(AP-17517)
    # Implement a function which undos the dictionary encoding.

    # TODO(AP-17516)
    # The function _value_index should be benchmarked and optimized

    def dictionary_encode(self, *args, **kwargs):
        # Already dictionary encoded
        return self

    def _getitem(self, index):
        if self.storage.is_null()[index].as_py():
            return self._null_value()

        return self._dict_values()[self._value_index(index)]

    def _null_value(self):
        return pa.scalar(None, type=self._value_type())

    def _value_index(self, index):
        """Get the index on which the value for the array index is saved."""

        # OPTION 1:
        # Find the first index each time using pyarrow.compute
        # dict_keys = self._dict_keys()
        # dict_key = dict_keys[index]
        # return pc.index(dict_keys, dict_key).as_py()

        # OPTION 2:
        # Initialize an array with all indices like in Java
        # TODO this can probably be optimized (maybe list comprehension)
        # pyarrow re-instantiates our DictEncodedExtensionArray for every access
        # Thus, the dict keys need to be recalculated. To save them a global LRU Cache dictionary is used,
        # to map the memory addresses of the data contained in each array to its keys
        key = get_unique_array_key(self)
        if key not in value_indices_per_array:
            dict_key_to_index = {}
            value_indices = []
            for idx, dict_key in enumerate(self._dict_keys()):
                if not dict_key.is_valid:
                    # Value is missing
                    value_indices.append(None)
                elif dict_key in dict_key_to_index:
                    # We already know the index for the dict key
                    value_indices.append(dict_key_to_index[dict_key])
                else:
                    # This is the first occurrence of the dict key
                    dict_key_to_index[dict_key] = idx
                    value_indices.append(idx)
            value_indices_per_array[key] = value_indices  # TODO use pyarrow.array?
        return value_indices_per_array[key][index]

        # OPTION 3:
        # Use a Map from dictKey to index and add all dictKeys that appear before the current index
        # TODO

    def _value_type(self):
        return self.storage.type[1].type

    def _dict_keys(self):
        return self.storage.flatten()[0]

    def _dict_values(self):
        return self.storage.flatten()[1]

    def dictionary_decode(self):
        # TODO use a more sophisticated mechanism. Maybe we can convert this to an (Arrow)dictionary encoded array
        py_list = self.to_pylist()
        return pa.array(py_list, type=self._value_type())


###############################################################################
# PRIVATE HELPER FUNCTIONS
###############################################################################


def _normalize_slice_idx(index, length):
    if index < 0:
        index += length
        if index < 0:
            return 0
        return index
    elif index >= length:
        return length
    return index
