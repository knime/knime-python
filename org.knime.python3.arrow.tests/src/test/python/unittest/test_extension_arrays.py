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
import unittest
import random
import string

import numpy as np
import pyarrow as pa

import knime_arrow as ka
import knime_arrow_struct_dict_encoding as kasde


def _random_string_array(
    num_rows, num_values, max_length=20, seed=0, missing_values=True
):
    random.seed(seed)

    def random_string(length):
        return "".join(random.choices(string.ascii_letters, k=length))

    def random_int(max):
        return int(random.random() * max)

    values = [random_string(random_int(max_length)) for _ in range(num_values)]
    if missing_values:
        values.append(None)

    return [values[random_int(len(values))] for _ in range(num_rows)]


def _struct_dict_encode(data):
    key_generator = kasde.DictKeyGenerator()
    return kasde.struct_dict_encode(pa.array(data), key_generator)


class AbstractArrayTest(abc.ABC):
    @abc.abstractmethod
    def create_data(self, num_rows, missing_values):
        raise NotImplementedError("Must be implemented in the subclass")

    @abc.abstractmethod
    def data_to_array(self, data):
        raise NotImplementedError("Must be implemented in the subclass")

    @abc.abstractmethod
    def expected_type(self):
        raise NotImplementedError("Must be implemented in the subclass")

    def test_type(self):
        data = self.create_data(10, False)
        array = self.data_to_array(data)
        self.assertEqual(self.expected_type(), array.type)

    def test_len(self):
        num_rows = 28
        data = self.create_data(num_rows, False)
        array = self.data_to_array(data)
        self.assertEqual(num_rows, len(array))

    def test_getitem(self):
        num_rows = 10
        data = self.create_data(num_rows, False)
        array = self.data_to_array(data)

        for i in range(num_rows):
            self.assertEqual(data[i], array[i].as_py())
            self.assertEqual(data[-i - 1], array[-i - 1].as_py())

    def test_iter(self):
        data = self.create_data(10, False)
        array = self.data_to_array(data)

        for i, v in enumerate(array):
            self.assertEqual(data[i], v.as_py())

    def test_missing(self):
        data = self.create_data(50, True)
        array = self.data_to_array(data)

        self.assertListEqual(
            [v is None for v in data],
            [not v.is_valid for v in array],
        )

    def test_is_null(self):
        data = self.create_data(10, True)
        array = self.data_to_array(data)

        self.assertListEqual(
            [v is None for v in data],
            [v.as_py() for v in array.is_null()],
        )

    def test_nullcount(self):
        data = self.create_data(10, True)
        array = self.data_to_array(data)

        self.assertEqual(sum([1 if d is None else 0 for d in data]), array.null_count)

    def test_to_pylist(self):
        data = self.create_data(10, False)
        array = self.data_to_array(data)

        array_list = array.to_pylist()
        self.assertIsInstance(array_list, list)
        self.assertListEqual(data, array_list)

    def test_to_numpy(self):
        data = self.create_data(10, False)
        array = self.data_to_array(data)

        array_np = array.to_numpy(zero_copy_only=False)
        self.assertIsInstance(array_np, np.ndarray)

        for np_val, val in zip(array_np, data):
            self.assertEqual(val, np_val)

    def test_np_array(self):
        data = self.create_data(10, False)
        array = self.data_to_array(data)

        array_np = np.array(array)
        self.assertIsInstance(array_np, np.ndarray)

        for np_val, val in zip(array_np, data):
            self.assertEqual(val, np_val)

    def create_slices(self, data):
        array = self.data_to_array(data)

        # Slicing using __getitem__
        yield array[3:8], data[3:8]
        yield array[:8], data[:8]
        yield array[3:], data[3:]
        yield array[3:8:1], data[3:8:1]
        yield array[-3:20], data[-3:20]
        yield array[-100:20], data[-100:20]

        # Slicing using slice
        yield array.slice(3), data[3:]
        yield array.slice(3, length=5), data[3 : (3 + 5)]

        # Slice of slice
        yield array[2:8][2:], data[2:8][2:]
        yield array[2:8][:4], data[2:8][:4]
        yield array[2:8][2:4], data[2:8][2:4]
        yield array[2:8][2:4:1], data[2:8][2:4:1]

        # Slice of a slice using slice
        yield array[2:8].slice(2), data[2:8][2:]
        yield array[2:8].slice(2, length=2), data[2:8][2 : (2 + 2)]

        indices = [1, 4, 5, 6, 8]
        take_data = [data[i] for i in indices]

        # Slice of take
        yield array.take(indices)[2:], take_data[2:]
        yield array.take(indices)[:4], take_data[:4]
        yield array.take(indices)[2:4], take_data[2:4]
        yield array.take(indices)[2:4:1], take_data[2:4:1]

        # Slice of take using slice
        yield array.take(indices).slice(2), take_data[2:]
        yield array.take(indices).slice(2, length=2), take_data[2 : (2 + 2)]

    def test_slice_type(self):
        data = self.create_data(10, False)
        for array_slice, _ in self.create_slices(data):
            self.assertEqual(self.expected_type(), array_slice.type)

    def test_slice_len(self):
        data = self.create_data(28, False)
        for array_slice, data_slice in self.create_slices(data):
            # NOTE: We can be sure that len(list) is correct
            self.assertEqual(len(data_slice), len(array_slice))

    def test_slice_getitem(self):
        data = self.create_data(10, False)
        for array_slice, data_slice in self.create_slices(data):
            for i in range(len(data_slice)):
                self.assertEqual(data_slice[i], array_slice[i].as_py())
                self.assertEqual(data_slice[-i - 1], array_slice[-i - 1].as_py())

    def test_slice_iter(self):
        data = self.create_data(10, False)
        for array_slice, data_slice in self.create_slices(data):
            for i, v in enumerate(array_slice):
                self.assertEqual(data_slice[i], v.as_py())

    def test_slice_missing(self):
        data = self.create_data(50, False)
        for array_slice, data_slice in self.create_slices(data):
            self.assertListEqual(
                [v is None for v in data_slice],
                [not v.is_valid for v in array_slice],
            )

    def test_slice_is_null(self):
        data = self.create_data(50, True)
        for array_slice, data_slice in self.create_slices(data):
            self.assertListEqual(
                [v is None for v in data_slice],
                [v.as_py() for v in array_slice.is_null()],
            )

    def test_slice_nullcount(self):
        data = self.create_data(50, True)
        for array_slice, data_slice in self.create_slices(data):
            self.assertEqual(
                sum([1 if d is None else 0 for d in data_slice]), array_slice.null_count
            )

    def test_slice_to_pylist(self):
        data = self.create_data(10, False)
        for array_slice, data_slice in self.create_slices(data):
            array_list = array_slice.to_pylist()
            self.assertIsInstance(array_list, list)
            self.assertListEqual(data_slice, array_list)

    def test_slice_to_numpy(self):
        data = self.create_data(10, False)
        for array_slice, data_slice in self.create_slices(data):
            array_np = array_slice.to_numpy(zero_copy_only=False)
            self.assertIsInstance(array_np, np.ndarray)

            for np_val, val in zip(array_np, data_slice):
                self.assertEqual(val, np_val)

    def test_slice_np_array(self):
        data = self.create_data(10, False)
        for array_slice, data_slice in self.create_slices(data):
            array_np = np.array(array_slice)
            self.assertIsInstance(array_np, np.ndarray)

            for np_val, val in zip(array_np, data_slice):
                self.assertEqual(val, np_val)

    def create_takes(self, data):
        array = self.data_to_array(data)

        def take_data(data, indices):
            return [data[i] if i is not None else None for i in indices]

        for indices in [[2, 1, 1, 4], [], [2, 1, None, 1, 4]]:
            np_indices = np.array(indices)
            pa_indices = pa.array(indices, type=pa.int32())

            # Simple take
            yield array.take(indices), take_data(data, indices)
            yield array.take(np_indices), take_data(data, indices)
            yield array.take(pa_indices), take_data(data, indices)

            # Take by __getitem__ with step size
            yield array[3:8:2], data[3:8:2]
            yield array[3:8:-1], data[3:8:-1]

            # Take on a slice
            yield array[3:8].take(indices), take_data(data[3:8], indices)
            yield array[3:8].take(np_indices), take_data(data[3:8], indices)
            yield array[3:8].take(pa_indices), take_data(data[3:8], indices)
            yield array[3:8][2:5:2], data[3:8][2:5:2]
            yield array[3:8][2:5:-1], data[3:8][2:5:-1]

            # Take on a take
            indices2 = [4, 1, 1, 2, 5, 1, 3, 2, 4, 0]
            array_take = array.take(indices2)
            data_take = take_data(data, indices2)
            yield array_take.take(indices), take_data(data_take, indices)
            yield array_take.take(np_indices), take_data(data_take, indices)
            yield array_take.take(pa_indices), take_data(data_take, indices)
            yield array_take[3:8:2], data_take[3:8:2]
            yield array_take[3:8:-1], data_take[3:8:-1]

    def test_take_type(self):
        data = self.create_data(10, False)
        for array_take, _ in self.create_takes(data):
            self.assertEqual(self.expected_type(), array_take.type)

    def test_take_len(self):
        data = self.create_data(28, False)
        for array_take, data_take in self.create_takes(data):
            # NOTE: We can be sure that len(list) is correct
            self.assertEqual(len(data_take), len(array_take))

    def test_take_getitem(self):
        data = self.create_data(10, False)
        for array_take, data_take in self.create_takes(data):
            for i in range(len(data_take)):
                self.assertEqual(data_take[i], array_take[i].as_py())

    def test_take_iter(self):
        data = self.create_data(10, False)
        for array_take, data_take in self.create_takes(data):
            for i, v in enumerate(array_take):
                self.assertEqual(data_take[i], v.as_py())

    def test_take_missing(self):
        data = self.create_data(50, False)
        for array_take, data_take in self.create_takes(data):
            self.assertListEqual(
                [v is None for v in data_take],
                [not v.is_valid for v in array_take],
            )

    def test_take_is_null(self):
        data = self.create_data(50, True)
        for array_take, data_take in self.create_takes(data):
            self.assertListEqual(
                [v is None for v in data_take],
                [v.as_py() for v in array_take.is_null()],
            )

    def test_take_nullcount(self):
        data = self.create_data(50, True)
        for array_take, data_take in self.create_takes(data):
            self.assertEqual(
                sum([1 if d is None else 0 for d in data_take]), array_take.null_count
            )

    def test_take_to_pylist(self):
        data = self.create_data(10, False)
        for array_take, data_take in self.create_takes(data):
            array_list = array_take.to_pylist()
            self.assertIsInstance(array_list, list)
            self.assertListEqual(data_take, array_list)

    def test_take_to_numpy(self):
        data = self.create_data(10, False)
        for array_take, data_take in self.create_takes(data):
            array_np = array_take.to_numpy(zero_copy_only=False)
            self.assertIsInstance(array_np, np.ndarray)

            for np_val, val in zip(array_np, data_take):
                self.assertEqual(val, np_val)

    def test_take_np_array(self):
        data = self.create_data(10, False)
        for array_take, data_take in self.create_takes(data):
            array_np = np.array(array_take)
            self.assertIsInstance(array_np, np.ndarray)

            for np_val, val in zip(array_np, data_take):
                self.assertEqual(val, np_val)


class StructDictArrayTest(AbstractArrayTest, unittest.TestCase):
    def create_data(self, num_rows, missing_values):
        return _random_string_array(
            num_rows, 1 if missing_values else 4, missing_values=missing_values
        )

    def data_to_array(self, data):
        return _struct_dict_encode(data)

    def expected_type(self):
        return kasde.StructDictEncodedType(pa.string())

    def test_create_from_list(self):
        self._create_and_check(
            lambda a, k: kasde.struct_dict_encode(a, k, value_type=pa.string())
        )
        self._create_and_check(lambda a, k: kasde.struct_dict_encode(a, k))

    def test_create_from_numpy(self):
        self._create_and_check(
            lambda a, k: kasde.struct_dict_encode(
                np.array(a), k, value_type=pa.string()
            )
        )
        self._create_and_check(lambda a, k: kasde.struct_dict_encode(np.array(a), k))

    def test_create_from_pyarrow(self):
        self._create_and_check(lambda a, k: kasde.struct_dict_encode(pa.array(a), k))

    def _create_and_check(self, create_fn):
        key_generator = kasde.DictKeyGenerator()

        # First array
        array = ["foo", "bar", "foo", "car", "foo", "bar", "foo"]
        dict_encoded = create_fn(array, key_generator)

        # General checks
        self.assertIsInstance(dict_encoded, kasde.StructDictEncodedArray)
        storage = dict_encoded.storage
        self.assertIsInstance(storage, pa.StructArray)
        self.assertEqual(
            kasde.knime_struct_type(pa.uint64(), pa.string()), storage.type
        )

        # Check that the keys are as expected
        keys = storage.flatten()[0].to_pylist()
        self.assertListEqual([0, 1, 0, 2, 0, 1, 0], keys)

        # Check that the entries are as expected
        entries = storage.flatten()[1].to_pylist()
        self.assertListEqual(["foo", "bar", None, "car", None, None, None], entries)

        # Second array
        array = ["aaa", "bbb", "ccc", "ccc", "bbb", "aaa", None, "bbb"]
        dict_encoded = create_fn(array, key_generator)

        # General checks
        self.assertIsInstance(dict_encoded, kasde.StructDictEncodedArray)
        storage = dict_encoded.storage
        self.assertIsInstance(storage, pa.StructArray)
        self.assertEqual(
            kasde.knime_struct_type(pa.uint64(), pa.string()), storage.type
        )

        # Check null values
        is_null = dict_encoded.is_null().to_pylist()
        self.assertListEqual(
            [False, False, False, False, False, False, True, False], is_null
        )

        # Check that the keys are as expected (should start with 3)
        keys = storage.flatten()[0].to_pylist()
        self.assertListEqual([3, 4, 5, 5, 4, 3, None, 4], keys)

        # Check that the entries are as expected
        entries = storage.flatten()[1].to_pylist()
        self.assertListEqual(
            ["aaa", "bbb", "ccc", None, None, None, None, None], entries
        )

    def test_lru_cache_dict(self):
        n = 8

        # test double setitem
        lru = kasde.LRUCacheDict(cache_len=n)
        lru[str(0)] = 1234
        lru[str(0)] = 5678
        self.assertEqual(lru[str(0)], 5678)

        # test eviction with cache len
        lru = kasde.LRUCacheDict(cache_len=n)
        for i in range(n + 1):
            lru[str(i)] = i
        correct = "LRUCacheDict([('1', 1), ('2', 2), ('3', 3), ('4', 4), ('5', 5), ('6', 6), ('7', 7), ('8', 8)])"
        self.assertEqual(str(lru), correct)

        # test lru strategy
        l = [0, 1, 2, 3, 4]
        lru = kasde.LRUCacheDict(cache_len=n)
        for i in l:
            lru[str(i)] = i
        self.assertEqual(lru[str(0)], 0)
        self.assertEqual(
            str(lru), "LRUCacheDict([('1', 1), ('2', 2), ('3', 3), ('4', 4), ('0', 0)])"
        )


if __name__ == "__main__":
    unittest.main()
