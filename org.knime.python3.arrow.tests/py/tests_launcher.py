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

import datetime

import numpy as np
import pyarrow as pa

import knime
import knime_arrow

FLOAT_COMPARISON_EPSILON = 1e-6
DOUBLE_COMPARISON_EPSILON = 1e-12

NUM_ROWS = 20
NUM_BATCHES = 3


# TODO(typextensions) There are multiple options to handle special types from Java
#
# * Not using Extension types:
#   Only map them in the Data class when accessing higher level API (like pandas).
#   We need to know the ColumnSchema to do this.
#
# * Using Extension types only in Python:
#   Only map them in the Data class after reading.
#   We need to know the ColumnSchema to do this.
#
# * Using Extension types in Python and Java:
#   Save them using an Extension type in Java and let Arrow take care of reading it
#   to the ExtenionArray.
#   We do not need to know the ColumnSchema.
#   Does not work for more special types (not our primitive types)

# class LocalDateType(pa.ExtensionType):
#
#     def __init__(self) -> None:
#         pa.ExtensionType.__init__(
#             self, pa.int64(), "org.knime.localdate")
#
#     def __arrow_ext_serialize__(self):
#         # No parameters -> no metadata needed
#         return b''
#
#     @classmethod
#     def __arrow_ext_deserialize__(self, storage_type, serialized):
#         # No metadata
#         return LocalDateType()
#
#     def __arrow_ext_class__(self):
#         return LocalDateArray
#
# class LocalDateArray(pa.ExtensionArray):
#     def to_array(self):
#         # TODO what about day-light savings and leap seconds?
#         return [datetime.datetime.utcfromtimestamp(d * 24 * 60 * 60) for d in self.storage.to_numpy()]
#
# pa.register_extension_type(LocalDateType())
# column0_datetime = pa.ExtensionArray.from_storage(
#     LocalDateType(), column0)


class EntryPoint(knime.client.EntryPoint):

    def testTypeToPython(self, data_type, data_provider):
        data = knime.data.mapDataProvider(data_provider)

        # Helper functions
        def assert_row_value(row, batch, expected, value):
            assert value == expected, \
                "Wrong value for row {} in batch {}. Expected '{}', got '{}'.".format(
                    row, batch, expected, value)

        def assert_row_value_float(row, batch, expected, value):
            assert abs(value - expected) < FLOAT_COMPARISON_EPSILON, \
                "Wrong value for row {} in batch {}. Expected '{}', got '{}'.".format(
                    row, batch, expected, value)

        def assert_row_value_double(row, batch, expected, value):
            assert abs(value - expected) < DOUBLE_COMPARISON_EPSILON, \
                "Wrong value for row {} in batch {}. Expected '{}', got '{}'.".format(
                    row, batch, expected, value)

        def assert_array_type(array, expected_type):
            assert isinstance(array, expected_type), \
                "Array has wrong type. Expected '{}', got '{}'.".format(
                    expected_type, type(array))

        # Checkers for different types
        if data_type == 'boolean':
            def check_value(r, b, v):
                assert_row_value(r, b, (r + b) % 5 == 0, v.as_py())
            expected_array_type = pa.BooleanArray

        elif data_type == 'byte':
            def check_value(r, b, v):
                assert_row_value(r, b, (r + b) % 256 - 128, v.as_py())
            expected_array_type = pa.Int8Array

        elif data_type == 'double':
            def check_value(r, b, v):
                assert pa.types.is_float64(v.type)
                assert_row_value_double(r, b, r / 10.0 + b, v.as_py())
            expected_array_type = pa.FloatingPointArray

        elif data_type == 'float':
            def check_value(r, b, v):
                assert pa.types.is_float32(v.type)
                assert_row_value_float(r, b, r / 10.0 + b, v.as_py())
            expected_array_type = pa.FloatingPointArray

        elif data_type == 'int':
            def check_value(r, b, v):
                assert_row_value(r, b, r + b, v.as_py())
            expected_array_type = pa.Int32Array

        elif data_type == 'long':
            def check_value(r, b, v):
                assert_row_value(r, b, r + b * 10_000_000_000, v.as_py())
            expected_array_type = pa.Int64Array

        elif data_type == 'varbinary':
            def check_value(r, b, v):
                assert_row_value(r, b, bytes(
                    [(b + i) % 128 for i in range(r % 10)]), v.as_py())
            expected_array_type = pa.LargeBinaryArray

        elif data_type == 'void':
            def check_value(r, b, v):
                pass
            expected_array_type = pa.NullArray

        elif data_type == 'struct':
            def check_value(r, b, v):
                assert_row_value(
                    r, b, {'0': r + b, '1': "Row: {}, Batch: {}".format(r, b)}, v.as_py())
            expected_array_type = pa.StructArray

        elif data_type == 'structcomplex':
            def check_value(r, b, v):
                list_length = r % 10
                int_list = [r + b + i for i in range(list_length)]
                string_list = [
                    'r:{},b:{},i:{}'.format(r, b, i) if i % 7 != 0 else None for i in range(list_length)
                ]
                expected = {
                    '0': [{'0': a, '1': b} for a, b in zip(int_list, string_list)],
                    '1': r + b
                }
                assert_row_value(r, b, expected, v.as_py())
            expected_array_type = pa.StructArray

        elif data_type == 'list':
            def check_value(r, b, v):
                assert_row_value(
                    r, b, [b + r + i for i in range(r % 10)], v.as_py())
            expected_array_type = pa.ListArray

        elif data_type == 'string':
            def check_value(r, b, v):
                assert_row_value(
                    r, b, "Row: {}, Batch: {}".format(r, b), v.as_py())
            expected_array_type = pa.StringArray

        elif data_type == 'duration':
            def check_value(r, b, v):
                assert_row_value(
                    r, b, {'seconds': datetime.timedelta(seconds=r), 'nanos': b}, v.as_py())
            expected_array_type = pa.StructArray

        elif data_type == 'localdate':
            def check_value(r, b, v):
                assert_row_value(r, b, r + b * 100, v.as_py())
            expected_array_type = pa.Int64Array

        elif data_type == 'localdatetime':
            def check_value(r, b, v):
                fields = [
                    pa.field('epochDay', type=pa.int64()),
                    pa.field('nanoOfDay', type=pa.time64('ns'))
                ]
                expected = pa.scalar(
                    {'epochDay': r + b * 100, 'nanoOfDay': r * 500 + b}, type=pa.struct(fields))
                assert_row_value(r, b, expected, v)
            expected_array_type = pa.StructArray

        elif data_type == 'localtime':
            def check_value(r, b, v):
                assert_row_value(
                    r, b, pa.scalar(r * 500 + b, type=pa.time64('ns')), v)
            expected_array_type = pa.Time64Array

        elif data_type == 'period':
            def check_value(r, b, v):
                assert_row_value(
                    r, b, {'years': r, 'months': b % 12, 'days': (r + b) % 28}, v.as_py())
            expected_array_type = pa.StructArray

        elif data_type == 'zoneddatetime':
            def check_value(r, b, v):
                print(v)
                assert_row_value(r, b, None, v.as_py())
            expected_array_type = pa.StructArray

        else:
            raise ValueError("Unknown type to check: '{}'.".format(data_type))

        # Check the values in one batch
        def check_batch(batch, b):
            array = batch.column(0)
            assert_array_type(array, expected_array_type)
            for r, v in enumerate(array):
                if r % 13 == 0:
                    assert_row_value(r, b, None, v.as_py())
                else:
                    check_value(r, b, v)

        # Loop over batches and check each value
        for b in range(len(data)):
            batch = data[b]
            check_batch(batch, b)
        data.close()

    def testTypeFromPython(self, data_type, data_callback):
        writer = knime.data.mapDataCallback(data_callback)

        # Writers for different types
        if data_type == 'boolean':
            def get_value(b, r):
                return (r + b) % 5 == 0
            arrow_type = pa.bool_()

        elif data_type == 'byte':
            def get_value(b, r):
                return (r + b) % 256 - 128
            arrow_type = pa.int8()

        elif data_type == 'double':
            def get_value(b, r):
                return r / 10.0 + b
            arrow_type = pa.float64()

        elif data_type == 'float':
            def get_value(b, r):
                return r / 10.0 + b
            arrow_type = pa.float32()

        elif data_type == 'int':
            def get_value(b, r):
                return r + b
            arrow_type = pa.int32()

        elif data_type == 'long':
            def get_value(b, r):
                return r + b * 10_000_000_000
            arrow_type = pa.int64()

        elif data_type == 'varbinary':
            def get_value(b, r):
                return bytes([(b + i) % 128 for i in range(r % 10)])
            arrow_type = pa.large_binary()

        elif data_type == 'void':
            def get_value(b, r):
                return None
            arrow_type = pa.null()

        elif data_type == 'struct':
            def get_value(b, r):
                return {'0': r + b, '1': 'Row: {}, Batch: {}'.format(r, b)}
            arrow_type = pa.struct([pa.field('0', type=pa.int32()),
                                    pa.field('1', type=pa.string())])

        elif data_type == 'structcomplex':
            def get_value(b, r):
                list_length = r % 10
                int_list = [r + b + i for i in range(list_length)]
                string_list = [
                    'r:{},b:{},i:{}'.format(r, b, i) if i % 7 != 0 else None for i in range(list_length)
                ]
                return {
                    '0': [{'0': a, '1': b} for a, b in zip(int_list, string_list)],
                    '1': r + b
                }
            arrow_type = pa.struct([
                pa.field('0', type=pa.list_(pa.struct([
                    pa.field('0', type=pa.int32()),
                    pa.field('1', type=pa.string())
                ]))),
                pa.field('1', type=pa.int32())
            ])

        elif data_type == 'list':
            def get_value(b, r):
                return [b + r + i for i in range(r % 10)]
            arrow_type = pa.list_(pa.int32())

        elif data_type == 'string':
            def get_value(b, r):
                return "Row: {}, Batch: {}".format(r, b)
            arrow_type = pa.string()

        elif data_type == 'duration':
            def get_value(b, r):
                return {'seconds': datetime.timedelta(seconds=r), 'nanos': b}
            arrow_type = pa.struct([pa.field('seconds', pa.duration('s')),
                                    pa.field('nanos', pa.int32())])

        elif data_type == 'localdate':
            # TODO(extensiontypes) how to say to Java that this is localdate?
            def get_value(b, r):
                return r + b * 100
            arrow_type = pa.int64()

        elif data_type == 'localdatetime':
            def get_value(b, r):
                return {'epochDay': r + b * 100, 'nanoOfDay': r * 500 + b}
            arrow_type = pa.struct([pa.field('epochDay', type=pa.int64()),
                                    pa.field('nanoOfDay', type=pa.time64('ns'))])

        elif data_type == 'localtime':
            def get_value(b, r):
                return r * 500 + b
            arrow_type = pa.time64('ns')

        elif data_type == 'period':
            def get_value(b, r):
                return {'years': r, 'months': b % 12, 'days': (r + b) % 28}
            arrow_type = pa.struct([pa.field('years', type=pa.int32()),
                                    pa.field('months', type=pa.int32()),
                                    pa.field('days', type=pa.int32())])

        elif data_type == 'zoneddatetime':
            # TODO(dictionary) implement this test
            def get_value(b, r):
                return 0
            arrow_type = None

        else:
            raise ValueError("Unknown type to check: '{}'.".format(data_type))

        # Create the data and write
        mask = np.array([r % 13 == 0 for r in range(NUM_ROWS)])
        for b in range(NUM_BATCHES):
            data = pa.array([get_value(b, r) for r in range(NUM_ROWS)],
                            type=arrow_type, mask=mask)
            record_batch = pa.record_batch([data], ['0'])
            writer.write(record_batch)

        writer.close()

    def testExpectedSchema(self, data_callback):
        num_batches = 2
        num_rows = 5

        writer = knime.data.mapDataCallback(data_callback)

        for b in range(num_batches):
            intData = pa.array(
                [i + b for i in range(num_rows)],
                type=pa.int32())
            stringData = pa.array(
                ["{},{}".format(i, b) for i in range(num_rows)],
                type=pa.string())
            structData = pa.array(
                [{'0': [x for x in range(i % 3)], '1': i * 0.1}
                 for i in range(num_rows)],
                type=pa.struct([
                    pa.field('0', type=pa.list_(pa.int32())),
                    pa.field('1', type=pa.float64())
                ])
            )
            batch = pa.record_batch(
                [intData, stringData, structData], ['0', '1', '2'])
            writer.write(batch)

        writer.close()

    def testMultipleInputsOutputs(self, data_providers, data_callbacks):
        inputs = knime.data.mapDataProviders(data_providers)
        outputs = knime.data.mapDataCallbacks(data_callbacks)

        # Check the values in the inputs
        assert len(inputs) == 4
        for idx, inp in enumerate(inputs):
            if idx == 2:
                assert isinstance(
                    inp._reader, knime_arrow._OffsetBasedRecordBatchFileReader)
            else:
                assert isinstance(inp._reader, pa.RecordBatchFileReader)
            assert len(inp) == 1
            batch0 = inp[0]
            assert len(batch0) == 1
            col0 = batch0[0]
            assert len(col0) == 1
            assert col0[0].as_py() == idx
            inp.close()

        # Write values to the outputs
        assert len(outputs) == 5
        for idx, oup in enumerate(outputs):
            data = pa.array([idx], type=pa.int32())
            rb = pa.record_batch([data], ['0'])
            oup.write(rb)
            oup.close()

    class Java:
        implements = ["org.knime.python3.arrow.TestUtils.ArrowTestEntryPoint"]


knime.client.connectToJava(EntryPoint())
