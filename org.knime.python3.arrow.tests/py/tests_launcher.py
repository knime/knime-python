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

import knime_gateway as kg
import knime_arrow as ka

FLOAT_COMPARISON_EPSILON = 1e-5
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


def structcomplex_test_value(r, b):
    list_length = r % 10
    int_list = [r + b + i for i in range(list_length)]
    string_list = [
        f"r:{r},b:{b},i:{i}" if i % 7 != 0 else None for i in range(list_length)
    ]
    return {"0": [{"0": a, "1": b} for a, b in zip(int_list, string_list)], "1": r + b}


def zoneddatetime_test_value(r, b):
    zone_ids = ["Etc/Universal", "Asia/Hong_Kong", "America/Los_Angeles"]
    return {
        "epochDay": r + b * 100,
        "nanoOfDay": datetime.time(microsecond=r),
        "zoneId": zone_ids[r % 3],
    }


TEST_VALUES = {
    "boolean": lambda r, b: (r + b) % 5 == 0,
    "byte": lambda r, b: (r + b) % 256 - 128,
    "double": lambda r, b: r / 10.0 + b,
    "float": lambda r, b: r / 10.0 + b,
    "int": lambda r, b: r + b,
    "long": lambda r, b: r + b * 10_000_000_000,
    "varbinary": lambda r, b: bytes([(b + i) % 256 for i in range(r % 10)]),
    "void": lambda r, b: None,
    "struct": lambda r, b: {"0": r + b, "1": f"Row: {r}, Batch: {b}"},
    "structcomplex": structcomplex_test_value,
    "list": lambda r, b: [b + r + i for i in range(r % 10)],
    "string": lambda r, b: f"Row: {r}, Batch: {b}",
    "duration": lambda r, b: {"seconds": datetime.timedelta(seconds=r), "nanos": b},
    "localdate": lambda r, b: r + b * 100,
    "localdatetime": lambda r, b: {"epochDay": r + b * 100, "nanoOfDay": r * 500 + b},
    "localtime": lambda r, b: r * 500 + b,
    "period": lambda r, b: {"years": r, "months": b % 12, "days": (r + b) % 28},
    "zoneddatetime": zoneddatetime_test_value,
}

TEST_ARRAY_TYPES = {
    "boolean": pa.BooleanArray,
    "byte": pa.Int8Array,
    "double": pa.FloatingPointArray,
    "float": pa.FloatingPointArray,
    "int": pa.Int32Array,
    "long": pa.Int64Array,
    "varbinary": pa.LargeBinaryArray,
    "void": pa.NullArray,
    "struct": pa.StructArray,
    "structcomplex": pa.StructArray,
    "list": pa.ListArray,
    "string": pa.StringArray,
    "duration": pa.StructArray,
    "localdate": pa.Int64Array,
    "localdatetime": pa.StructArray,
    "localtime": pa.Time64Array,
    "period": pa.StructArray,
    "zoneddatetime": pa.StructArray,
}

TEST_VALUE_TYPES = {
    "boolean": pa.bool_(),
    "byte": pa.int8(),
    "double": pa.float64(),
    "float": pa.float32(),
    "int": pa.int32(),
    "long": pa.int64(),
    "varbinary": pa.large_binary(),
    "void": pa.null(),
    "struct": pa.struct(
        [pa.field("0", type=pa.int32()), pa.field("1", type=pa.string())]
    ),
    "structcomplex": pa.struct(
        [
            pa.field(
                "0",
                type=pa.list_(
                    pa.field(
                        "$data$",
                        type=pa.struct(
                            [
                                pa.field("0", type=pa.int32()),
                                pa.field("1", type=pa.string()),
                            ]
                        ),
                    )
                ),
            ),
            pa.field("1", type=pa.int32()),
        ]
    ),
    "list": pa.list_(pa.field("$data$", type=pa.int32())),
    "string": pa.string(),
    "duration": pa.struct(
        [pa.field("seconds", pa.duration("s")), pa.field("nanos", pa.int32())]
    ),
    "localdate": pa.int64(),
    "localdatetime": pa.struct(
        [
            pa.field("epochDay", type=pa.int64()),
            pa.field("nanoOfDay", type=pa.time64("ns")),
        ]
    ),
    "localtime": pa.time64("ns"),
    "period": pa.struct(
        [
            pa.field("years", type=pa.int32()),
            pa.field("months", type=pa.int32()),
            pa.field("days", type=pa.int32()),
        ]
    ),
    "zoneddatetime": pa.struct(
        [
            pa.field("epochDay", type=pa.int64()),
            pa.field("nanoOfDay", type=pa.time64("ns")),
            pa.field("zoneOffset", type=pa.int32()),
            pa.field("zoneId", type=pa.string()),
        ]
    ),
}


class EntryPoint(kg.EntryPoint):
    def testTypeToPython(self, data_type, data_source):
        expected_array_type = TEST_ARRAY_TYPES[data_type]
        expected_value_type = TEST_VALUE_TYPES[data_type]

        def wrong_value_message(row, batch, expected, value):
            return f"Wrong value for row {row} in batch {batch}. Expected '{expected}', got '{value}'."

        def assert_value(r, b, v):
            expected = TEST_VALUES[data_type](r, b)

            if data_type == "void":
                # Nothing to check for void
                pass

            elif data_type == "float":
                # Compare with epsilon
                assert (
                    abs(v.as_py() - expected) < FLOAT_COMPARISON_EPSILON
                ), wrong_value_message(r, b, expected, v)

            elif data_type == "double":
                # Compare with epsilon
                assert (
                    abs(v.as_py() - expected) < DOUBLE_COMPARISON_EPSILON
                ), wrong_value_message(r, b, expected, v)

            elif data_type in ["localdatetime", "localtime"]:
                # We do not compare python values but arrow values for them
                pa_expected = pa.scalar(expected, type=TEST_VALUE_TYPES[data_type])
                assert v == pa_expected, wrong_value_message(r, b, pa_expected, v)

            elif data_type == "zoneddatetime":
                # We do not compare the zoneOffset
                pyv = v.as_py()
                expected_nano = pa.scalar(expected["nanoOfDay"], type=pa.time64("ns"))
                assert (
                    pyv["epochDay"] == expected["epochDay"]
                    and v["nanoOfDay"] == expected_nano
                    and pyv["zoneId"] == expected["zoneId"]
                ), wrong_value_message(r, b, expected, pyv)

            else:
                # Default: Compare the python values
                assert v.as_py() == expected, wrong_value_message(r, b, expected, v)

        # Check the values in one batch
        def check_batch(batch, b):
            array = batch.column(0)
            # Check length
            assert (
                len(array) == NUM_ROWS
            ), f"Array has the wrong length. Expected {NUM_ROWS} got {len(array)}."
            # Check array type
            assert isinstance(
                array, expected_array_type
            ), f"Array has wrong type. Expected '{expected_array_type}', got '{type(array)}'."
            for r, v in enumerate(array):
                if r % 13 == 0:
                    # Check that every 13th value is missing
                    assert v.as_py() is None, wrong_value_message(r, b, None, v)
                else:
                    # Check value type
                    assert (
                        v.type == expected_value_type
                    ), f"Value has wrong type. Expected '{expected_value_type}', got '{v.type}'"
                    # Check value
                    assert_value(r, b, v)

        # Loop over batches and check each value
        with kg.data_source_mapper(data_source) as source:
            for b in range(len(source)):
                check_batch(source[b], b)

    def testTypeFromPython(self, data_type, data_sink):
        with kg.data_sink_mapper(data_sink) as sink:
            # Every 13th element is missing
            mask = np.array([r % 13 == 0 for r in range(NUM_ROWS)])

            # Loop over batches
            for b in range(NUM_BATCHES):

                # Create the array and write it to the sink
                data = pa.array(
                    [TEST_VALUES[data_type](r, b) for r in range(NUM_ROWS)],
                    type=TEST_VALUE_TYPES[data_type],
                    mask=mask,
                )
                record_batch = pa.record_batch([data], ["0"])
                sink.write(record_batch)

    def testExpectedSchema(self, data_sink):
        num_batches = 2
        num_rows = 5

        with kg.data_sink_mapper(data_sink) as sink:

            # Loop over batches
            for b in range(num_batches):

                # Create some data of different types
                int_data = pa.array([i + b for i in range(num_rows)], type=pa.int32())
                string_data = pa.array(
                    [f"{i},{b}" for i in range(num_rows)], type=pa.string()
                )
                struct_data = pa.array(
                    [
                        {"0": [x for x in range(i % 3)], "1": i * 0.1}
                        for i in range(num_rows)
                    ],
                    type=pa.struct(
                        [
                            pa.field("0", type=pa.list_(pa.int32())),
                            pa.field("1", type=pa.float64()),
                        ]
                    ),
                )

                # Write the data to the sink
                batch = pa.record_batch(
                    [int_data, string_data, struct_data], ["0", "1", "2"]
                )
                sink.write(batch)

    def testMultipleInputsOutputs(self, data_sources, data_sinks):
        sources = [kg.data_source_mapper(d) for d in data_sources]
        with kg.SequenceContextManager(sources) as inputs:
            # Check the values in the inputs
            assert len(inputs) == 4
            for idx, inp in enumerate(inputs):
                if idx == 2:
                    assert isinstance(inp._reader, ka._OffsetBasedRecordBatchFileReader)
                else:
                    assert isinstance(inp._reader, pa.RecordBatchFileReader)
                assert len(inp) == 1
                batch0 = inp[0]
                assert len(batch0) == 1
                col0 = batch0[0]
                assert len(col0) == 1
                assert col0[0].as_py() == idx

        sinks = [kg.data_sink_mapper(d) for d in data_sinks]
        with kg.SequenceContextManager(sinks) as outputs:
            # Write values to the outputs
            assert len(outputs) == 5
            for idx, oup in enumerate(outputs):
                data = pa.array([idx], type=pa.int32())
                rb = pa.record_batch([data], ["0"])
                oup.write(rb)

    class Java:
        implements = ["org.knime.python3.arrow.TestUtils.ArrowTestEntryPoint"]


kg.connect_to_knime(EntryPoint())
