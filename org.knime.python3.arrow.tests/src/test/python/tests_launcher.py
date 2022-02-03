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
import pandas as pd  # just to load this at startup and don't count the time

import knime_arrow as ka
import knime_arrow_table as kat
import knime_arrow_struct_dict_encoding as kas
import knime_gateway as kg

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


def zoneddatetime_test_value():
    zone_ids = ["Etc/Universal", "Asia/Hong_Kong", "America/Los_Angeles"]

    def test_value(r, b):
        return {
            "epochDay": r + b * 100,
            "nanoOfDay": datetime.time(microsecond=r),
            "zoneId": zone_ids[r % 3],
        }

    return test_value


def dictstring_test_value():
    d = ["foo", "bar", "car", "aaa"]

    def test_value(r, b):
        if b == 0:
            return d[r % (len(d) - 1)]
        else:
            return d[r % len(d)]

    return test_value


def dictvarbinary_test_value():
    d = [bytes([(j + 50) % 128 for j in range(i + 1)]) for i in range(4)]

    def test_value(r, b):
        if b == 0:
            return d[r % (len(d) - 1)]
        else:
            return d[r % len(d)]

    return test_value


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
    "zoneddatetime": zoneddatetime_test_value(),
    "dictstring": dictstring_test_value(),
    "dictvarbinary": dictvarbinary_test_value(),
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
    "dictstring": kas.StructDictEncodedArray,
    "dictvarbinary": kas.StructDictEncodedArray,
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
    "dictstring": pa.string(),
    "dictvarbinary": pa.large_binary(),
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

            # TODO(typeextensions) this should happen automatically
            if data_type == "dictstring":
                array = pa.ExtensionArray.from_storage(
                    kas.StructDictEncodedType(pa.string()), array
                )
            elif data_type == "dictvarbinary":
                array = pa.ExtensionArray.from_storage(
                    kas.StructDictEncodedType(pa.large_binary()), array
                )

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

            dict_encoded = data_type in ["dictstring", "dictvarbinary"]
            if dict_encoded:
                key_gen = kas.DictKeyGenerator()

            # Loop over batches
            for b in range(NUM_BATCHES):

                # Create the array
                data = pa.array(
                    [TEST_VALUES[data_type](r, b) for r in range(NUM_ROWS)],
                    type=TEST_VALUE_TYPES[data_type],
                    mask=mask,
                )

                # Dictionary encode if necessary
                if dict_encoded:
                    data = kas.struct_dict_encode(data, key_gen)

                # Write to the sink
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

    def testRowKeyChecking(self, duplicates, data_sink):
        with kg.data_sink_mapper(data_sink) as sink:
            num_batches = 5
            b = 0
            while True:
                num_rows = 100
                keys_py = [f"Row{b}_{i}" for i in range(num_rows)]

                if duplicates == "far" and b == 3:
                    # Duplicates at batch0,row0 and batch3,row20
                    keys_py[20] = "Row0_0"
                elif duplicates == "close" and b == 0:
                    # Duplicates at batch0,row0 and batch0,row1
                    keys_py[1] = "Row0_0"

                b += 1

                # For close duplicates we loop until we get the exception
                if duplicates != "close" and b >= num_batches:
                    break

                keys = pa.array(keys_py, type=pa.string())
                data = pa.array(list(range(num_rows)), type=pa.int32())
                batch = pa.record_batch([keys, data], ["0", "1"])
                sink.write(batch)

    def testDomainCalculation(self, scenario, data_sink):
        num_batches = 5
        num_rows = 100

        if scenario == "double":
            dtype = np.dtype("float64")
            min_value = 0
            max_value = 1
            full_data = np.linspace(min_value, max_value, num=(num_rows * num_batches))

        with kg.data_sink_mapper(data_sink) as sink:
            for batch_idx in range(num_batches):
                batch_start = batch_idx * num_rows
                batch_end = batch_start + num_rows
                keys = [f"Row_{i}" for i in range(batch_start, batch_end)]
                pa_keys = pa.array(keys, type=pa.string())

                if scenario == "double":
                    data = list(full_data[batch_start:batch_end])
                    dtype = pa.float64()
                elif scenario == "int":
                    data = list(range(batch_start, batch_end))
                    dtype = pa.int32()
                elif scenario == "string":
                    data = keys
                    dtype = pa.string()
                elif scenario == "categorical":
                    data = [f"str{batch_idx}"] * num_rows
                    dtype = pa.string()
                pa_data = pa.array(data, type=dtype)
                batch = pa.record_batch([pa_keys, pa_data], ["0", "1"])
                sink.write(batch)

    # -------------------------------------------------------------------------
    # def udf(read_table) -> WriteTable:
    #     write_table = kta.WriteTable.create()

    #     for read_batch in read_table.batches():
    #         pandas_df = read_batch.to_pandas()
    #         pandas_df["0"] = pandas_df["0"] * 2
    #         pandas_df["1"] = pandas_df["1"] * 2
    #         write_batch = kata.ArrowBatch.from_pandas(pandas_df)
    #         write_table.append_batch(write_batch)

    #     return write_table

    # def exec(udf, data_source):
    #     with kg.data_source_mapper(data_source) as source:
    #         sink = udf(source)
    #         sink.close()

    # -------------------------------------------------------------------------
    # def udf2(read_table: kta.ReadTable, write_table: kta.WriteTable):
    #     for read_batch in read_table.batches():
    #         pandas_df = read_batch.to_pandas()
    #         pandas_df["0"] = pandas_df["0"] * 2
    #         pandas_df["1"] = pandas_df["1"] * 2
    #         write_batch = kta.batch(pandas_df)
    #         write_batch = kta.batch(pyarrow_recordbatch)
    #         write_table.append_batch(write_batch)
    #         # syntactic sugar
    #         write_table.append_batch(pandas_df, from_pandas_kw_args)
    #         write_table.append_batch(pyarrow_recordbatch, from_pyarrow_kw_args)

    #     p = read_table.to_pandas()
    #     ...
    #     write_table.from_pandas(p)

    # def exec2(udf, data_source, data_sink):
    #     with kg.data_source_mapper(data_source) as source:
    #         with kg.data_sink_mapper(data_sink) as sink:
    #             udf(source, sink)

    # -------------------------------------------------------------------------

    def testKnimeTable(self, data_source, sink_creator, num_rows, num_columns, mode):
        import knime_table as kta
        import knime_arrow_table as kata

        def create_python_sink():
            java_sink = sink_creator.createSink()
            return ka.ArrowDataSink(java_sink)

        kta._backend = kata.ArrowBackend(sink_creator=create_python_sink)

        expected_shape = (num_rows, num_columns)

        with kg.data_source_mapper(data_source) as source:
            read_table = kata.ArrowReadTable(source)
            assert isinstance(read_table, kta.ReadTable)
            assert isinstance(read_table, kta._Table)

            assert num_rows == read_table.num_rows
            assert num_columns == read_table.num_columns
            assert expected_shape == read_table.shape

            write_table = kta.batch_write_table()
            assert isinstance(write_table, kata.ArrowBatchWriteTable)
            assert isinstance(write_table, kta.WriteTable)
            assert isinstance(write_table, kta._Table)

            # import debugpy

            # debugpy.listen(5678)
            # print("Waiting for debugger attach")
            # debugpy.wait_for_client()
            # debugpy.breakpoint()

            for read_batch in read_table.batches():
                assert num_rows >= read_batch.num_rows
                assert num_columns == read_batch.num_columns
                assert isinstance(read_batch, kata.ArrowBatch)
                assert isinstance(read_batch, kta.Batch)

                if mode == "arrow":
                    arrow_batch = read_batch.to_pyarrow()
                    arrays = []
                    arrays.append(pa.array(arrow_batch.column(0).to_numpy() * 2))
                    arrays.append(pa.array(arrow_batch.column(1).to_numpy() * 2))
                    arrays.append(arrow_batch.column(2))
                    new_batch = pa.RecordBatch.from_arrays(
                        arrays, schema=arrow_batch.schema
                    )
                    write_batch = kat.ArrowBatch(new_batch)
                elif mode == "arrow-sentinel":
                    arrow_batch = read_batch.to_pyarrow(sentinel=0)
                    arrays = []
                    arrays.append(pa.array(arrow_batch.column(0).to_numpy() * 2))
                    arrays.append(pa.array(arrow_batch.column(1).to_numpy() * 2))
                    arrays.append(arrow_batch.column(2))
                    new_batch = pa.RecordBatch.from_arrays(
                        arrays, schema=arrow_batch.schema
                    )
                    write_batch = kat.ArrowBatch(new_batch, sentinel=0)
                elif mode == "pandas":
                    pandas_df = read_batch.to_pandas()
                    pandas_df["0"] = pandas_df["0"] * 2
                    pandas_df["1"] = pandas_df["1"] * 2
                    write_batch = kat.ArrowBatch(pandas_df)
                elif mode == "dict":
                    d = read_batch.to_pyarrow().to_pydict()
                    d["0"] = [i * 2 for i in d["0"]]
                    d["1"] = [i * 2 for i in d["1"]]
                    write_batch = kat.ArrowBatch(
                        # from_pydict requires pyarrow 6.0! Install via PIP
                        pa.RecordBatch.from_pydict(d)
                    )
                else:
                    write_batch = read_batch

                assert isinstance(write_batch, kata.ArrowBatch)
                assert isinstance(write_batch, kta.Batch)
                assert read_batch.num_rows == write_batch.num_rows
                assert read_batch.num_columns == write_batch.num_columns
                write_table.append(write_batch)

            assert num_rows == write_table.num_rows
            assert num_columns == write_table.num_columns
            assert read_table.num_batches == write_table.num_batches
            assert expected_shape == write_table.shape

        kta._backend.close()
        return write_table._sink._java_data_sink

    class Java:
        implements = ["org.knime.python3.arrow.TestUtils.ArrowTestEntryPoint"]


kg.connect_to_knime(EntryPoint())
