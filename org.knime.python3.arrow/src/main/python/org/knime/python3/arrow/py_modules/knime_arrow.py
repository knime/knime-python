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
import pyarrow as pa
import knime_gateway as kg

ARROW_CHUNK_SIZE_KEY = "KNIME:basic:chunkSize"
ARROW_FACTORY_VERSIONS_KEY = "KNIME:basic:factoryVersions"


def gateway():
    if kg.client_server is None:
        raise RuntimeError(
            "A connection to KNIME must be established before using a knime_arrow source or sink."
        )
    return kg.client_server


def schema_with_knime_metadata(schema: pa.Schema, chunk_size: int) -> pa.Schema:
    factory_versions = ",".join([factory_version_for(t) for t in schema.types])
    # TODO bytes instead of strings?
    metadata = {
        ARROW_CHUNK_SIZE_KEY: str(chunk_size),
        ARROW_FACTORY_VERSIONS_KEY: factory_versions,
    }
    return schema.with_metadata(metadata)


def factory_version_for(arrow_type: pa.DataType):
    # TODO synchronize with the versions on the Java side
    if isinstance(arrow_type, pa.StructType):
        return "0[{}]".format(
            "".join([factory_version_for(f.type) + ";" for f in arrow_type])
        )
    if isinstance(arrow_type, pa.ListType):
        return "0[{};]".format(factory_version_for(arrow_type.value_type))
    return "0"


def convert_schema(schema: pa.Schema):
    # TODO we would like to use a schema with the virtual types not the physical types
    schema_builder = gateway().jvm.org.knime.python3.arrow.PythonColumnarSchemaBuilder()
    for t in schema.types:
        schema_builder.addColumn(convert_type(t))
    return schema_builder.build()


def convert_type(arrow_type: pa.DataType):
    # Struct
    if isinstance(arrow_type, pa.StructType):
        dataspec_class = gateway().jvm.org.knime.core.table.schema.DataSpec
        children_spec = gateway().new_array(dataspec_class, arrow_type.num_fields)
        for i, f in enumerate(arrow_type):
            children_spec[i] = convert_type(f.type)
        return gateway().jvm.org.knime.core.table.schema.StructDataSpec(children_spec)

    # List
    if isinstance(arrow_type, pa.ListType):
        child_spec = convert_type(arrow_type.value_type)
        return gateway().jvm.org.knime.core.table.schema.ListDataSpec(child_spec)

    # Others
    if arrow_type == pa.bool_():
        return gateway().jvm.org.knime.core.table.schema.DataSpec.booleanSpec()
    if arrow_type == pa.int8():
        return gateway().jvm.org.knime.core.table.schema.DataSpec.byteSpec()
    if arrow_type == pa.float64():
        return gateway().jvm.org.knime.core.table.schema.DataSpec.doubleSpec()
    if arrow_type == pa.float32():
        return gateway().jvm.org.knime.core.table.schema.DataSpec.floatSpec()
    if arrow_type == pa.int32():
        return gateway().jvm.org.knime.core.table.schema.DataSpec.intSpec()
    if arrow_type == pa.int64():
        return gateway().jvm.org.knime.core.table.schema.DataSpec.longSpec()
    if arrow_type == pa.large_binary():
        return gateway().jvm.org.knime.core.table.schema.DataSpec.varBinarySpec()
    if arrow_type == pa.null():
        return gateway().jvm.org.knime.core.table.schema.DataSpec.voidSpec()
    if arrow_type == pa.string():
        return gateway().jvm.org.knime.core.table.schema.DataSpec.stringSpec()
    if arrow_type == pa.time64("ns"):
        return gateway().jvm.org.knime.core.table.schema.DataSpec.localTimeSpec()

    raise ValueError("Unsupported Arrow type: '{}'.".format(arrow_type))


class _OffsetBasedRecordBatchFileReader:
    def __init__(self, source_file: pa.MemoryMappedFile, java_data_source) -> None:
        self._source_file = source_file
        self._java_data_source = java_data_source
        # TODO check the ARROW1 magic number???

        # Read the schema
        self._source_file.seek(8)  # Skip the ARROW1 magic number + padding
        self.schema = pa.ipc.read_schema(self._source_file)

    @property
    def num_record_batches(self):
        return self._java_data_source.numBatches()

    def get_batch(self, index: int) -> pa.RecordBatch:
        # TODO(dictionary) handle dictionaries
        offset = self._java_data_source.getRecordBatchOffset(index)
        self._source_file.seek(offset)
        # TODO do we need to map columns somehow (in Java we have the factory versions)
        return pa.ipc.read_record_batch(self._source_file, self.schema)


@kg.data_source("org.knime.python3.arrow")
class ArrowDataSource:
    """A view on KNIME table data in an Arrow file.

    Note that __getitem__ memory-maps the data from disk each time and does not cache the data.
    """

    # TODO(benjamin) Do I want to enforce the usage of a context manager?
    # TODO(benjamin) Context manager for lists of data sources

    def __init__(self, java_data_source) -> None:
        self._file: pa.MemoryMappedFile = pa.memory_map(
            java_data_source.getAbsolutePath()
        )

        if java_data_source.isFooterWritten():
            self._reader = pa.ipc.open_file(self._file)
        else:
            self._reader = _OffsetBasedRecordBatchFileReader(
                self._file, java_data_source
            )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    @property
    def schema(self) -> pa.Schema:
        return self._reader.schema

    def __len__(self) -> int:
        return self._reader.num_record_batches

    def __getitem__(self, index: int) -> pa.RecordBatch:
        # The type of index must be int
        if not isinstance(index, int):
            raise TypeError(
                "index must be an integer, not {}".format(type(index).__name__)
            )

        # Wrap negative indices and check if the index is valid
        length = len(self)
        if index < 0:
            index = index + length
        if index < 0 or index >= length:
            raise IndexError("index out of range")

        # TODO do we need to map columns somehow (in Java we have the factory versions)
        return self._reader.get_batch(index)

    def close(self):
        self._file.close()

    # API to get higher level access

    def to_pandas(self):
        # TODO
        raise NotImplementedError()


@kg.data_sink("org.knime.python3.arrow")
class ArrowDataSink:
    """A class writing record batches to a file to be read by KNIME."""

    def __init__(self, java_data_sink) -> None:
        self._java_data_sink = java_data_sink

        # Open the file
        self._file = pa.OSFile(java_data_sink.getAbsolutePath(), mode="wb")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def write(self, b: pa.RecordBatch):
        if not hasattr(self, "_writer"):
            # Init the writer if this is the first batch
            # Also use the offset retuned by the init method because the file position
            # is not updated yet
            offset = self._init_writer(schema_with_knime_metadata(b.schema, len(b)))
        else:
            # Remember the current file location
            offset = self._file.tell()

        self._writer.write(b)
        self._file.flush()
        self._java_data_sink.reportBatchWritten(offset)

    def _init_writer(self, schema: pa.Schema):
        # Create the writer
        self._writer = pa.ipc.new_file(self._file, schema=schema)

        # Set the ColumnarSchema for java
        self._java_data_sink.setColumnarSchema(convert_schema(schema))

        # We need to know the size of the serialized schema
        # to know the offset of the first batch
        # NOTE: This is not expensive because we don't serialize data
        schema_buf = schema.serialize()
        # +8 for ARROW1 magic + padding
        return len(schema_buf) + 8

    def close(self):
        self._writer.close()
