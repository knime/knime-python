/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 */

package org.knime.python2.serde.arrow;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullableBigIntVector;
import org.apache.arrow.vector.NullableBitVector;
import org.apache.arrow.vector.NullableFloat4Vector;
import org.apache.arrow.vector.NullableFloat8Vector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.NullableVarBinaryVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.stream.ArrowStreamReader;
import org.apache.arrow.vector.stream.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.FileUtil;
import org.knime.core.util.ThreadUtils;
import org.knime.python2.extensions.serializationlibrary.SerializationException;
import org.knime.python2.extensions.serializationlibrary.SerializationOptions;
import org.knime.python2.extensions.serializationlibrary.interfaces.Row;
import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibrary;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableCreator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableIterator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;
import org.knime.python2.extensions.serializationlibrary.interfaces.VectorExtractor;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.CellImpl;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.RowImpl;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.TableSpecImpl;
import org.knime.python2.kernel.PythonCancelable;
import org.knime.python2.kernel.PythonCanceledExecutionException;
import org.knime.python2.kernel.PythonExecutionException;
import org.knime.python2.serde.arrow.ReadContextManager.ReadContext;
import org.knime.python2.serde.arrow.extractors.BooleanExtractor;
import org.knime.python2.serde.arrow.extractors.BooleanListExtractor;
import org.knime.python2.serde.arrow.extractors.BooleanSetExtractor;
import org.knime.python2.serde.arrow.extractors.BytesExtractor;
import org.knime.python2.serde.arrow.extractors.BytesListExtractor;
import org.knime.python2.serde.arrow.extractors.BytesSetExtractor;
import org.knime.python2.serde.arrow.extractors.DoubleExtractor;
import org.knime.python2.serde.arrow.extractors.DoubleListExtractor;
import org.knime.python2.serde.arrow.extractors.DoubleSetExtractor;
import org.knime.python2.serde.arrow.extractors.FloatExtractor;
import org.knime.python2.serde.arrow.extractors.FloatListExtractor;
import org.knime.python2.serde.arrow.extractors.FloatSetExtractor;
import org.knime.python2.serde.arrow.extractors.IntListExtractor;
import org.knime.python2.serde.arrow.extractors.IntSetExtractor;
import org.knime.python2.serde.arrow.extractors.IntegerExtractor;
import org.knime.python2.serde.arrow.extractors.LongExtractor;
import org.knime.python2.serde.arrow.extractors.LongListExtractor;
import org.knime.python2.serde.arrow.extractors.LongSetExtractor;
import org.knime.python2.serde.arrow.extractors.MissingExtractor;
import org.knime.python2.serde.arrow.extractors.StringExtractor;
import org.knime.python2.serde.arrow.extractors.StringListExtractor;
import org.knime.python2.serde.arrow.extractors.StringSetExtractor;
import org.knime.python2.serde.arrow.inserters.ArrowVectorInserter;
import org.knime.python2.serde.arrow.inserters.BooleanInserter;
import org.knime.python2.serde.arrow.inserters.BooleanListInserter;
import org.knime.python2.serde.arrow.inserters.BooleanSetInserter;
import org.knime.python2.serde.arrow.inserters.BytesInserter;
import org.knime.python2.serde.arrow.inserters.BytesListInserter;
import org.knime.python2.serde.arrow.inserters.BytesSetInserter;
import org.knime.python2.serde.arrow.inserters.DoubleInserter;
import org.knime.python2.serde.arrow.inserters.DoubleListInserter;
import org.knime.python2.serde.arrow.inserters.DoubleSetInserter;
import org.knime.python2.serde.arrow.inserters.FloatInserter;
import org.knime.python2.serde.arrow.inserters.FloatListInserter;
import org.knime.python2.serde.arrow.inserters.FloatSetInserter;
import org.knime.python2.serde.arrow.inserters.IntListInserter;
import org.knime.python2.serde.arrow.inserters.IntSetInserter;
import org.knime.python2.serde.arrow.inserters.IntegerInserter;
import org.knime.python2.serde.arrow.inserters.LongInserter;
import org.knime.python2.serde.arrow.inserters.LongListInserter;
import org.knime.python2.serde.arrow.inserters.LongSetInserter;
import org.knime.python2.serde.arrow.inserters.StringInserter;
import org.knime.python2.serde.arrow.inserters.StringListInserter;
import org.knime.python2.serde.arrow.inserters.StringSetInserter;
import org.knime.python2.util.PythonUtils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Serializes tables to bytes and deserializes bytes to tables using the Apache Arrow Format. The serialized data is
 * written to temporary files, the file paths are shared via the command socket.
 *
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public class ArrowSerializationLibrary implements SerializationLibrary {

    /* Note: should be a power of 2 */
    private static final int ASSUMED_ROWID_VAL_BYTE_SIZE = 4;

    /* Note: should be a power of 2 */
    private static final int ASSUMED_STRING_VAL_BYTE_SIZE = 64;

    /* Note: should be a power of 2 */
    private static final int ASSUMED_BYTES_VAL_BYTE_SIZE = 32;

    private enum PandasType {
            BOOL("bool"), INT("int"), UNICODE("unicode"), BYTES("bytes");

        private final String m_id;

        PandasType(final String id) {
            this.m_id = id;
        }

        String getId() {
            return m_id;
        }
    }

    private enum NumpyType {
            OBJECT("object"), INT32("int32"), INT64("int64"), FLOAT64("float64"), FLOAT32("float32");

        private final String m_id;

        NumpyType(final String id) {
            this.m_id = id;
        }

        String getId() {
            return m_id;
        }
    }

    /**
     * Builds a metadata tag in the following format: {"name": <name>, "pandas_type": <pandasType>, "numpy_type":
     * <numpyType>, "metadata": {"type_id": <knimeType>, "serializer_id": <serializer>}}
     */
    private static JsonObjectBuilder createColumnMetadataBuilder(final String name, final PandasType pandasType,
        final NumpyType numpyType, final Type knimeType, final String serializer) {
        final JsonObjectBuilder colMetadataBuilder = Json.createObjectBuilder();
        colMetadataBuilder.add("name", name);
        colMetadataBuilder.add("pandas_type", pandasType.getId());
        colMetadataBuilder.add("numpy_type", numpyType.getId());
        final JsonObjectBuilder knimeMetadataBuilder = Json.createObjectBuilder();
        knimeMetadataBuilder.add("type_id", knimeType.getId());
        knimeMetadataBuilder.add("serializer_id", serializer);
        colMetadataBuilder.add("metadata", knimeMetadataBuilder);
        return colMetadataBuilder;
    }

    private static JsonObjectBuilder createColumnMetadataBuilder(final String name, final PandasType pandasType,
        final NumpyType numpyType, final Type knimeType) {
        return createColumnMetadataBuilder(name, pandasType, numpyType, knimeType, "");
    }

    /** Used to make (de-)serialization cancelable. */
    private final ExecutorService m_executorService = ThreadUtils.executorServiceWithContext(
        Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("python-arrow-serde-%d").build()));

    /**
     * The root directory in which the temporary files used for data transfer are stored. Will be populated during the
     * first call of {@link #tableToBytes(TableIterator, SerializationOptions, PythonCancelable)}.
     */
    private File m_tempDir;

    // Note: we will never get a multi-index due to index standardization in FromPandasTable.
    private String m_indexColumnName = null;

    private String[] m_missingColumnNames = null;

    @Override
    public byte[] tableToBytes(final TableIterator tableIterator, final SerializationOptions serializationOptions,
        final PythonCancelable cancelable) throws SerializationException, PythonCanceledExecutionException {
        File file = null;
        try {
            // Temporary files are used for data transfer.
            if (m_tempDir == null || !m_tempDir.exists()) {
                // Deleted upon JVM shutdown (or #close()).
                m_tempDir = FileUtil.createTempDir("knime-python-");
            }
            file = FileUtil.createTempFile("java-to-python-", ".dat", m_tempDir, false);
            final File finalFile = file;
            return PythonUtils.Misc.executeCancelable(() -> {
                try (RandomAccessFile raf = new RandomAccessFile(finalFile, "rw");
                        FileChannel channel = raf.getChannel()) {
                    return tableToBytesInternal(tableIterator, serializationOptions, channel,
                        finalFile.getAbsolutePath());
                }
            }, m_executorService, cancelable);
        } catch (IOException | PythonExecutionException e) {
            PythonUtils.Misc.invokeSafely(null, File::delete, file);
            throw new SerializationException("An error occurred during serialization. See log for errors.", e);
        } catch (final OversizedAllocationException ex) {
            PythonUtils.Misc.invokeSafely(null, File::delete, file);
            throw new SerializationException(
                "The requested buffer size during serialization exceeds the maximum buffer size."
                    + " Please consider decreasing the 'Rows per chunk' parameter in the 'Options' tab of the"
                    + " configuration dialog.");
        } catch (final Exception ex) {
            PythonUtils.Misc.invokeSafely(null, File::delete, file);
            throw ex;
        }
    }

    /**
     * Possibly interrupted by {@link #tableToBytes(TableIterator, SerializationOptions, PythonCancelable)}.
     */
    private static byte[] tableToBytesInternal(final TableIterator tableIterator,
        final SerializationOptions serializationOptions, final FileChannel fc, final String path) throws IOException {
        try (RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE)) {
            List<ArrowVectorInserter> inserters = null;
            try {
                final String indexColName = "__index_level_0__";
                // Metadata is transferred in JSON format.
                final JsonObjectBuilder metadataBuilder = Json.createObjectBuilder();
                final TableSpec spec = tableIterator.getTableSpec();
                inserters = new ArrayList<>();
                final JsonArrayBuilder icBuilder = Json.createArrayBuilder();

                icBuilder.add(indexColName);
                metadataBuilder.add("index_columns", icBuilder);
                final JsonArrayBuilder colBuilder = Json.createArrayBuilder();
                final int numRows = tableIterator.getNumberRemainingRows();
                // Row ids
                final JsonObjectBuilder rowIdBuilder =
                    createColumnMetadataBuilder(indexColName, PandasType.UNICODE, NumpyType.OBJECT, Type.STRING);
                inserters.add(new StringInserter(indexColName, rootAllocator, numRows, ASSUMED_ROWID_VAL_BYTE_SIZE));
                colBuilder.add(rowIdBuilder);

                // Create inserters and metadata.
                for (int i = 0; i < spec.getNumberColumns(); i++) {
                    JsonObjectBuilder colMetadataBuilder;
                    switch (spec.getColumnTypes()[i]) {
                        case BOOLEAN:
                            colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], PandasType.BOOL,
                                NumpyType.OBJECT, Type.BOOLEAN);
                            inserters.add(new BooleanInserter(spec.getColumnNames()[i], rootAllocator, numRows));
                            break;
                        case INTEGER:
                            colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], PandasType.INT,
                                NumpyType.INT32, Type.INTEGER);
                            inserters.add(new IntegerInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                                serializationOptions));
                            break;
                        case LONG:
                            colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], PandasType.INT,
                                NumpyType.INT64, Type.LONG);
                            inserters.add(new LongInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                                serializationOptions));
                            break;
                        case DOUBLE:
                            colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], PandasType.INT,
                                NumpyType.FLOAT64, Type.DOUBLE);
                            inserters.add(new DoubleInserter(spec.getColumnNames()[i], rootAllocator, numRows));
                            break;
                        case FLOAT:
                            colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], PandasType.INT,
                                NumpyType.FLOAT32, Type.FLOAT);
                            inserters.add(new FloatInserter(spec.getColumnNames()[i], rootAllocator, numRows));
                            break;
                        case STRING:
                            colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i],
                                PandasType.UNICODE, NumpyType.OBJECT, Type.STRING);
                            inserters.add(new StringInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                                ASSUMED_STRING_VAL_BYTE_SIZE));
                            break;
                        case BYTES:
                            colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], PandasType.BYTES,
                                NumpyType.OBJECT, Type.BYTES,
                                spec.getColumnSerializers().get(spec.getColumnNames()[i]));
                            inserters.add(new BytesInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                                ASSUMED_BYTES_VAL_BYTE_SIZE));
                            break;
                        case INTEGER_LIST:
                            colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], PandasType.BYTES,
                                NumpyType.OBJECT, Type.INTEGER_LIST);
                            inserters.add(new IntListInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                                ASSUMED_BYTES_VAL_BYTE_SIZE));
                            break;
                        case INTEGER_SET:
                            colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], PandasType.BYTES,
                                NumpyType.OBJECT, Type.INTEGER_SET);
                            inserters.add(new IntSetInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                                ASSUMED_BYTES_VAL_BYTE_SIZE));
                            break;
                        case LONG_LIST:
                            colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], PandasType.BYTES,
                                NumpyType.OBJECT, Type.LONG_LIST);
                            inserters.add(new LongListInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                                ASSUMED_BYTES_VAL_BYTE_SIZE));
                            break;
                        case LONG_SET:
                            colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], PandasType.BYTES,
                                NumpyType.OBJECT, Type.LONG_SET);
                            inserters.add(new LongSetInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                                ASSUMED_BYTES_VAL_BYTE_SIZE));
                            break;
                        case DOUBLE_LIST:
                            colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], PandasType.BYTES,
                                NumpyType.OBJECT, Type.DOUBLE_LIST);
                            inserters.add(new DoubleListInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                                ASSUMED_BYTES_VAL_BYTE_SIZE));
                            break;
                        case DOUBLE_SET:
                            colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], PandasType.BYTES,
                                NumpyType.OBJECT, Type.DOUBLE_SET);
                            inserters.add(new DoubleSetInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                                ASSUMED_BYTES_VAL_BYTE_SIZE));
                            break;
                        case FLOAT_LIST:
                            colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], PandasType.BYTES,
                                NumpyType.OBJECT, Type.FLOAT_LIST);
                            inserters.add(new FloatListInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                                ASSUMED_BYTES_VAL_BYTE_SIZE));
                            break;
                        case FLOAT_SET:
                            colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], PandasType.BYTES,
                                NumpyType.OBJECT, Type.FLOAT_SET);
                            inserters.add(new FloatSetInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                                ASSUMED_BYTES_VAL_BYTE_SIZE));
                            break;
                        case BOOLEAN_LIST:
                            colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], PandasType.BYTES,
                                NumpyType.OBJECT, Type.BOOLEAN_LIST);
                            inserters.add(new BooleanListInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                                ASSUMED_BYTES_VAL_BYTE_SIZE));
                            break;
                        case BOOLEAN_SET:
                            colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], PandasType.BYTES,
                                NumpyType.OBJECT, Type.BOOLEAN_SET);
                            inserters.add(new BooleanSetInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                                ASSUMED_BYTES_VAL_BYTE_SIZE));
                            break;
                        case STRING_LIST:
                            colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], PandasType.BYTES,
                                NumpyType.OBJECT, Type.STRING_LIST);
                            inserters.add(new StringListInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                                ASSUMED_BYTES_VAL_BYTE_SIZE));
                            break;
                        case STRING_SET:
                            colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], PandasType.BYTES,
                                NumpyType.OBJECT, Type.STRING_SET);
                            inserters.add(new StringSetInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                                ASSUMED_BYTES_VAL_BYTE_SIZE));
                            break;
                        case BYTES_LIST:
                            colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], PandasType.BYTES,
                                NumpyType.OBJECT, Type.BYTES_LIST,
                                spec.getColumnSerializers().get(spec.getColumnNames()[i]));
                            inserters.add(new BytesListInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                                ASSUMED_BYTES_VAL_BYTE_SIZE));
                            break;
                        case BYTES_SET:
                            colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], PandasType.BYTES,
                                NumpyType.OBJECT, Type.BYTES_SET,
                                spec.getColumnSerializers().get(spec.getColumnNames()[i]));
                            inserters.add(new BytesSetInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                                ASSUMED_BYTES_VAL_BYTE_SIZE));
                            break;
                        default:
                            throw new IllegalStateException(
                                "Serialization is not implemented for type: " + spec.getColumnTypes()[i].name());
                    }
                    colBuilder.add(colMetadataBuilder);
                }
                metadataBuilder.add("columns", colBuilder);

                // Iterate over table and put every cell in an arrow buffer using the inserters.
                while (tableIterator.hasNext()) {
                    if (Thread.interrupted()) {
                        // Stop serialization if canceled by client.
                        throw new CancellationException("Serialization canceled by client.");
                    }
                    final Row row = tableIterator.next();
                    inserters.get(0).put(new CellImpl(row.getRowKey()));
                    for (int i = 0; i < spec.getNumberColumns(); i++) {
                        inserters.get(i + 1).put(row.getCell(i));
                    }
                }

                // Build final representation and transmit.
                final Map<String, String> metadata = new HashMap<>();
                metadata.put("pandas", metadataBuilder.build().toString());

                final List<FieldVector> vecs = new ArrayList<>();
                final List<Field> fields = new ArrayList<>();
                for (int i = 0; i < inserters.size(); i++) {
                    final FieldVector vec = inserters.get(i).retrieveVector(); // Closed via inserters.
                    vecs.add(vec);
                    fields.add(vec.getField());
                }
                final Schema schema = new Schema(fields, metadata);
                try (ArrowStreamWriter writer =
                    new ArrowStreamWriter(new VectorSchemaRoot(schema, vecs, numRows), null, fc)) {
                    writer.writeBatch();
                }
            } finally {
                // Close inserters to free memory.
                PythonUtils.Misc.invokeSafely(null, ArrowVectorInserter::close, inserters);
            }
        }
        return path.getBytes("UTF-8");
    }

    private static VectorExtractor getStringOrByteExtractor(final FieldVector vec) {
        if (vec instanceof NullableVarCharVector) {
            return new StringExtractor((NullableVarCharVector)vec);
        } else {
            return new BytesExtractor((NullableVarBinaryVector)vec, true);
        }
    }

    @Override
    public void bytesIntoTable(final TableCreator<?> tableCreator, final byte[] bytes,
        final SerializationOptions serializationOptions, final PythonCancelable cancelable)
        throws SerializationException, PythonCanceledExecutionException {
        File file = null;
        try {
            file = new File(new String(bytes, StandardCharsets.UTF_8));
            final File finalFile = file;
            final TableSpec spec = tableSpecFromBytes(bytes, cancelable);
            PythonUtils.Misc.executeCancelable(() -> {
                bytesIntoTableInternal(tableCreator, serializationOptions, spec, finalFile);
                return null;
            }, m_executorService, cancelable);
        } catch (final PythonExecutionException e) {
            throw new SerializationException("An error occurred during deserialization. See log for details.", e);
        } finally {
            PythonUtils.Misc.invokeSafely(null, f -> {
                if (!ReadContextManager.destroy(f)) {
                    NodeLogger.getLogger(ArrowSerializationLibrary.class).warn("Could not destroy content object.");
                }
            }, file);
            PythonUtils.Misc.invokeSafely(null, File::delete, file);
        }
    }

    /**
     * Possibly interrupted by {@link #bytesIntoTable(TableCreator, byte[], SerializationOptions, PythonCancelable)}.
     */
    private void bytesIntoTableInternal(final TableCreator<?> tableCreator,
        final SerializationOptions serializationOptions, final TableSpec spec, final File file) throws IOException {
        final ReadContext rc = ReadContextManager.createForFile(file);
        if (spec.getNumberColumns() > 0 && rc.getNumRows() > 0) {
            try (ArrowStreamReader reader = ReadContextManager.createForFile(file).getReader()) {
                final VectorSchemaRoot root = reader.getVectorSchemaRoot(); // Will be closed by reader.
                final Type[] types = spec.getColumnTypes();
                final String[] names = spec.getColumnNames();

                final List<VectorExtractor> extractors = new ArrayList<>();
                // Index is always string.
                extractors.add(getStringOrByteExtractor(root.getVector(m_indexColumnName)));

                // Setup an extractor for every column.
                for (int j = 0; j < spec.getNumberColumns(); j++) {
                    if (ArrayUtils.contains(m_missingColumnNames, names[j])) {
                        extractors.add(new MissingExtractor());
                    } else {
                        switch (types[j]) {
                            case BOOLEAN:
                                extractors.add(new BooleanExtractor((NullableBitVector)root.getVector(names[j])));
                                break;
                            case INTEGER:
                                extractors.add(new IntegerExtractor((NullableIntVector)root.getVector(names[j]),
                                    serializationOptions));
                                break;
                            case LONG:
                                extractors.add(new LongExtractor((NullableBigIntVector)root.getVector(names[j]),
                                    serializationOptions));
                                break;
                            case DOUBLE:
                                extractors.add(new DoubleExtractor((NullableFloat8Vector)root.getVector(names[j])));
                                break;
                            case FLOAT:
                                extractors.add(new FloatExtractor((NullableFloat4Vector)root.getVector(names[j])));
                                break;
                            case STRING:
                                extractors.add(getStringOrByteExtractor(root.getVector(names[j])));
                                break;
                            case BYTES:
                                extractors.add(new BytesExtractor((NullableVarBinaryVector)root.getVector(names[j])));
                                break;
                            case INTEGER_LIST:
                                extractors.add(new IntListExtractor((NullableVarBinaryVector)root.getVector(names[j])));
                                break;
                            case INTEGER_SET:
                                extractors.add(new IntSetExtractor((NullableVarBinaryVector)root.getVector(names[j])));
                                break;
                            case LONG_LIST:
                                extractors
                                    .add(new LongListExtractor((NullableVarBinaryVector)root.getVector(names[j])));
                                break;
                            case LONG_SET:
                                extractors.add(new LongSetExtractor((NullableVarBinaryVector)root.getVector(names[j])));
                                break;
                            case DOUBLE_LIST:
                                extractors
                                    .add(new DoubleListExtractor((NullableVarBinaryVector)root.getVector(names[j])));
                                break;
                            case DOUBLE_SET:
                                extractors
                                    .add(new DoubleSetExtractor((NullableVarBinaryVector)root.getVector(names[j])));
                                break;
                            case FLOAT_LIST:
                                extractors
                                    .add(new FloatListExtractor((NullableVarBinaryVector)root.getVector(names[j])));
                                break;
                            case FLOAT_SET:
                                extractors
                                    .add(new FloatSetExtractor((NullableVarBinaryVector)root.getVector(names[j])));
                                break;
                            case BOOLEAN_LIST:
                                extractors
                                    .add(new BooleanListExtractor((NullableVarBinaryVector)root.getVector(names[j])));
                                break;
                            case BOOLEAN_SET:
                                extractors
                                    .add(new BooleanSetExtractor((NullableVarBinaryVector)root.getVector(names[j])));
                                break;
                            case STRING_LIST:
                                extractors
                                    .add(new StringListExtractor((NullableVarBinaryVector)root.getVector(names[j])));
                                break;
                            case STRING_SET:
                                extractors
                                    .add(new StringSetExtractor((NullableVarBinaryVector)root.getVector(names[j])));
                                break;
                            case BYTES_LIST:
                                extractors
                                    .add(new BytesListExtractor((NullableVarBinaryVector)root.getVector(names[j])));
                                break;
                            case BYTES_SET:
                                extractors
                                    .add(new BytesSetExtractor((NullableVarBinaryVector)root.getVector(names[j])));
                                break;
                            default:
                                throw new IllegalStateException(
                                    "Deserialization is not implemented for type: " + types[j]);
                        }
                    }
                }
                // Extract each value as a Cell, collate the cells to Rows and add the rows to the table creator for
                // further processing
                for (int i = 0; i < root.getRowCount(); i++) {
                    if (Thread.interrupted()) {
                        // Stop deserialization if canceled by client.
                        throw new CancellationException("Deserialization canceled by client.");
                    }
                    final Row row = new RowImpl(extractors.get(0).extract().getStringValue(), spec.getNumberColumns());
                    for (int j = 0; j < spec.getNumberColumns(); j++) {
                        row.setCell(extractors.get(j + 1).extract(), j);
                    }
                    tableCreator.addRow(row);
                }
            }
        }
    }

    @Override
    public TableSpec tableSpecFromBytes(final byte[] bytes, final PythonCancelable cancelable)
        // Note: We don't implement cancellation here, because reading the spec should be cancelable in a timely manner
        // anyway.
        throws SerializationException {
        final String path = new String(bytes, StandardCharsets.UTF_8);
        final File file = new File(path);
        try {
            final ReadContext rc = ReadContextManager.createForFile(file);
            if (rc.getTableSpec() == null) {
                if (file.exists()) {
                    ArrowStreamReader reader = null;
                    try {
                        reader = rc.getReader();
                        reader.loadNextBatch();
                        final Schema schema = reader.getVectorSchemaRoot().getSchema();
                        final Map<String, String> metadata = schema.getCustomMetadata();
                        final Map<String, String> columnSerializers = new HashMap<>();
                        // Build the table spec out of the metadata available in JSON format
                        // Format: {"ArrowSerializationLibrary": {"index_columns": String[1], "columns": Column[?],
                        //          "missing_columns": String[?], "num_rows": int}}
                        // Column format: {"name": String, "metadata": {"serializer_id": String, "type_id": int}}
                        final String customMetadata = metadata.get("ArrowSerializationLibrary");
                        if (customMetadata != null) {
                            try (JsonReader jsreader = Json.createReader(new StringReader(customMetadata))) {
                                final JsonObject jpandasMetadata = jsreader.readObject();
                                final JsonArray indexCols = jpandasMetadata.getJsonArray("index_columns");
                                final JsonArray cols = jpandasMetadata.getJsonArray("columns");
                                final JsonArray missingCols = jpandasMetadata.getJsonArray("missing_columns");
                                rc.setNumRows(jpandasMetadata.getInt("num_rows"));
                                final String[] names = new String[cols.size() - indexCols.size()];
                                final Type[] types = new Type[cols.size() - indexCols.size()];
                                int noIdxCtr = 0;
                                for (int i = 0; i < cols.size(); i++) {
                                    final JsonObject col = cols.getJsonObject(i);
                                    boolean contained = false;
                                    for (int j = 0; j < indexCols.size(); j++) {
                                        if (indexCols.getString(j).contentEquals(col.getString("name"))) {
                                            contained = true;
                                            break;
                                        }
                                    }
                                    if (contained) {
                                        m_indexColumnName = col.getString("name");
                                        continue;
                                    }
                                    names[noIdxCtr] = col.getString("name");
                                    final JsonObject typeObj = col.getJsonObject("metadata");
                                    types[noIdxCtr] = Type.getTypeForId(typeObj.getInt("type_id"));
                                    if (types[noIdxCtr] == Type.BYTES || types[noIdxCtr] == Type.BYTES_LIST
                                        || types[noIdxCtr] == Type.BYTES_SET) {
                                        final String serializerId =
                                            col.getJsonObject("metadata").getString("serializer_id");
                                        if (!StringUtils.isBlank(serializerId)) {
                                            columnSerializers.put(names[noIdxCtr], serializerId);
                                        }
                                    }
                                    noIdxCtr++;
                                }
                                m_missingColumnNames = new String[missingCols.size()];
                                for (int i = 0; i < missingCols.size(); i++) {
                                    m_missingColumnNames[i] = missingCols.getString(i);
                                }
                                rc.setTableSpec(new TableSpecImpl(types, names, columnSerializers));
                            }
                        }
                    } catch (final Exception ex) {
                        // Only close reader on exception. Otherwise it will be closed in #bytesIntoTableInternal(..).
                        PythonUtils.Misc.closeSafely(null, reader);
                        throw ex;
                    }
                } else {
                    rc.setTableSpec(new TableSpecImpl(new Type[0], new String[0], null));
                }
            }
            if (rc.getTableSpec() == null) {
                throw new IllegalStateException("Could not build TableSpec!");
            }
            return rc.getTableSpec();
        } catch (final IOException ex) {
            PythonUtils.Misc.invokeSafely(null, File::delete, file);
            throw new SerializationException("An error occurred during deserialization.", ex);
        } catch (final Exception ex) {
            PythonUtils.Misc.invokeSafely(null, File::delete, file);
            throw ex;
        }
    }

    @Override
    public void close() {
        PythonUtils.Misc.invokeSafely(null, ExecutorService::shutdownNow, m_executorService);
        if (m_tempDir != null) {
            PythonUtils.Misc.invokeSafely(null, FileUtil::deleteRecursively, m_tempDir);
        }
    }
}
