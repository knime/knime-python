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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
import org.knime.core.util.FileUtil;
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
import org.knime.python2.serde.arrow.extractors.BooleanExtractor;
import org.knime.python2.serde.arrow.extractors.BooleanListExtractor;
import org.knime.python2.serde.arrow.extractors.BooleanSetExtractor;
import org.knime.python2.serde.arrow.extractors.BytesExtractor;
import org.knime.python2.serde.arrow.extractors.BytesListExtractor;
import org.knime.python2.serde.arrow.extractors.BytesSetExtractor;
import org.knime.python2.serde.arrow.extractors.DoubleExtractor;
import org.knime.python2.serde.arrow.extractors.DoubleListExtractor;
import org.knime.python2.serde.arrow.extractors.DoubleSetExtractor;
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
import org.knime.python2.serde.arrow.inserters.IntListInserter;
import org.knime.python2.serde.arrow.inserters.IntSetInserter;
import org.knime.python2.serde.arrow.inserters.IntegerInserter;
import org.knime.python2.serde.arrow.inserters.LongInserter;
import org.knime.python2.serde.arrow.inserters.LongListInserter;
import org.knime.python2.serde.arrow.inserters.LongSetInserter;
import org.knime.python2.serde.arrow.inserters.StringInserter;
import org.knime.python2.serde.arrow.inserters.StringListInserter;
import org.knime.python2.serde.arrow.inserters.StringSetInserter;

/**
 * @author Clemens von Schwerin, KNIME
 */
public class ArrowSerializationLibrary implements SerializationLibrary {

    /*Note: should be a power of 2*/
    private final static int ASSUMED_ROWID_VAL_BYTE_SIZE = 4;

    /*Note: should be a power of 2*/
    private final static int ASSUMED_STRING_VAL_BYTE_SIZE = 64;

    /*Note: should be a power of 2*/
    private final static int ASSUMED_BYTES_VAL_BYTE_SIZE = 32;

    //    private final static long FIXED_BATCH_BYTE_SIZE = 5 * 1024 * 1024;

    private ArrowStreamReader m_streamReader = null;

    private TableSpec m_tableSpec = null;

    private String m_fromPythonPath = null;

    //NOTE: we will never get a multi-index due to index standardization in FromPandasTable
    private String m_indexColumnName = null;

    private String[] m_missingColumnNames = null;

    private JsonObjectBuilder createColumnMetadataBuilder(final String name, final String pandasType,
        final String numpyType, final Type knimeType, final String serializer) {
        JsonObjectBuilder colMetadataBuilder = Json.createObjectBuilder();
        colMetadataBuilder.add("name", name);
        colMetadataBuilder.add("pandas_type", pandasType);
        colMetadataBuilder.add("numpy_type", numpyType);
        JsonObjectBuilder knimeMetadataBuilder = Json.createObjectBuilder();
        knimeMetadataBuilder.add("type_id", knimeType.getId());
        knimeMetadataBuilder.add("serializer_id", serializer);
        colMetadataBuilder.add("metadata", knimeMetadataBuilder);
        return colMetadataBuilder;
    }

    private JsonObjectBuilder createColumnMetadataBuilder(final String name, final String pandasType,
        final String numpyType, final Type knimeType) {
        return createColumnMetadataBuilder(name, pandasType, numpyType, knimeType, "");
    }

    @Override
    public byte[] tableToBytes(final TableIterator tableIterator, final SerializationOptions serializationOptions)
            throws SerializationException{
        File file = null;
        try {
            file = FileUtil.createTempFile("arrow-memory-mapped-" + UUID.randomUUID().toString(), ".dat", true);
            // TODO try to pass RandomAccessFile rather than fc + path
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw"); FileChannel channel = raf.getChannel()) {
                return tableToBytesDynamic(tableIterator, serializationOptions, channel, file.getAbsolutePath());
            }
        } catch (IOException e) {
            // TODO Logging and better exception handling?
            throw new IllegalStateException(e);
        } catch (OversizedAllocationException ex) {
            throw new SerializationException("The requested buffersize during serialization exceeds the maximum buffer size."
                    + " Please consider decreasing the 'Rows per chunk' parameter in the 'Options' tab of the configuration dialog.");
        }
    }

    private byte[] tableToBytesDynamic(final TableIterator tableIterator,
        final SerializationOptions serializationOptions, final FileChannel fc, final String path) throws IOException {

        final String INDEX_COL_NAME = "__index_level_0__";
        JsonObjectBuilder metadataBuilder = Json.createObjectBuilder();
        TableSpec spec = tableIterator.getTableSpec();
        //List<FieldVector> vecs = new ArrayList<FieldVector>();
        List<ArrowVectorInserter> inserters = new ArrayList<>();
        //List<Field> fields = new ArrayList<Field>();
        JsonArrayBuilder icBuilder = Json.createArrayBuilder();
        RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
        icBuilder.add(INDEX_COL_NAME);
        metadataBuilder.add("index_columns", icBuilder);
        JsonArrayBuilder colBuilder = Json.createArrayBuilder();
        int numRows = tableIterator.getNumberRemainingRows();
        // Row ids
        JsonObjectBuilder rowIdBuilder = createColumnMetadataBuilder(INDEX_COL_NAME, "unicode", "object", Type.STRING);
        inserters.add(new StringInserter(INDEX_COL_NAME, rootAllocator, numRows, ASSUMED_ROWID_VAL_BYTE_SIZE));
        colBuilder.add(rowIdBuilder);

        // Create FieldVectors and metadata
        for (int i = 0; i < spec.getNumberColumns(); i++) {
            JsonObjectBuilder colMetadataBuilder;
            switch (spec.getColumnTypes()[i]) {
                case BOOLEAN:
                    colMetadataBuilder =
                        createColumnMetadataBuilder(spec.getColumnNames()[i], "bool", "object", Type.BOOLEAN);
                    inserters.add(new BooleanInserter(spec.getColumnNames()[i], rootAllocator, numRows));
                    break;
                case INTEGER:
                    colMetadataBuilder =
                        createColumnMetadataBuilder(spec.getColumnNames()[i], "int", "int32", Type.INTEGER);
                    inserters.add(
                        new IntegerInserter(spec.getColumnNames()[i], rootAllocator, numRows, serializationOptions));
                    break;
                case LONG:
                    colMetadataBuilder =
                        createColumnMetadataBuilder(spec.getColumnNames()[i], "int", "int64", Type.LONG);
                    inserters
                        .add(new LongInserter(spec.getColumnNames()[i], rootAllocator, numRows, serializationOptions));
                    break;
                case DOUBLE:
                    colMetadataBuilder =
                        createColumnMetadataBuilder(spec.getColumnNames()[i], "int", "float64", Type.DOUBLE);
                    // Allocate vector for column
                    inserters.add(new DoubleInserter(spec.getColumnNames()[i], rootAllocator, numRows));
                    break;
                case STRING:
                    colMetadataBuilder =
                        createColumnMetadataBuilder(spec.getColumnNames()[i], "unicode", "object", Type.STRING);
                    inserters.add(new StringInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                        ASSUMED_STRING_VAL_BYTE_SIZE));
                    break;
                case BYTES:
                    colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], "bytes", "object",
                        Type.BYTES, spec.getColumnSerializers().get(spec.getColumnNames()[i]));
                    inserters.add(new BytesInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                        ASSUMED_BYTES_VAL_BYTE_SIZE));
                    break;
                case INTEGER_LIST:
                    // TODO "bytes" and "object" etc maybe part of enum?
                    colMetadataBuilder =
                        createColumnMetadataBuilder(spec.getColumnNames()[i], "bytes", "object", Type.INTEGER_LIST);

                    //TODO maybe guess assumed size based on first row?
                    inserters.add(new IntListInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                        ASSUMED_BYTES_VAL_BYTE_SIZE));
                    break;
                case INTEGER_SET:
                    colMetadataBuilder =
                        createColumnMetadataBuilder(spec.getColumnNames()[i], "bytes", "object", Type.INTEGER_SET);
                    inserters.add(new IntSetInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                        ASSUMED_BYTES_VAL_BYTE_SIZE));
                    break;
                case LONG_LIST:
                    // TODO "bytes" and "object" etc maybe part of enum?
                    colMetadataBuilder =
                        createColumnMetadataBuilder(spec.getColumnNames()[i], "bytes", "object", Type.LONG_LIST);

                    //TODO maybe guess assumed size based on first row?
                    inserters.add(new LongListInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                        ASSUMED_BYTES_VAL_BYTE_SIZE));
                    break;
                case LONG_SET:
                    colMetadataBuilder =
                        createColumnMetadataBuilder(spec.getColumnNames()[i], "bytes", "object", Type.LONG_SET);
                    inserters.add(new LongSetInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                        ASSUMED_BYTES_VAL_BYTE_SIZE));
                    break;
                case DOUBLE_LIST:
                    // TODO "bytes" and "object" etc maybe part of enum?
                    colMetadataBuilder =
                        createColumnMetadataBuilder(spec.getColumnNames()[i], "bytes", "object", Type.DOUBLE_LIST);

                    //TODO maybe guess assumed size based on first row?
                    inserters.add(new DoubleListInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                        ASSUMED_BYTES_VAL_BYTE_SIZE));
                    break;
                case DOUBLE_SET:
                    colMetadataBuilder =
                        createColumnMetadataBuilder(spec.getColumnNames()[i], "bytes", "object", Type.DOUBLE_SET);
                    inserters.add(new DoubleSetInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                        ASSUMED_BYTES_VAL_BYTE_SIZE));
                    break;
                case BOOLEAN_LIST:
                    // TODO "bytes" and "object" etc maybe part of enum?
                    colMetadataBuilder =
                        createColumnMetadataBuilder(spec.getColumnNames()[i], "bytes", "object", Type.BOOLEAN_LIST);

                    //TODO maybe guess assumed size based on first row?
                    inserters.add(new BooleanListInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                        ASSUMED_BYTES_VAL_BYTE_SIZE));
                    break;
                case BOOLEAN_SET:
                    colMetadataBuilder =
                        createColumnMetadataBuilder(spec.getColumnNames()[i], "bytes", "object", Type.BOOLEAN_SET);
                    inserters.add(new BooleanSetInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                        ASSUMED_BYTES_VAL_BYTE_SIZE));
                    break;
                case STRING_LIST:
                    // TODO "bytes" and "object" etc maybe part of enum?
                    colMetadataBuilder =
                        createColumnMetadataBuilder(spec.getColumnNames()[i], "bytes", "object", Type.STRING_LIST);

                    //TODO maybe guess assumed size based on first row?
                    inserters.add(new StringListInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                        ASSUMED_BYTES_VAL_BYTE_SIZE));
                    break;
                case STRING_SET:
                    colMetadataBuilder =
                        createColumnMetadataBuilder(spec.getColumnNames()[i], "bytes", "object", Type.STRING_SET);
                    inserters.add(new StringSetInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                        ASSUMED_BYTES_VAL_BYTE_SIZE));
                    break;
                case BYTES_LIST:
                    // TODO "bytes" and "object" etc maybe part of enum?
                    colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], "bytes", "object",
                        Type.BYTES_LIST, spec.getColumnSerializers().get(spec.getColumnNames()[i]));

                    //TODO maybe guess assumed size based on first row?
                    inserters.add(new BytesListInserter(spec.getColumnNames()[i], rootAllocator, numRows,
                        ASSUMED_BYTES_VAL_BYTE_SIZE));
                    break;
                case BYTES_SET:
                    colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], "bytes", "object",
                        Type.BYTES_SET, spec.getColumnSerializers().get(spec.getColumnNames()[i]));
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

        while (tableIterator.hasNext()) {
            Row row = tableIterator.next();
            inserters.get(0).put(new CellImpl(row.getRowKey()));

            for (int i = 0; i < spec.getNumberColumns(); i++) {
                inserters.get(i + 1).put(row.getCell(i));
            }
        }

        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("pandas", metadataBuilder.build().toString());

        List<FieldVector> vecs = new ArrayList<FieldVector>();
        List<Field> fields = new ArrayList<Field>();
        for (int i = 0; i < inserters.size(); i++) {
            final FieldVector vec = inserters.get(i).retrieveVector();
            vecs.add(vec);
            fields.add(vec.getField());
        }

        Schema schema = new Schema(fields, metadata);
        VectorSchemaRoot vsr = new VectorSchemaRoot(schema, vecs, numRows);
        ArrowStreamWriter writer = new ArrowStreamWriter(vsr, null, fc);

        writer.writeBatch();
        writer.close();
        fc.close();

        //Close inserters to free memory
        for(ArrowVectorInserter is:inserters) {
            is.close();
        }
        rootAllocator.close();

        return path.getBytes("UTF-8");
    }

    private ArrowStreamReader getReader(final File file) throws FileNotFoundException {
        if (m_streamReader == null) {
            // TODO when do we actually close this guy?
            ArrowStreamReader reader =
                new ArrowStreamReader(new RandomAccessFile(file, "rw").getChannel(), new RootAllocator(Long.MAX_VALUE));
            m_streamReader = reader;
        }
        return m_streamReader;
    }

    @Override
    public void bytesIntoTable(final TableCreator<?> tableCreator, final byte[] bytes,
        final SerializationOptions serializationOptions) {
        // TODO sentinel
        String path = new String(bytes);
        TableSpec spec = tableSpecFromBytes(bytes);
        if (spec.getNumberColumns() > 0) {
            final File f = new File(path);
            try {
                ArrowStreamReader reader = getReader(f);
                // Index is always string
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                Type[] types = spec.getColumnTypes();
                String[] names = spec.getColumnNames();

                List<VectorExtractor> extractors = new ArrayList<VectorExtractor>();
                extractors.add(new StringExtractor((NullableVarCharVector)root.getVector(m_indexColumnName)));

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
                            case STRING:
                                extractors.add(new StringExtractor((NullableVarCharVector)root.getVector(names[j])));
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

                for (int i = 0; i < root.getRowCount(); i++) {
                    Row row = new RowImpl(extractors.get(0).extract().getStringValue(), spec.getNumberColumns());
                    for (int j = 0; j < spec.getNumberColumns(); j++) {
                        row.setCell(extractors.get(j + 1).extract(), j);
                    }
                    tableCreator.addRow(row);
                }
                reader.close();
            } catch (FileNotFoundException e) {
                // TODO better logging
                e.printStackTrace();
            } catch (IOException e) {
                // TODO better logging
                e.printStackTrace();
            } finally {
                if (f.exists()) {
                    f.delete();
                }
            }
        }
        m_streamReader = null;
        m_tableSpec = null;
    }

    @Override
    public TableSpec tableSpecFromBytes(final byte[] bytes) {

        String path = new String(bytes);
        if (m_tableSpec == null) {
            m_fromPythonPath = path;
            final File f = new File(path);
            if (f.exists()) {
                try {
                    ArrowStreamReader reader = getReader(f);
                    reader.loadNextBatch();
                    Schema schema = reader.getVectorSchemaRoot().getSchema();
                    Map<String, String> metadata = schema.getCustomMetadata();
                    Map<String, String> columnSerializers = new HashMap<String, String>();
                    String pandas_metadata = metadata.get("pandas");
                    if (pandas_metadata != null) {
                        JsonReader jsreader = Json.createReader(new StringReader(pandas_metadata));
                        JsonObject jpandas_metadata = jsreader.readObject();
                        JsonArray index_cols = jpandas_metadata.getJsonArray("index_columns");
                        JsonArray cols = jpandas_metadata.getJsonArray("columns");
                        JsonArray missing_cols = jpandas_metadata.getJsonArray("missing_columns");
                        String[] names = new String[cols.size() - index_cols.size()];
                        Type[] types = new Type[cols.size() - index_cols.size()];
                        int noIdxCtr = 0;
                        for (int i = 0; i < cols.size(); i++) {
                            JsonObject col = cols.getJsonObject(i);
                            boolean contained = false;
                            for (int j = 0; j < index_cols.size(); j++) {
                                if (index_cols.getString(j).contentEquals(col.getString("name"))) {
                                    contained = true;
                                    break;
                                }
                            }
                            if (contained) {
                                m_indexColumnName = col.getString("name");
                                continue;
                            }
                            names[noIdxCtr] = col.getString("name");
                            JsonObject typeObj = col.getJsonObject("metadata");
                            types[noIdxCtr] = Type.getTypeForId(typeObj.getInt("type_id"));
                            if (types[noIdxCtr] == Type.BYTES || types[noIdxCtr] == Type.BYTES_LIST
                                || types[noIdxCtr] == Type.BYTES_SET) {
                                String serializerId = col.getJsonObject("metadata").getString("serializer_id");
                                if (!StringUtils.isBlank(serializerId)) {
                                    columnSerializers.put(names[noIdxCtr], serializerId);
                                }
                            }
                            noIdxCtr++;
                        }
                        m_missingColumnNames = new String[missing_cols.size()];
                        for (int i = 0; i < missing_cols.size(); i++) {
                            m_missingColumnNames[i] = missing_cols.getString(i);
                        }
                        m_tableSpec = new TableSpecImpl(types, names, columnSerializers);
                    }
                } catch (IOException e) {
                    //TODO is illegal state the correct exception here?
                    //TODO better logging
                    throw new IllegalStateException(e);
                }
            } else {
                m_tableSpec = new TableSpecImpl(new Type[0], new String[0], null);
            }
        } else if (!path.contentEquals(m_fromPythonPath)) {
            throw new IllegalStateException("New table spec was requested before the old table was read!");
        }

        if (m_tableSpec == null) {
            throw new IllegalStateException("Could not build TableSpec!");
        }

        return m_tableSpec;
    }

    //    /**
    //     * Rounds down the provided value to the nearest power of two.
    //     *
    //     * @param val An integer value.
    //     * @return The closest power of two of that value.
    //     */
    //    private static int nextSmallerPowerOfTwo(final int val) {
    //        int highestBit = Integer.highestOneBit(val);
    //        if (highestBit == val) {
    //            return val;
    //        } else {
    //            return highestBit;
    //        }
    //    }

    //    private byte[] tableToBytesFixedSize(final TableIterator tableIterator,
    //        final SerializationOptions serializationOptions, final FileChannel fc, final String path) throws IOException {
    //        // Get metadata
    //        final String INDEX_COL_NAME = "__index_level_0__";
    //        JsonObjectBuilder metadataBuilder = Json.createObjectBuilder();
    //        TableSpec spec = tableIterator.getTableSpec();
    //        JsonArrayBuilder icBuilder = Json.createArrayBuilder();
    //        icBuilder.add(INDEX_COL_NAME);
    //        metadataBuilder.add("index_columns", icBuilder);
    //        JsonArrayBuilder colBuilder = Json.createArrayBuilder();
    //
    //        int perRowEstimateBit = 0;
    //        // Row ids
    //        JsonObjectBuilder rowIdBuilder =
    //            createColumnMetadataBuilder(INDEX_COL_NAME, "unicode", "object", Type.STRING, "");
    //        //row_id length + offsetVector entry length + null vector entry length
    //        perRowEstimateBit += 8 * (ASSUMED_ROWID_VAL_BYTE_SIZE + 4) + 1;
    //        colBuilder.add(rowIdBuilder);
    //        //Regular columns
    //        for (int i = 0; i < spec.getNumberColumns(); i++) {
    //            JsonObjectBuilder colMetadataBuilder;
    //            switch (spec.getColumnTypes()[i]) {
    //                case BOOLEAN:
    //                    colMetadataBuilder =
    //                        createColumnMetadataBuilder(spec.getColumnNames()[i], "bool", "object", Type.BOOLEAN, "");
    //                    //Null vector + value vector = 2 Bit
    //                    perRowEstimateBit += 2;
    //                    break;
    //                case INTEGER:
    //                    colMetadataBuilder =
    //                        createColumnMetadataBuilder(spec.getColumnNames()[i], "int", "int32", Type.INTEGER, "");
    //                    perRowEstimateBit += 33;
    //                    break;
    //                case LONG:
    //                    colMetadataBuilder =
    //                        createColumnMetadataBuilder(spec.getColumnNames()[i], "int", "int64", Type.LONG, "");
    //                    perRowEstimateBit += 65;
    //                    break;
    //                case DOUBLE:
    //                    colMetadataBuilder =
    //                        createColumnMetadataBuilder(spec.getColumnNames()[i], "int", "float64", Type.DOUBLE, "");
    //                    perRowEstimateBit += 65;
    //                    break;
    //                case STRING:
    //                    colMetadataBuilder =
    //                        createColumnMetadataBuilder(spec.getColumnNames()[i], "unicode", "object", Type.STRING, "");
    //                    //string length + offsetVector entry length + null vector entry length
    //                    perRowEstimateBit += 8 * (ASSUMED_STRING_VAL_BYTE_SIZE + 4) + 1;
    //                    break;
    //                case BYTES:
    //                    colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], "bytes", "object",
    //                        Type.BYTES, spec.getColumnSerializers().get(spec.getColumnNames()[i]));
    //                    //string length + offsetVector entry length + null vector entry length
    //                    perRowEstimateBit += 8 * (ASSUMED_BYTES_VAL_BYTE_SIZE + 4) + 1;
    //                    break;
    //                default:
    //                    throw new IllegalStateException(
    //                        "Serialization is not implemented for type: " + spec.getColumnTypes()[i].name());
    //            }
    //            colBuilder.add(colMetadataBuilder);
    //        }
    //        metadataBuilder.add("columns", colBuilder);
    //        //Build pandas metadata
    //        Map<String, String> metadata = new HashMap<String, String>();
    //        metadata.put("pandas", metadataBuilder.build().toString());
    //
    //        //Maximum overhead per column 2 * 63 Bit < 16 Byte
    //        int numRowsEstimateBatch = (int)(8 * (FIXED_BATCH_BYTE_SIZE - 16) / perRowEstimateBit);
    //
    //        Row cachRow = null;
    //        ArrowBatchWriter writer = null;
    //        Schema schema = null;
    //
    //        while (tableIterator.hasNext()) {
    //
    //            BufferAllocator allocator = new RootAllocator(FIXED_BATCH_BYTE_SIZE);
    //
    //            /*int numRowsInBatch = Math.min(numRowsEstimateBatch,
    //                    tableIterator.getNumberRemainingRows() + ((cachRow == null) ? 0 : 1)); */
    //            int numRowsInBatch = nextSmallerPowerOfTwo(numRowsEstimateBatch);
    //
    //            List<FieldVector> vecs = new ArrayList<FieldVector>();
    //            List<Field> fields = new ArrayList<Field>();
    //
    //            //Row ids
    //            NullableVarCharVector rowIdVector = new NullableVarCharVector(INDEX_COL_NAME, allocator);
    //            rowIdVector.allocateNew(nextSmallerPowerOfTwo(ASSUMED_ROWID_VAL_BYTE_SIZE * numRowsInBatch),
    //                numRowsInBatch - 1);
    //            vecs.add(rowIdVector);
    //            fields.add(rowIdVector.getField());
    //            // Create FieldVectors and metadata
    //            for (int i = 0; i < spec.getNumberColumns(); i++) {
    //
    //                FieldVector vec;
    //                switch (spec.getColumnTypes()[i]) {
    //                    case BOOLEAN:
    //
    //                        // Allocate vector for column
    //                        NullableBitVector bovec = new NullableBitVector(spec.getColumnNames()[i], allocator);
    //                        bovec.allocateNew(numRowsInBatch);
    //                        vec = bovec;
    //                        break;
    //                    case INTEGER:
    //
    //                        // Allocate vector for column
    //                        NullableIntVector ivec = new NullableIntVector(spec.getColumnNames()[i], allocator);
    //                        ivec.allocateNew(numRowsInBatch);
    //                        vec = ivec;
    //                        break;
    //                    case LONG:
    //
    //                        // Allocate vector for column
    //                        NullableBigIntVector lvec = new NullableBigIntVector(spec.getColumnNames()[i], allocator);
    //                        lvec.allocateNew(numRowsInBatch);
    //                        vec = lvec;
    //                        break;
    //                    case DOUBLE:
    //
    //                        // Allocate vector for column
    //                        NullableFloat8Vector dvec = new NullableFloat8Vector(spec.getColumnNames()[i], allocator);
    //                        dvec.allocateNew(numRowsInBatch);
    //                        vec = dvec;
    //                        break;
    //                    case STRING:
    //
    //                        NullableVarCharVector vvec = new NullableVarCharVector(spec.getColumnNames()[i], allocator);
    //                        vvec.allocateNew(nextSmallerPowerOfTwo(ASSUMED_STRING_VAL_BYTE_SIZE * numRowsInBatch),
    //                            numRowsInBatch - 1);
    //                        vec = vvec;
    //                        break;
    //                    case BYTES:
    //
    //                        NullableVarBinaryVector bvec = new NullableVarBinaryVector(spec.getColumnNames()[i], allocator);
    //                        bvec.allocateNew(nextSmallerPowerOfTwo(ASSUMED_BYTES_VAL_BYTE_SIZE * numRowsInBatch),
    //                            numRowsInBatch - 1);
    //                        vec = bvec;
    //                        break;
    //                    default:
    //                        throw new IllegalStateException(
    //                            "Serialization is not implemented for type: " + spec.getColumnTypes()[i].name());
    //                }
    //                vecs.add(vec);
    //                fields.add(vec.getField());
    //            }
    //
    //            int ctr = 0;
    //            int[] val_length = new int[vecs.size()];
    //            boolean bufferFull = false;
    //            boolean first = true;
    //
    //            //TODO check row size < FIXED_BUFFER_SIZE
    //            while (tableIterator.hasNext() && !bufferFull && ctr < numRowsInBatch) {
    //                Row row;
    //                if (first && cachRow != null) {
    //                    row = cachRow;
    //                    cachRow = null;
    //                } else {
    //                    row = tableIterator.next();
    //                }
    //                first = false;
    //                byte[] bRowKey = row.getRowKey().getBytes("UTF-8");
    //                val_length[0] += bRowKey.length;
    //                if (val_length[0] > ((NullableVarCharVector)vecs.get(0)).getByteCapacity()) {
    //                    bufferFull = true;
    //                } else {
    //                    ((NullableVarCharVector.Mutator)vecs.get(0).getMutator()).set(ctr, bRowKey);
    //                    for (int i = 0; i < spec.getNumberColumns(); i++) {
    //                        if (bufferFull) {
    //                            break;
    //                        }
    //                        switch (spec.getColumnTypes()[i]) {
    //                            case BOOLEAN:
    //                                if (row.getCell(i).isMissing()) {
    //                                    ((NullableBitVector.Mutator)vecs.get(i + 1).getMutator()).setNull(ctr);
    //                                } else {
    //                                    val_length[i + 1]++;
    //                                    ((NullableBitVector.Mutator)vecs.get(i + 1).getMutator()).set(ctr,
    //                                        row.getCell(i).getBooleanValue().booleanValue() ? 1 : 0);
    //                                }
    //                                break;
    //                            case INTEGER:
    //                                if (row.getCell(i).isMissing()) {
    //                                    if (serializationOptions.getConvertMissingToPython()) {
    //                                        val_length[i + 1]++;
    //                                        ((NullableIntVector.Mutator)vecs.get(i + 1).getMutator()).set(ctr,
    //                                            (int)serializationOptions.getSentinelForType(Type.INTEGER));
    //                                    } else {
    //                                        ((NullableIntVector.Mutator)vecs.get(i + 1).getMutator()).setNull(ctr);
    //                                    }
    //                                } else {
    //                                    val_length[i + 1]++;
    //                                    ((NullableIntVector.Mutator)vecs.get(i + 1).getMutator()).set(ctr,
    //                                        row.getCell(i).getIntegerValue().intValue());
    //                                }
    //                                break;
    //                            case LONG:
    //                                if (row.getCell(i).isMissing()) {
    //                                    if (serializationOptions.getConvertMissingToPython()) {
    //                                        val_length[i + 1]++;
    //                                        ((NullableBigIntVector.Mutator)vecs.get(i + 1).getMutator()).set(ctr,
    //                                            serializationOptions.getSentinelForType(Type.LONG));
    //                                    } else {
    //                                        ((NullableBigIntVector.Mutator)vecs.get(i + 1).getMutator()).setNull(ctr);
    //                                    }
    //                                } else {
    //                                    val_length[i + 1]++;
    //                                    ((NullableBigIntVector.Mutator)vecs.get(i + 1).getMutator()).set(ctr,
    //                                        row.getCell(i).getLongValue().longValue());
    //                                }
    //                                break;
    //                            case DOUBLE:
    //                                if (row.getCell(i).isMissing()) {
    //                                    ((NullableFloat8Vector.Mutator)vecs.get(i + 1).getMutator()).setNull(ctr);
    //                                } else {
    //                                    val_length[i + 1]++;
    //                                    ((NullableFloat8Vector.Mutator)vecs.get(i + 1).getMutator()).set(ctr,
    //                                        row.getCell(i).getDoubleValue().doubleValue());
    //                                }
    //                                break;
    //                            case STRING:
    //                                if (ctr >= ((NullableVarCharVector)vecs.get(i + 1)).getValueCapacity()) {
    //                                    bufferFull = true;
    //                                    break;
    //                                }
    //                                if (row.getCell(i).isMissing()) {
    //                                    ((NullableVarCharVector.Mutator)vecs.get(i + 1).getMutator()).setNull(ctr);
    //                                } else {
    //                                    byte[] bVal = row.getCell(i).getStringValue().getBytes("UTF-8");
    //                                    val_length[i + 1] += bVal.length;
    //                                    if (val_length[i + 1] > ((NullableVarCharVector)vecs.get(i + 1))
    //                                        .getByteCapacity()) {
    //                                        bufferFull = true;
    //                                        break;
    //                                    }
    //                                    ((NullableVarCharVector.Mutator)vecs.get(i + 1).getMutator()).set(ctr, bVal);
    //                                }
    //                                break;
    //                            case BYTES:
    //                                if (ctr >= ((NullableVarBinaryVector)vecs.get(i + 1)).getValueCapacity()) {
    //                                    bufferFull = true;
    //                                    break;
    //                                }
    //                                if (row.getCell(i).isMissing()) {
    //                                    ((NullableVarBinaryVector.Mutator)vecs.get(i + 1).getMutator()).setNull(ctr);
    //                                } else {
    //                                    //TODO ugly
    //                                    byte[] bytes = ArrayUtils.toPrimitive(row.getCell(i).getBytesValue());
    //                                    val_length[i + 1] += bytes.length;
    //                                    if (val_length[i + 1] > ((NullableVarBinaryVector)vecs.get(i + 1))
    //                                        .getByteCapacity()) {
    //                                        bufferFull = true;
    //                                        break;
    //                                    }
    //                                    ((NullableVarBinaryVector.Mutator)vecs.get(i + 1).getMutator()).set(ctr, bytes);
    //                                }
    //                                break;
    //                            default:
    //                                throw new IllegalStateException(
    //                                    "Serialization is not implemented for type: " + spec.getColumnTypes()[i].name());
    //                        }
    //                    }
    //                    if (!bufferFull) {
    //                        for (int col = 0; col < spec.getNumberColumns() + 1; col++) {
    //                            vecs.get(col).getMutator().setValueCount(ctr + 1);
    //                        }
    //                    }
    //                }
    //                if (bufferFull) {
    //                    cachRow = row;
    //                } else {
    //                    ctr++;
    //                }
    //            }
    //
    //            VectorSchemaRoot vsr;
    //            if (writer == null) {
    //                schema = new Schema(fields, metadata);
    //                vsr = new VectorSchemaRoot(schema, vecs, ctr);
    //                writer = new ArrowBatchWriter(vsr, null, fc);
    //            } else {
    //                vsr = new VectorSchemaRoot(schema, vecs, ctr);
    //            }
    //            writer.writeRecordBatch(new VectorUnloader(vsr).getRecordBatch());
    //        }
    //        writer.close();
    //        fc.close();
    //
    //        return path.getBytes("UTF-8");
    //    }

}
