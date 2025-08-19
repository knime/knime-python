/*
 * ------------------------------------------------------------------------
 *
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
 * ---------------------------------------------------------------------
 *
 * History
 *   22 Apr 2022 (chaubold): created
 */
package org.knime.python3.nodes.ports;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.IDataRepository;
import org.knime.core.data.filestore.FileStore;
import org.knime.core.data.filestore.FileStoreKey;
import org.knime.core.data.filestore.internal.FileStoreProxy.FlushCallback;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.ValueFactoryUtils;
import org.knime.core.data.v2.schema.DataTableValueSchema;
import org.knime.core.data.v2.schema.DataTableValueSchemaUtils;
import org.knime.core.node.ExecutionContext;
import org.knime.core.table.schema.AnnotatedColumnarSchema;
import org.knime.core.table.schema.AnnotatedColumnarSchema.ColumnMetaData;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DefaultAnnotatedColumnarSchema;
import org.knime.core.table.virtual.serialization.AnnotatedColumnarSchemaSerializer;
import org.knime.core.table.virtual.serialization.ColumnarSchemaSerializer;
import org.knime.python3.arrow.PythonArrowDataUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Provide helper methods to serialize and deserialize DataTableSpecs
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class TableSpecSerializationUtils {
    /**
     * Convert a {@link DataTableSpec} to a serialized string representation using JSON
     *
     * @param spec The {@link DataTableSpec} to serialize to JSON
     * @return The string representation of the JSON serialized {@link DataTableSpec}.
     */
    static String serializeTableSpec(final DataTableSpec spec) {
        final var annotatedSchema = specToSchema(spec);
        return serializeColumnarValueSchema(annotatedSchema);
    }

    /**
     * Deserialize a list of JSON-serialized {@link DataTableSpec}s from strings
     *
     * @param serializedSpecs The string representations of the JSON serialized {@link DataTableSpec}s.
     * @return The deserialized {@link DataTableSpec}s
     */
    static DataTableSpec[] deserializeTableSpecs(final List<String> serializedSpecs) {
        AnnotatedColumnarSchema[] outSchemas =
            serializedSpecs.stream().map(TableSpecSerializationUtils::deserializeAnnotatedColumnarSchema)
                .toArray(AnnotatedColumnarSchema[]::new);

        return Arrays.stream(outSchemas).map(acs -> {
            return PythonArrowDataUtils.createDataTableSpec(acs, acs.getColumnNames());
        }).toArray(DataTableSpec[]::new);
    }

    private TableSpecSerializationUtils() {
    }

    /**
     * Dummy implementation of a file store handler because we're only using it when converting a DataTableSpec to a
     * {@link ColumnarSchema}. We do not need full {@link DataTableValueSchema} but are reusing the conversion methods
     * from the {@link DataTableValueSchemaUtils}, so we pass in a dummy file store handler.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    private static class DummyFileStoreHandler implements IWriteFileStoreHandler {

        @Override
        public IDataRepository getDataRepository() {
            return null;
        }

        @Override
        public void clearAndDispose() {
            // empty because not needed for dummy implementation
        }

        @Override
        public FileStore getFileStore(final FileStoreKey key) {
            return null;
        }

        @Override
        public FileStore createFileStore(final String name) throws IOException {
            return null;
        }

        @Override
        public FileStore createFileStore(final String name, final int[] nestedLoopPath, final int iterationIndex)
            throws IOException {
            return null;
        }

        @Override
        public void open(final ExecutionContext exec) {
            // empty because not needed for dummy implementation
        }

        @Override
        public void addToRepository(final IDataRepository repository) {
            // empty because not needed for dummy implementation
        }

        @Override
        public void close() {
            // empty because not needed for dummy implementation
        }

        @Override
        public void ensureOpenAfterLoad() throws IOException {
            // empty because not needed for dummy implementation
        }

        @Override
        public FileStoreKey translateToLocal(final FileStore fs, final FlushCallback flushCallback) {
            return null;
        }

        @Override
        public boolean mustBeFlushedPriorSave(final FileStore fs) {
            return false;
        }

        @Override
        public UUID getStoreUUID() {
            return null;
        }

        @Override
        public File getBaseDir() {
            return null;
        }

        @Override
        public boolean isReference() {
            return false;
        }

    }

    private static AnnotatedColumnarSchema specToSchema(final DataTableSpec spec) {
        final var vs = DataTableValueSchemaUtils.create(spec, RowKeyType.CUSTOM, new DummyFileStoreHandler());
        var columnNames = new String[spec.getNumColumns() + 1];
        columnNames[0] = "RowKey";
        var columnMetaData = new ColumnMetaData[columnNames.length];
        // Set metadata object for the RowKey column to null
        columnMetaData[0] = null;
        for (var i = 0; i < columnMetaData.length - 1; i++) {
            columnMetaData[i + 1] = new PythonColumnMetaData(spec.getColumnSpec(i).getType());
        }
        System.arraycopy(spec.getColumnNames(), 0, columnNames, 1, spec.getNumColumns());
        return DefaultAnnotatedColumnarSchema.annotate(vs, columnNames, columnMetaData);
    }

    private static final class PythonColumnMetaData implements ColumnMetaData {

        private final String m_preferredValueType;

        private final String m_displayedColumnType;

        public PythonColumnMetaData(final DataType dataType) {
            m_preferredValueType = dataType.getPreferredValueClass().getName();
            m_displayedColumnType = dataType.getName();
        }

        @Override
        public JsonNode toJson(final JsonNodeFactory factory) {
            ObjectNode objectNode = factory.objectNode();
            objectNode.put("preferred_value_type", m_preferredValueType);
            objectNode.put("displayed_column_type", m_displayedColumnType);
            return objectNode;
        }

    }

    private static String serializeColumnarValueSchema(final AnnotatedColumnarSchema schema) {
        return AnnotatedColumnarSchemaSerializer.save(schema, JsonNodeFactory.instance).toString();
    }

    private static AnnotatedColumnarSchema deserializeAnnotatedColumnarSchema(final String serializedSchema) {
        final var om = new ObjectMapper();
        try {
            return AnnotatedColumnarSchemaSerializer.load(om.readTree(serializedSchema));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Python node returned invalid serialized columnar schema", ex);
        }
    }

    private static JsonNode preferredValueTypeToJson(final DataType dataType, final JsonNodeFactory factory) {
        return (new PythonColumnMetaData(dataType)).toJson(factory);
    }

    /**
     * Given a JSON-serialized {@link AnnotatedColumnarSchema}, this method will return a string-serialized JSON array
     * of elements with "preferred_value_type" and "display_column_type"s which are needed for the UI schema generation
     * of all column selection UI elements.
     *
     * @param tableSchemaJson A JSON-serialized {@link AnnotatedColumnarSchema}
     * @return A JSON array of elements with "preferred_value_type"s, serialized to string
     */
    public static String getPreferredValueTypesForSerializedSchema(final String tableSchemaJson) {
        final var om = new ObjectMapper();
        try {
            final var rootNode = om.readTree(tableSchemaJson);
            final var columnarSchema = ColumnarSchemaSerializer.load(rootNode.get("schema"));

            final var factory = JsonNodeFactory.instance;
            final var arrayNode = factory.arrayNode();
            // We don't have a preferred_value_type for the RowKey
            for (var columnIdx = 1; columnIdx < columnarSchema.numColumns(); columnIdx++) {
                final var dataType = ValueFactoryUtils.getDataTypeForTraits(columnarSchema.getTraits(columnIdx));
                arrayNode.add(preferredValueTypeToJson(dataType, factory));
            }

            return arrayNode.toString();
        } catch (JsonProcessingException ex) { // NOSONAR: if we don't split this block up, Eclipse doesn't like it for some reason
            throw new IllegalStateException("Could not parse AnnotatedColumnarSchema from given JSON data", ex);
        }
    }

}
