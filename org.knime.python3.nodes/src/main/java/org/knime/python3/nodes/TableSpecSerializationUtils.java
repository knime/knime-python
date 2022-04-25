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
package org.knime.python3.nodes;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.IDataRepository;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.filestore.FileStore;
import org.knime.core.data.filestore.FileStoreKey;
import org.knime.core.data.filestore.internal.FileStoreProxy.FlushCallback;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.schema.ValueSchemaUtils;
import org.knime.core.node.ExecutionContext;
import org.knime.core.table.schema.AnnotatedColumnarSchema;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DefaultAnnotatedColumnarSchema;
import org.knime.core.table.virtual.serialization.AnnotatedColumnarSchemaSerializer;
import org.knime.python3.arrow.PythonArrowDataUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * Provide helper methods to serialize and deserialize DataTableSpecs
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class TableSpecSerializationUtils {
    /**
     * Convert an Array of {@link DataTableSpec} to a serialized string representation using JSON
     *
     * @param specs The {@link DataTableSpec}s to serialize to JSON
     * @return The string representations of the JSON serialized {@link DataTableSpec}s.
     */
    static String[] serializeTableSpecs(final DataTableSpec[] specs) {
        final AnnotatedColumnarSchema[] inSchemas =
            Stream.of(specs).map(TableSpecSerializationUtils::specToSchema).toArray(AnnotatedColumnarSchema[]::new);

        return Arrays.stream(inSchemas).map(TableSpecSerializationUtils::serializeColumnarValueSchema)
            .toArray(String[]::new);
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
     * {@link ColumnarSchema}. We do not need full {@link ColumnarValueSchema} but are reusing the conversion methods
     * from the {@link ValueSchemaUtils}, so we pass in a dummy file store handler.
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
        final var vs = ValueSchemaUtils.create(spec, RowKeyType.CUSTOM, new DummyFileStoreHandler());
        final var cvs = ColumnarValueSchemaUtils.create(vs);
        // TODO: pass metadata as well?
        var columnNames = new String[spec.getNumColumns() + 1];
        columnNames[0] = "RowKey";
        System.arraycopy(spec.getColumnNames(), 0, columnNames, 1, spec.getNumColumns());
        return DefaultAnnotatedColumnarSchema.annotate(cvs, columnNames);
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
}
