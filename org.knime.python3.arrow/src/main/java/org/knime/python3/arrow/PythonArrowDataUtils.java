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
 *   May 6, 2021 (benjamin): created
 */
package org.knime.python3.arrow;

import java.io.Flushable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.knime.core.columnar.arrow.ArrowBatchReadStore;
import org.knime.core.columnar.arrow.ArrowBatchStore;
import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.columnar.arrow.ArrowReaderWriterUtils;
import org.knime.core.columnar.arrow.ArrowReaderWriterUtils.OffsetProvider;
import org.knime.core.columnar.arrow.ArrowSchemaUtils;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataTypeRegistry;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.UnsavedColumnarContainerTable;
import org.knime.core.data.v2.DataCellSerializerFactory;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.ValueFactoryUtils;
import org.knime.core.data.v2.ValueSchema;
import org.knime.core.data.v2.value.DefaultRowKeyValueFactory;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.LogicalTypeTrait;
import org.knime.core.util.DuplicateKeyException;
import org.knime.python3.PythonDataSink;
import org.knime.python3.PythonDataSource;
import org.knime.python3.PythonEntryPoint;
import org.knime.python3.PythonException;

/**
 * Utilities for handling {@link PythonDataSource} and {@link PythonDataSink} for Arrow data that needs to be transfered
 * to and from Python.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class PythonArrowDataUtils {

    private PythonArrowDataUtils() {
    }

    /**
     * Create a {@link PythonArrowDataSource} that provides the data from the given {@link ArrowBatchStore}.
     *
     * @param store the store which holds the data
     * @param numBatches the total number of batches that are available at the store
     * @return the {@link PythonArrowDataSource} that can be given to a {@link PythonEntryPoint} and will be wrapped
     *         into a Python object for easy access to the data
     */
    public static PythonArrowDataSource createSource(final ArrowBatchStore store, final int numBatches) {
        return new PythonArrowBatchStoreDataSource(store.getPath().toAbsolutePath().toString(),
            store.getOffsetProvider(), numBatches, null);
    }

    /**
     * Create a {@link PythonArrowDataSource} that provides the data from the given {@link ArrowBatchStore} and has the
     * provided column names
     *
     *
     * @param store the store which holds the data
     * @param numBatches the total number of batches that are available at the store
     * @param columnNames names of the columns in KNIME
     * @return the {@link PythonArrowDataSource} that can be given to a {@link PythonEntryPoint} and will be wrapped
     *         into a Python object for easy access to the data
     */
    public static PythonArrowDataSource createSource(final ArrowBatchStore store, final int numBatches,
        final String[] columnNames) {
        return new PythonArrowBatchStoreDataSource(store.getPath().toAbsolutePath().toString(),
            store.getOffsetProvider(), numBatches, columnNames);
    }

    /**
     * Create a {@link PythonArrowDataSource} that provides the data from the given {@link ArrowBatchReadStore}
     *
     * @param store the store which holds the data
     * @return the {@link PythonArrowDataSource} that can be given to a {@link PythonEntryPoint} and will be wrapped
     *         into a Python object for easy access to the data
     */
    public static PythonArrowDataSource createSource(final ArrowBatchReadStore store) {
        return new PythonArrowBatchStoreDataSource(store.getPath().toAbsolutePath().toString(), null,
            store.numBatches(), null);
    }

    /**
     * Create an {@link PythonArrowDataSink} that writes an Arrow file to the given path.
     *
     * @param targetPath the path to write the Arrow file to
     * @return a {@link PythonArrowDataSink} that can be given to a {@link PythonEntryPoint} and will be wrapped into a
     *         Python object for easy access for setting the data
     */
    public static DefaultPythonArrowDataSink createSink(final Path targetPath) {
        return new DefaultPythonArrowDataSink(targetPath);
    }

    /**
     * Create a {@link RowKeyChecker} that checks all batches that are written to the dataSink.
     *
     * @param dataSink the {@link PythonArrowDataSink} that data is written to
     * @param storeFactory an {@link ArrowColumnStoreFactory} to create the readable
     * @return A {@link RowKeyChecker}. After all batches have been written to the {@link PythonArrowDataSink}
     *         {@link RowKeyChecker#allUnique()} can be called to check if the row keys are unique.
     */
    public static RowKeyChecker createRowKeyChecker(final DefaultPythonArrowDataSink dataSink,
        final ArrowColumnStoreFactory storeFactory) {
        final var rowKeyChecker = RowKeyChecker.fromRandomAccessReadable(() -> createReadable(dataSink, storeFactory));
        dataSink.registerBatchListener(() -> {
            try {
                rowKeyChecker.checkNextBatch();
            } catch (final DuplicateKeyException | IOException e) {
                throw new PythonException(e.getMessage(), e);
            }
        });
        return rowKeyChecker;
    }

    /**
     * Create a {@link RandomAccessBatchReadable} that provides batches from the data written by the Python process to
     * the given {@link PythonArrowDataSink}.
     *
     * @param dataSink the data sink which was given to the Python process
     * @param storeFactory an {@link ArrowColumnStoreFactory} to create the readable
     * @return the {@link RandomAccessBatchReadable} with the data
     */
    public static RandomAccessBatchReadable createReadable(final DefaultPythonArrowDataSink dataSink,
        final ArrowColumnStoreFactory storeFactory) {
        // TODO Do not require DefaultPythonArrowDataSink but an interface
        return storeFactory.createPartialFileReadable(dataSink.getSchema(), dataSink.getPath(),
            getOffsetProvider(dataSink));
    }

    /**
     * Creates a {@link UnsavedColumnarContainerTable table} from the provided {@link DefaultPythonArrowDataSink
     * dataSink}.
     *
     * @param dataSink filled by Python
     * @param storeFactory for creation Arrow stores in Java
     * @param tableId for the newly created table (needed by KNIME for tracking)
     * @return the table with the content written into dataSink
     */
    @SuppressWarnings("resource") // the readStore will be closed when the table is cleared
    public static UnsavedColumnarContainerTable createTable(final DefaultPythonArrowDataSink dataSink,
        final ArrowColumnStoreFactory storeFactory, final int tableId) {
        var size = dataSink.getSize();
        var path = dataSink.getPath();
        var schema = createColumnarValueSchema(dataSink);
        var readStore = storeFactory.createReadStore(schema, path);
        // nothing to flash as of now
        Flushable flushable = () -> {
        };// prevent formatting
        return UnsavedColumnarContainerTable.create(path, tableId, storeFactory, schema, readStore, flushable, size);
    }

    private static ColumnarValueSchema createColumnarValueSchema(final DefaultPythonArrowDataSink dataSink) {
        final var schema = ArrowReaderWriterUtils.readSchema(dataSink.getPath().toFile());
        final var columnarSchema = ArrowSchemaUtils.convertSchema(schema);
        final var names = ArrowSchemaUtils.extractColumnNames(schema);
        final List<ValueFactory<?, ?>> factories = new ArrayList<>(columnarSchema.numColumns());
        final List<DataColumnSpec> specs = new ArrayList<>(columnarSchema.numColumns() - 1);
        factories.add(new DefaultRowKeyValueFactory());
        for (int i = 1; i < columnarSchema.numColumns(); i++) {//NOSONAR
            var traits = columnarSchema.getTraits(i);
            var valueFactory = getValueFactory(
                DataTraits.getTrait(traits, LogicalTypeTrait.class).map(LogicalTypeTrait::getLogicalType).orElse(null),
                columnarSchema.getSpec(i));
            factories.add(valueFactory);
            var dataType = DataTypeRegistry.getInstance().getDataTypeForValueFactory(valueFactory);
            specs.add(new DataColumnSpecCreator(names[i], dataType).createSpec());
        }
        var tableSpec = new DataTableSpec(specs.toArray(DataColumnSpec[]::new));
        // TODO shouldn't be necessary once we store the serializer alongside the data (AP-17501)
        var cellSerializerFactory = new DataCellSerializerFactory();
        return ColumnarValueSchemaUtils
            .create(ValueSchema.create(tableSpec, factories.toArray(ValueFactory<?, ?>[]::new), cellSerializerFactory));
    }

    private static ValueFactory<?, ?> getValueFactory(final String valueFactoryClassName, final DataSpec dataSpec) {
        if (valueFactoryClassName != null) {
            // TODO special handling for collections (and DataCellValueFactories?)
            return ValueFactoryUtils.createValueFactoryForClassName(valueFactoryClassName)//
                .orElseThrow(() -> new IllegalStateException(
                    String.format("No value factory with the name '%s' is known. Are you missing an extension?",
                        valueFactoryClassName)));
        } else {
            return ValueFactoryUtils.getDefaultValueFactory(dataSpec)//
                .orElseThrow(
                    () -> new IllegalArgumentException(String.format("There was no value factory provided from python "
                        + "and there also doesn't exist a default value factory for '%s'.", dataSpec)));
        }
    }

    /**
     * Create a {@link RandomAccessBatchReadable} that provides batches from the data written by the Python process to
     * the given {@link PythonArrowDataSink} and check that the data has a specific schema.
     *
     * @param dataSink the data sink which was given to the Python process
     * @param expectedSchema the expected schema
     * @param storeFactory an {@link ArrowColumnStoreFactory} to create the readable
     * @return the {@link RandomAccessBatchReadable} with the data
     * @throws IllegalStateException if Python did not report data with the expected schema to the sink
     */
    public static RandomAccessBatchReadable createReadable(final DefaultPythonArrowDataSink dataSink,
        final ColumnarSchema expectedSchema, final ArrowColumnStoreFactory storeFactory) {
        // TODO Do not require DefaultPythonArrowDataSink but an interface
        // TODO(extensiontypes) we need an expected schema with virtual types/extension types
        checkSchema(dataSink.getSchema(), expectedSchema);
        return createReadable(dataSink, storeFactory);
    }

    private static OffsetProvider getOffsetProvider(final DefaultPythonArrowDataSink dataSink) {
        return new OffsetProvider() {

            @Override
            public long getRecordBatchOffset(final int index) {
                return dataSink.getRecordBatchOffsets().get(index);
            }

            @Override
            public long[] getDictionaryBatchOffsets(final int index) {
                // TODO support dictionary batches
                return new long[0];
            }
        };
    }

    /** Check if the given schema fits the expected schema. Throws an {@link IllegalStateException} if not. */
    private static void checkSchema(final ColumnarSchema schema, final ColumnarSchema expectedSchema) {
        if (schema.numColumns() != expectedSchema.numColumns()) {
            // TODO test this
            throw new IllegalStateException("The schema by Python does not have the same amount of columns. "
                + "Expected " + expectedSchema.numColumns() + ", got " + schema.numColumns() + ".");
        }
        for (int i = 0; i < schema.numColumns(); i++) {
            final DataSpec expectedSpec = expectedSchema.getSpec(i);
            final DataSpec spec = schema.getSpec(i);
            if (!spec.equals(expectedSpec)) {
                // TODO test this
                throw new IllegalStateException("The schema by Python does not have the specs in column " + i + ". "
                    + "Expected " + expectedSpec + ", got " + spec + ".");
            }
        }
    }

    private static final class PythonArrowBatchStoreDataSource implements PythonArrowDataSource {

        private final String m_path;

        private final OffsetProvider m_offsetProvider;

        private final int m_numBatches;

        private final String[] m_columnNames;

        public PythonArrowBatchStoreDataSource(final String path, final OffsetProvider offsetProvider,
            final int numBatches, final String[] columnNames) {
            m_path = path;
            m_offsetProvider = offsetProvider;
            m_numBatches = numBatches;
            m_columnNames = columnNames;
        }

        @Override
        public String getAbsolutePath() {
            return m_path;
        }

        @Override
        public int numBatches() {
            return m_numBatches;
        }

        @Override
        public boolean isFooterWritten() {
            return m_offsetProvider == null;
        }

        @Override
        public long getRecordBatchOffset(final int index) {
            return m_offsetProvider.getRecordBatchOffset(index);
        }

        @Override
        public List<Long> getDictionaryBatchOffsets(final int index) {
            return Arrays.stream(m_offsetProvider.getDictionaryBatchOffsets(index)).boxed()
                .collect(Collectors.toList());
        }

        @Override
        public String[] getColumnNames() {
            return m_columnNames;
        }

        @Override
        public boolean hasColumnNames() {
            return m_columnNames != null;
        }
    }
}