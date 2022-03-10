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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.knime.core.columnar.arrow.ArrowBatchReadStore;
import org.knime.core.columnar.arrow.ArrowBatchStore;
import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.columnar.arrow.ArrowReaderWriterUtils;
import org.knime.core.columnar.arrow.ArrowReaderWriterUtils.OffsetProvider;
import org.knime.core.columnar.arrow.ArrowSchemaUtils;
import org.knime.core.columnar.arrow.PathBackedFileHandle;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.IDataRepository;
import org.knime.core.data.columnar.domain.DefaultDomainWritableConfig;
import org.knime.core.data.columnar.domain.DomainWritableConfig;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.ColumnarRowReadTable;
import org.knime.core.data.columnar.table.ColumnarRowWriteTable;
import org.knime.core.data.columnar.table.ColumnarRowWriteTableSettings;
import org.knime.core.data.columnar.table.UnsavedColumnarContainerTable;
import org.knime.core.data.meta.DataColumnMetaData;
import org.knime.core.data.v2.RowKeyValueFactory;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.ValueFactoryUtils;
import org.knime.core.data.v2.schema.ValueSchemaUtils;
import org.knime.core.data.v2.value.DefaultRowKeyValueFactory;
import org.knime.core.data.v2.value.VoidRowKeyFactory;
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

    private static final ColumnarRowWriteTableSettings EMPTY_TABLE_SETTINGS =
        new ColumnarRowWriteTableSettings(false, 0, false, false);

    private static final ColumnarValueSchema EMPTY_SCHEMA = ColumnarValueSchemaUtils
        .create(ValueSchemaUtils.create(new DataTableSpec(), new ValueFactory<?, ?>[]{VoidRowKeyFactory.INSTANCE}));

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
        return new PythonArrowBatchStoreDataSource(store.getFileHandle().asPath().toAbsolutePath().toString(),
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
        return new PythonArrowBatchStoreDataSource(store.getFileHandle().asPath().toAbsolutePath().toString(),
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
        return new PythonArrowBatchStoreDataSource(store.getFileHandle().asPath().toAbsolutePath().toString(), null,
            store.numBatches(), null);
    }

    /**
     * Create a {@link PythonArrowDataSource} that provides the data from the given {@link ArrowBatchReadStore} and has
     * the provided column names
     *
     * @param store the store which holds the data
     * @param columnNames names of the columns in KNIME
     * @return the {@link PythonArrowDataSource} that can be given to a {@link PythonEntryPoint} and will be wrapped
     *         into a Python object for easy access to the data
     */
    public static PythonArrowDataSource createSource(final ArrowBatchReadStore store, final String[] columnNames) {
        return new PythonArrowBatchStoreDataSource(store.getFileHandle().asPath().toAbsolutePath().toString(), null,
            store.numBatches(), columnNames);
    }

    /**
     * Create a {@link PythonArrowDataSink} that writes an Arrow file to the given path.
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
                rowKeyChecker.processNextBatch();
            } catch (IOException e) {
                throw new PythonException(e.getMessage(), e);
            }
        });
        return rowKeyChecker;
    }

    /**
     * Create a {@link DomainCalculator} that computes the domain of each batch asynchronously when they are provided
     * from Python
     *
     * @param sink the {@link PythonArrowDataSink} that data is written to
     * @param storeFactory an {@link ArrowColumnStoreFactory} to create the readable
     * @param maxPossibleNominalDomainValues the maximum number of possible values for nominal domains
     * @param dataRepository the {@link IDataRepository} to use for this table
     * @return A {@link DomainCalculator}. After all batches have been written to the {@link PythonArrowDataSink}
     *         {@link DomainCalculator#getDomain(int)} and {@link DomainCalculator#getMetadata(int)} can be called to
     *         obtain domain and metadata per column.
     */
    public static DomainCalculator createDomainCalculator(final DefaultPythonArrowDataSink sink,
        final ArrowColumnStoreFactory storeFactory, final int maxPossibleNominalDomainValues,
        final IDataRepository dataRepository) {
        // Create the domain calculator
        final Supplier<RandomAccessBatchReadable> batchReadableSupplier = () -> createReadable(sink, storeFactory);
        final Supplier<DomainWritableConfig> configSupplier = () -> {
            // NB: The schema will be known when this method is called
            final ColumnarValueSchema schema =
                PythonArrowDataUtils.createColumnarValueSchema(sink, TableDomainAndMetadata.empty(), dataRepository);
            return new DefaultDomainWritableConfig(schema, maxPossibleNominalDomainValues, false);
        };
        final var domainCalculator = DomainCalculator.fromRandomAccessReadable(batchReadableSupplier, configSupplier);

        // Register a listener that processes a batch as soon as available
        sink.registerBatchListener(() -> {
            try {
                domainCalculator.processNextBatch();
            } catch (final DuplicateKeyException | IOException e) {
                throw new PythonException(e.getMessage(), e);
            }
        });
        return domainCalculator;
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
        return storeFactory.createPartialFileReadable(dataSink.getPath(), getOffsetProvider(dataSink));
    }

    /**
     * Creates a {@link UnsavedColumnarContainerTable table} from the provided {@link DefaultPythonArrowDataSink
     * dataSink}.
     *
     * @param dataSink filled by Python
     * @param domainAndMetadata the domain and metadata for the table
     * @param storeFactory for creation Arrow stores in Java
     * @param dataRepository the {@link IDataRepository} to use for this table
     * @return the table with the content written into dataSink
     */
    @SuppressWarnings("resource") // the readStore will be closed when the table is cleared
    public static UnsavedColumnarContainerTable createTable(final DefaultPythonArrowDataSink dataSink,
        final TableDomainAndMetadata domainAndMetadata, final ArrowColumnStoreFactory storeFactory,
        final IDataRepository dataRepository) {
        final int tableId = dataRepository.generateNewID();
        final var size = dataSink.getSize();
        final var path = dataSink.getPath();
        if (isEmpty(path)) {
            return createEmptyTable(path, tableId, storeFactory);
        }
        final var schema = createColumnarValueSchema(dataSink, domainAndMetadata, dataRepository);
        final var readStore = storeFactory.createReadStore(path);
        return UnsavedColumnarContainerTable.create(tableId,
            new ColumnarRowReadTable(schema, storeFactory, readStore, size), () -> {
                /*Python already wrote everything to disk.*/});
    }

    private static boolean isEmpty(final Path path) {
        try {
            return Files.size(path) == 0;
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to obtain file size.", ex);
        }
    }

    @SuppressWarnings("resource") // table is henceforth managed by the returned UnsavedColumnarContainerTable
    private static UnsavedColumnarContainerTable createEmptyTable(final Path path, final int tableId,
        final ArrowColumnStoreFactory storeFactory) {
        try (var store = storeFactory.createStore(EMPTY_SCHEMA, new PathBackedFileHandle(path));
                var writeTable = new ColumnarRowWriteTable(EMPTY_SCHEMA, storeFactory, EMPTY_TABLE_SETTINGS)) {
            var table = writeTable.finish();
            return UnsavedColumnarContainerTable.create(tableId, table, writeTable.getStoreFlusher());
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to create empty table.", ex);
        }
    }

    /**
     * Create a {@link ColumnarValueSchema} representing the data coming from the {@link DefaultPythonArrowDataSink}.
     * Must only be called after the sink has received the first.
     *
     * @param dataSink The Python data sink that has already received the first batch
     * @param domainAndMetadata the domain and metadata for the table
     * @param dataRepository the {@link IDataRepository} to use for this table
     * @return The {@link ColumnarValueSchema} of the data coming from the {@link PythonDataSink}
     */
    public static ColumnarValueSchema createColumnarValueSchema(final DefaultPythonArrowDataSink dataSink,
        final TableDomainAndMetadata domainAndMetadata, final IDataRepository dataRepository) {
        final var schema = ArrowReaderWriterUtils.readSchema(dataSink.getPath().toFile());
        final var columnarSchema = ArrowSchemaUtils.convertSchema(schema);
        final var columnNames = ArrowSchemaUtils.extractColumnNames(schema);

        return createColumnarValueSchema(columnarSchema, columnNames, dataRepository, domainAndMetadata);
    }

    /**
     * Create a {@link ColumnarValueSchema} from a {@link ColumnarSchema}.
     *
     * @param columnNames The names of the columns in the table
     * @param dataRepository the {@link IDataRepository} to use for this table
     * @param domainAndMetadata the domain and metadata for the table. Can be null, then no domain and metadata info is
     *            set for the columns.
     * @param columnarSchema The schema of the table for which to create a {@link ColumnarValueSchema}.
     *
     * @return The {@link ColumnarValueSchema} of the data coming from the {@link PythonDataSink}
     */
    public static ColumnarValueSchema createColumnarValueSchema(final ColumnarSchema columnarSchema,
        final String[] columnNames, final IDataRepository dataRepository,
        final TableDomainAndMetadata domainAndMetadata) {
        final List<ValueFactory<?, ?>> factories = new ArrayList<>(columnarSchema.numColumns());
        final List<DataColumnSpec> specs = new ArrayList<>(columnarSchema.numColumns() - 1);

        // Add factory for the row keys
        factories.add(extractRowKeyValueFactory(columnarSchema));

        // Loop and get factory and spec for each column
        for (int i = 1; i < columnarSchema.numColumns(); i++) {//NOSONAR

            // Get the value factory for this column
            final var valueFactory =
                getValueFactory(columnarSchema.getTraits(i), columnarSchema.getSpec(i), dataRepository);
            factories.add(valueFactory);

            // Get the DataColumnSpec for this column
            final var dataType = ValueFactoryUtils.getDataTypeForValueFactory(valueFactory);
            final var specCreator = new DataColumnSpecCreator(columnNames[i], dataType);
            if (domainAndMetadata != null) {
                specCreator.setDomain(domainAndMetadata.getDomain(i));
                for (final DataColumnMetaData m : domainAndMetadata.getMetadata(i)) {
                    specCreator.addMetaData(m, false); // TODO overwrite or merge?
                }
            }
            specs.add(specCreator.createSpec());
        }
        var tableSpec = new DataTableSpec(specs.toArray(DataColumnSpec[]::new));
        return ColumnarValueSchemaUtils
            .create(ValueSchemaUtils.create(tableSpec, factories.toArray(ValueFactory<?, ?>[]::new)));
    }

    /**
     * Create a {@link DataTableSpec} from a {@link ColumnarSchema}. No metadata or domains will be attached to the returned {@link DataTableSpec}.
     *
     * @param columnNames The names of the columns in the table
     * @param columnarSchema The schema of the table for which to create a {@link ColumnarValueSchema}.
     *
     * @return The {@link DataTableSpec}
     */
    public static DataTableSpec createDataTableSpec(final ColumnarSchema columnarSchema,
        final String[] columnNames) {
        final List<DataColumnSpec> specs = new ArrayList<>(columnarSchema.numColumns() - 1);

        // Loop and get factory and spec for each column
        for (int i = 1; i < columnarSchema.numColumns(); i++) {//NOSONAR

            // Get the value factory for this column
            final var dataType = ValueFactoryUtils.createDataTypeForTraits(columnarSchema.getTraits(i));

            final var specCreator = new DataColumnSpecCreator(columnNames[i], dataType);
            specs.add(specCreator.createSpec());
        }
        return new DataTableSpec(specs.toArray(DataColumnSpec[]::new));
    }

    private static RowKeyValueFactory<?, ?> extractRowKeyValueFactory(final ColumnarSchema schema) {
        final var rowKeyTraits = schema.getTraits(0);
        if (rowKeyTraits.hasTrait(LogicalTypeTrait.class)) {
            return ValueFactoryUtils.loadRowKeyValueFactory(rowKeyTraits);
        } else {
            return DefaultRowKeyValueFactory.INSTANCE;
        }
    }

    private static ValueFactory<?, ?> getValueFactory(final DataTraits traits, final DataSpec dataSpec,
        final IDataRepository dataRepo) {
        if (traits.hasTrait(LogicalTypeTrait.class)) {
            return ValueFactoryUtils.loadValueFactory(traits, dataRepo);
        } else {
            return DefaultValueFactories.getDefaultValueFactory(dataSpec)//
                .orElseThrow(
                    () -> new IllegalArgumentException(String.format("There was no value factory provided from python "
                        + "and there also doesn't exist a default value factory for '%s'.", dataSpec)));
        }
    }

    /** Interface for an object holding a domain and an array of metadata for each column of a table. */
    public interface TableDomainAndMetadata {

        /**
         * @param colIndex the index of the column
         * @return the domain at this column
         */
        DataColumnDomain getDomain(int colIndex);

        /**
         * @param colIndex the index of the column
         * @return the metadata at this column
         */
        DataColumnMetaData[] getMetadata(int colIndex);

        /**
         * @return {@link TableDomainAndMetadata} where each column has {@code null} domain and empty metadata.
         */
        public static TableDomainAndMetadata empty() {
            return new TableDomainAndMetadata() {
                @Override
                public DataColumnDomain getDomain(final int colIndex) {
                    return null;
                }

                @Override
                public DataColumnMetaData[] getMetadata(final int colIndex) {
                    return new DataColumnMetaData[0];
                }
            };
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