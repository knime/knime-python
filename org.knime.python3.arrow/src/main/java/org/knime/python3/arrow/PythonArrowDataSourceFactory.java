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
 *   May 2, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.arrow;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.knime.core.columnar.arrow.ArrowBatchReadStore;
import org.knime.core.columnar.arrow.ArrowBatchStore;
import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.data.columnar.table.ColumnarBatchReadStore;
import org.knime.core.data.columnar.table.ColumnarContainerTable;
import org.knime.core.data.columnar.table.ColumnarRowReadTable;
import org.knime.core.data.columnar.table.ColumnarRowWriteTable;
import org.knime.core.data.columnar.table.ColumnarRowWriteTableSettings;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.RowWriteCursor;
import org.knime.core.data.v2.schema.ValueSchemaUtils;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.BufferedDataTable.KnowsRowCountTable;
import org.knime.core.node.Node;
import org.knime.core.node.NodeLogger;

/**
 * Creates {@link PythonArrowDataSource PythonArrowDataSources} from {@link BufferedDataTable BufferedDataTables}. If
 * the input table is already backed by an arrow store, it is simply unwrapped, otherwise the table's content is copied
 * into a new store.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class PythonArrowDataSourceFactory implements Closeable {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonArrowDataSourceFactory.class);

    private final Set<ColumnarBatchReadStore> m_copiedStores = new HashSet<>(1);

    private final AtomicBoolean m_closed = new AtomicBoolean(false);

    private final IWriteFileStoreHandler m_fsHandler;

    private final ArrowColumnStoreFactory m_storeFactory;

    /**
     * Constructor.
     *
     * @param fsHandler for handling file stores
     * @param storeFactory to create new stores if a table is not backed by a store itself
     */
    public PythonArrowDataSourceFactory(final IWriteFileStoreHandler fsHandler,
        final ArrowColumnStoreFactory storeFactory) {
        m_fsHandler = fsHandler;
        m_storeFactory = storeFactory;
    }

    /**
     * Creates a source from the provided table. If the table contains an Arrow store, then this store is extracted.
     * Otherwise the content of the table is written into a new Arrow store.
     *
     * @param table to turn into a source
     * @return a source that contains the data in table
     * @throws IOException if copying the table into an Arrow store fails
     */
    // If store is extracted from table, then it is also closed by table
    // if it is newly created by copying table, then it will be closed by #close()
    @SuppressWarnings("resource")
    public PythonArrowDataSource createSource(final BufferedDataTable table) throws IOException {
        var store = extractStoreCopyTableIfNecessary(table);
        return convertStoreIntoSource(store, table.getDataTableSpec().getColumnNames());
    }

    @Override
    public void close() {
        if (m_closed.compareAndSet(false, true)) {
            cleanupCopiedStores();
        }
    }

    private void cleanupCopiedStores() {
        synchronized (m_copiedStores) {
            m_copiedStores.forEach(PythonArrowDataSourceFactory::cleanupStore);
        }
    }

    private static void cleanupStore(final ColumnarBatchReadStore store) {
        var path = store.getFileHandle();
        try {
            store.close();
        } catch (IOException ex) {
            LOGGER.debug("Failed to close store.", ex);
        }
        path.delete();
    }

    // Store will be closed along with table. If it is a copy, it will have already been closed.
    @SuppressWarnings("resource")
    private ColumnarBatchReadStore extractStoreCopyTableIfNecessary(final BufferedDataTable table) throws IOException {
        final KnowsRowCountTable delegate = Node.invokeGetDelegate(table);
        if (delegate instanceof ColumnarContainerTable) {
            var columnarTable = (ColumnarContainerTable)delegate;
            final var baseStore = columnarTable.getStore().getDelegateBatchReadStore();
            final boolean isLegacyArrow;
            if (baseStore instanceof ArrowBatchReadStore) {
                isLegacyArrow = ((ArrowBatchReadStore)baseStore).isUseLZ4BlockCompression()
                    || ValueSchemaUtils.storesDataCellSerializersSeparately(columnarTable.getSchema());
            } else if (baseStore instanceof ArrowBatchStore) {
                // Write stores shouldn't be using the old compression format or the old ValueSchema anymore
                isLegacyArrow = false;
            } else {
                // Not Arrow at all (= a new storage back end), treat like legacy, i.e. copy.
                isLegacyArrow = true;
            }
            if (!isLegacyArrow) {
                return ((ColumnarContainerTable)delegate).getStore();
            }
        }
        // Fallback for legacy and virtual tables.
        return copyTableToArrowStore(table);
    }

    @SuppressWarnings("resource") // the store is closed when the kernel is closed
    private ColumnarBatchReadStore copyTableToArrowStore(final BufferedDataTable table) throws IOException {
        synchronized (m_copiedStores) {
            if (m_closed.get()) {
                throw new IllegalStateException("Attempting to copy a table after the factory has been closed.");
            } else {
                var copiedTable = copyTable(table);
                final var store = copiedTable.getStore();
                m_copiedStores.add(store);
                return store;
            }
        }
    }

    private ColumnarRowReadTable copyTable(final BufferedDataTable table) throws IOException {
        final var schema = ValueSchemaUtils.create(table.getSpec(), RowKeyType.CUSTOM, m_fsHandler);
        try (final var columnarTable = new ColumnarRowWriteTable(schema, m_storeFactory,
            new ColumnarRowWriteTableSettings(true, false, -1, false, false, false, 100, 4))) {
            try (final RowCursor inCursor = table.cursor();
                    final RowWriteCursor outCursor = columnarTable.createCursor()) {
                while (inCursor.canForward()) {
                    outCursor.forward().setFrom(inCursor.forward());
                }
                return columnarTable.finish();
            }
        }
    }

    // Store will be closed along with table. If it is a copy, it will have already been closed.
    @SuppressWarnings("resource")
    private static PythonArrowDataSource convertStoreIntoSource(final ColumnarBatchReadStore columnarStore,
        final String[] columnNames) throws IOException {
        // Unwrap the underlying physical Arrow store from the table. Along the way, flush any cached table
        // content to disk to make it available to Python.
        //
        // TODO: ideally, we want to be able to flush per batch/up to some batch index. Once this is supported,
        // defer flushing until actually needed (i.e. when Python pulls data).
        if (columnarStore instanceof Flushable) {
            ((Flushable)columnarStore).flush();
        }
        final var baseStore = columnarStore.getDelegateBatchReadStore();
        if (baseStore instanceof ArrowBatchReadStore) {
            final ArrowBatchReadStore store = (ArrowBatchReadStore)baseStore;
            return PythonArrowDataUtils.createSource(store, columnNames);
        } else if (baseStore instanceof ArrowBatchStore) {
            final ArrowBatchStore store = (ArrowBatchStore)baseStore;
            return PythonArrowDataUtils.createSource(store, store.numBatches(), columnNames);
        } else {
            // Any non-Arrow store should already have been copied into an Arrow store further above.
            throw new IllegalStateException(
                "Unrecognized store type: " + baseStore.getClass().getName() + ". This is an implementation error.");
        }
    }

}
