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
 *   May 3, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.arrow;

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;

/**
 * Handles the conversion of {@link BufferedDataTable BufferedDataTables} to {@link PythonArrowDataSource
 * PythonArrowDataSources} and from {@link PythonArrowDataSink PythonArrowDataSinks}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class PythonArrowTableConverter implements AutoCloseable {

    private final PythonArrowDataSourceFactory m_sourceFactory;

    private final SinkManager m_sinkManager;

    private final CancelableExecutor m_executor;

    /**
     * Constructor.
     *
     * @param executorService for asynchronous execution
     * @param storeFactory for creating arrow stores
     * @param fsHandler for file store handling
     */
    public PythonArrowTableConverter(final ExecutorService executorService, final ArrowColumnStoreFactory storeFactory,
        final IWriteFileStoreHandler fsHandler) {
        m_executor = new CancelableExecutor(executorService);
        m_sourceFactory = new PythonArrowDataSourceFactory(fsHandler, storeFactory);
        m_sinkManager = new SinkManager(fsHandler::getDataRepository, storeFactory);
    }

    /**
     * Creates a single source from a {@link BufferedDataTable}. If the table is backed by a single Arrow store, this
     * store is unpacked and used directly. Otherwise the data in table is written into a new Arrow store.
     *
     * If you have multiple sources, consider using {@link #createSources(BufferedDataTable[], ExecutionMonitor)}
     * instead.
     *
     * @param table containing the data
     * @return a source containing the data in table
     * @throws IOException if writing the table into an Arrow store fails
     */
    public PythonArrowDataSource createSource(final BufferedDataTable table) throws IOException {
        return m_sourceFactory.createSource(table);
    }

    /**
     * Creates sources from the provided tables. If a table is backed by a single Arrow store, this store is unpacked
     * and used directly. Otherwise the data in the table is written into a new Arrow store.
     *
     * @param tables to create source from
     * @param exec for progress reporting and cancellation
     * @return an array of sources
     * @throws IOException if writing the tables into Arrow store fails
     * @throws CanceledExecutionException if the execution is cancelled
     */
    public PythonArrowDataSource[] createSources(final BufferedDataTable[] tables, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        try {
            return m_executor.performCancelable(Stream.of(tables)//
                .map(this::createSourceTask)//
                .collect(toList()), //
                exec::checkCanceled)//
                .toArray(PythonArrowDataSource[]::new);
        } catch (ExecutionException ex) {// NOSONAR just holds another exception
            throw new IOException(ex.getCause());
        }
    }

    private Callable<PythonArrowDataSource> createSourceTask(final BufferedDataTable table) {
        return () -> m_sourceFactory.createSource(table);
    }

    /**
     * @return a sink for the use in Python
     * @throws IOException if the temporary file for the sink can't be created
     */
    public PythonArrowDataSink createSink() throws IOException {
        return m_sinkManager.create_sink();
    }

    /**
     * Turns the sink into a {@link BufferedDataTable}.
     *
     * @param sink containing data from Python
     * @param exec for table creation
     * @return a {@link BufferedDataTable} wrapping the sink
     * @throws InterruptedException if the process is interrupted
     * @throws IOException if the table contains duplicate row keys TODO special exception?
     */
    public BufferedDataTable convertToTable(final PythonArrowDataSink sink, final ExecutionContext exec)
        throws InterruptedException, IOException {
        return m_sinkManager.convertToTable(sink, exec);
    }

    /**
     * Turns the sinks into {@link BufferedDataTable BufferedDataTables}.
     *
     * @param sinks containing data from Python
     * @param exec for table creation as well as progress reporting and cancellation
     * @return an array of tables wrapping the sinks
     * @throws IOException if any of the tables contains duplicate row keys TODO special exception?
     * @throws CanceledExecutionException if the execution is cancelled by the user
     */
    public BufferedDataTable[] convertToTables(final List<PythonArrowDataSink> sinks, final ExecutionContext exec)
        throws IOException, CanceledExecutionException {
        var tables = new BufferedDataTable[sinks.size()];
        int i = 0;//NOSONAR
        for (var s : sinks) {
            try {
                tables[i] =
                    m_executor.performCancelable(() -> m_sinkManager.convertToTable(s, exec), exec::checkCanceled);
            } catch (ExecutionException ex) {// NOSONAR the ExecutionException acts as a holder for another exception
                var cause = ex.getCause();
                if (cause instanceof IOException ioCause) {
                    throw ioCause;
                } else {
                    throw new IOException(cause);
                }
            }
            i++;
        }
        return tables;
    }

    @Override
    public void close() {
        m_sourceFactory.close();
        m_sinkManager.close();
    }
}
