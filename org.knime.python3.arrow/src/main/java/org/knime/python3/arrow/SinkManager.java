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
 *   Mar 23, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.arrow;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.data.IDataRepository;
import org.knime.core.data.container.DataContainer;
import org.knime.core.data.container.DataContainerSettings;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.Node;
import org.knime.core.node.NodeLogger;
import org.knime.python3.arrow.PythonArrowDataUtils.TableDomainAndMetadata;

/**
 * A class for managing a set of sinks with rowKeyCheckers and domainCalculators
 *
 * @noreference Non-public API - may change at any point in time
 * @noinstantiate Non-public API - may change at any point in time
 */
public final class SinkManager implements AutoCloseable {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SinkManager.class);

    private final Set<DefaultPythonArrowDataSink> m_sinks = new HashSet<>();

    private final Set<DefaultPythonArrowDataSink> m_usedSinks = new HashSet<>();

    private final Map<DefaultPythonArrowDataSink, RowKeyChecker> m_rowKeyCheckers = new HashMap<>();

    private final Map<DefaultPythonArrowDataSink, DomainCalculator> m_domainCalculators = new HashMap<>();

    private final Supplier<IDataRepository> m_dataRepositorySupplier;

    private final ArrowColumnStoreFactory m_arrowStoreFactory;

    /**
     * Constructor.
     *
     * @param dataRepositorySupplier that provides access to the DataRepository of the current node/workflow
     * @param arrowStoreFactory used to read from arrow files
     */
    public SinkManager(final Supplier<IDataRepository> dataRepositorySupplier,
        final ArrowColumnStoreFactory arrowStoreFactory) {
        m_dataRepositorySupplier = dataRepositorySupplier;
        m_arrowStoreFactory = arrowStoreFactory;
    }

    /**
     * Converts the provided sink into {@link BufferedDataTable}.
     * Waits for
     *
     * @param pythonSink the sink to convert into a table (must be created from this SinkManager)
     * @param exec of the current node execution
     * @return {@link BufferedDataTable} with the contents of pythonSink
     * @throws InterruptedException
     * @throws IOException
     */
    public BufferedDataTable convertToTable(final PythonArrowDataSink pythonSink, final ExecutionContext exec)
        throws InterruptedException, IOException {
        assert m_sinks
            .contains(pythonSink) : "Sink was not created by Python3KernelBackend#createSink. This is a coding issue.";
        // Must be a DefaultPythonArrowDataSink because it was created by #createSink
        final DefaultPythonArrowDataSink sink = (DefaultPythonArrowDataSink)pythonSink;

        checkRowKeys(sink);
        final var domainAndMetadata = getDomain(sink);
        final IDataRepository dataRepository = Node.invokeGetDataRepository(exec);
        @SuppressWarnings("resource") // Closed by the framework when the table is not needed anymore
        final BufferedDataTable table =
            PythonArrowDataUtils.createTable(sink, domainAndMetadata, m_arrowStoreFactory, dataRepository).create(exec);

        m_usedSinks.add(sink);
        return table;
    }

    @SuppressWarnings("resource") // All rowKeyCheckers are closed at #close
    private void checkRowKeys(final DefaultPythonArrowDataSink sink) throws InterruptedException, IOException {
        final var rowKeyChecker = m_rowKeyCheckers.get(sink);
        if (!rowKeyChecker.allUnique()) {
            throw new IOException("Row key checking: " + rowKeyChecker.getInvalidCause());
        }
    }

    @SuppressWarnings("resource") // All domainCalculators are closed at #close
    private TableDomainAndMetadata getDomain(final DefaultPythonArrowDataSink sink) throws InterruptedException {
        final var domainCalc = m_domainCalculators.get(sink);
        return domainCalc.getTableDomainAndMetadata();
    }

    /**
     * @return PythonArrowDataSink to be filled by Python
     * @throws IOException if no temporary file for the sink can be created
     */
    @SuppressWarnings("resource") // The resources are remembered and closed in #close
    public synchronized PythonArrowDataSink create_sink() throws IOException {//NOSONAR used by Python
        final var path = DataContainer.createTempFile(".knable").toPath();
        final var sink = PythonArrowDataUtils.createSink(path);

        // Check row keys and compute the domain as soon as anything is written to the sink
        final var rowKeyChecker = PythonArrowDataUtils.createRowKeyChecker(sink, m_arrowStoreFactory);
        final var domainCalculator = PythonArrowDataUtils.createDomainCalculator(sink, m_arrowStoreFactory,
            DataContainerSettings.getDefault().getMaxDomainValues(), m_dataRepositorySupplier.get());

        // Remember the sink, rowKeyChecker and domainCalc for cleaning up later
        m_sinks.add(sink);
        m_rowKeyCheckers.put(sink, rowKeyChecker);
        m_domainCalculators.put(sink, domainCalculator);

        return sink;
    }

    @Override
    public void close() {
        closeSafely(m_rowKeyCheckers.values());
        closeSafely(m_domainCalculators.values());
        deleteUnusedSinkFiles();
    }

    private static void closeSafely(final Iterable<? extends AutoCloseable> closeables) {
        for (var closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception ex) {
                LOGGER.debug(ex);
            }
        }
    }

    /** Deletes the temporary files of all sinks that have not been used */
    private void deleteUnusedSinkFiles() {
        m_sinks.removeAll(m_usedSinks);
        for (var sink : m_sinks) {
            try {
                Files.deleteIfExists(Path.of(sink.getAbsolutePath()));
            } catch (IOException ex) {
                LOGGER.debug("Failed to delete unused sink.", ex);
            }
        }
    }
}