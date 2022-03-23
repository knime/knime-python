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
package org.knime.python3.scripting;

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
import org.knime.core.node.NodeLogger;
import org.knime.python2.util.PythonUtils;
import org.knime.python3.arrow.DefaultPythonArrowDataSink;
import org.knime.python3.arrow.DomainCalculator;
import org.knime.python3.arrow.PythonArrowDataSink;
import org.knime.python3.arrow.PythonArrowDataUtils;
import org.knime.python3.arrow.RowKeyChecker;

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

    public boolean contains(final Object sink) {
        return m_sinks.contains(sink);
    }

    public RowKeyChecker getRowKeyChecker(final DefaultPythonArrowDataSink sink) {
        return m_rowKeyCheckers.get(sink);
    }

    public DomainCalculator getDomainCalculator(final DefaultPythonArrowDataSink sink) {
        return m_domainCalculators.get(sink);
    }

    public void markUsed(final DefaultPythonArrowDataSink sink) {
        m_usedSinks.add(sink);
    }

    @Override
    public void close() {
        PythonUtils.Misc.closeSafely(LOGGER::debug, m_rowKeyCheckers.values());
        PythonUtils.Misc.closeSafely(LOGGER::debug, m_domainCalculators.values());
        deleteUnusedSinkFiles();
    }

    /** Deletes the temporary files of all sinks that have not been used */
    private void deleteUnusedSinkFiles() {
        m_sinks.removeAll(m_usedSinks);
        PythonUtils.Misc.invokeSafely(LOGGER::debug, s -> {
            try {
                Files.deleteIfExists(Path.of(s.getAbsolutePath()));
            } catch (IOException ex) {
                throw new IllegalStateException(ex);
            }
        }, m_sinks);
    }
}