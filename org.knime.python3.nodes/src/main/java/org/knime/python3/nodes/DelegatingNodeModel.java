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
 *   Jan 20, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;
import java.util.function.Supplier;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.util.asynclose.AsynchronousCloseableTracker;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.python3.nodes.proxy.CloseableNodeModelProxy;

/**
 * NodeModel that delegates its operations to a proxy implemented in Python.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
// TODO Perhaps move to the extension package?
public final class DelegatingNodeModel extends NodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DelegatingNodeModel.class);

    private final Supplier<CloseableNodeModelProxy> m_proxySupplier;

    private JsonNodeSettings m_settings;

    private Function<NodeSettingsRO, JsonNodeSettings> m_settingsFactory;

    private final AsynchronousCloseableTracker<RuntimeException> m_proxyShutdownTracker =
        new AsynchronousCloseableTracker<>(t -> LOGGER.debug("Exception during proxy shutdown.", t));

    /**
     * Constructor.
     *
     * @param pythonNodeSupplier supplier for proxies
     * @param initialSettings of the node
     * @param settingsFactory for creating JsonNodeSettings from NodeSettingsRO
     */
    // TODO retrieve the expected in and outputs from Python
    public DelegatingNodeModel(final Supplier<CloseableNodeModelProxy> pythonNodeSupplier,
        final JsonNodeSettings initialSettings, final Function<NodeSettingsRO, JsonNodeSettings> settingsFactory) {
        super(getInputPorts(pythonNodeSupplier), getOutputPorts(pythonNodeSupplier));
        m_proxySupplier = pythonNodeSupplier;
        m_settings = initialSettings;
        m_settingsFactory = settingsFactory;
    }

    private static PortType[] getInputPorts(final Supplier<CloseableNodeModelProxy> pythonNodeSupplier) {
        try (var node = pythonNodeSupplier.get()) {
            return node.getInputPortTypes();
        }
    }

    private static PortType[] getOutputPorts(final Supplier<CloseableNodeModelProxy> pythonNodeSupplier) {
        try (var node = pythonNodeSupplier.get()) {
            return node.getOutputPortTypes();
        }
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        try (var node = m_proxySupplier.get()) {
            node.loadValidatedSettings(m_settings);
            var result = node.configure(inSpecs);
            // allows for auto-configure
            m_settings = node.saveSettings();
            m_proxyShutdownTracker.closeAsynchronously(node);
            return result;
        }
    }

    @Override
    protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec)
        throws Exception {
        try (var node = m_proxySupplier.get()) {
            node.loadValidatedSettings(m_settings);
            var result = node.execute(inData, exec);
            m_settings = node.saveSettings();
            m_proxyShutdownTracker.closeAsynchronously(node);
            return result;
        }
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveTo(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        var jsonSettings = m_settingsFactory.apply(settings);
        try (var node = m_proxySupplier.get()) {
            node.validateSettings(jsonSettings);
            m_proxyShutdownTracker.waitForAllToClose();
        }
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings = m_settingsFactory.apply(settings);
        // the settings are not set on the proxy because the proxy is closed anyway
        // and any other operation will be performed on a new proxy where the settings are set anyway
    }

    @Override
    protected void onDispose() {
        m_proxyShutdownTracker.waitForAllToClose();
    }

    @Override
    protected void reset() {
        // nothing to reset
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // Nothing to load
    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // Nothing to save
    }
}
