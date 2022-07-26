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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NodeView;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.VariableType;
import org.knime.core.node.workflow.VariableTypeRegistry;
import org.knime.core.util.PathUtils;
import org.knime.core.util.asynclose.AsynchronousCloseableTracker;
import org.knime.python3.nodes.proxy.model.NodeModelProxy;
import org.knime.python3.nodes.proxy.model.NodeModelProxy.FlowVariablesProxy;
import org.knime.python3.nodes.proxy.model.NodeModelProxy.WarningConsumer;
import org.knime.python3.nodes.proxy.model.NodeModelProxyProvider;
import org.knime.python3.nodes.settings.JsonNodeSettings;
import org.knime.python3.nodes.settings.JsonNodeSettingsSchema;
import org.knime.python3.utils.FlowVariableUtils;

/**
 * NodeModel that delegates its operations to a proxy implemented in Python.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class DelegatingNodeModel extends NodeModel implements FlowVariablesProxy, WarningConsumer {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DelegatingNodeModel.class);

    private final NodeModelProxyProvider m_proxyProvider;

    private JsonNodeSettings m_settings;

    private Optional<Path> m_view;

    private String m_extensionVersion;

    private final AsynchronousCloseableTracker<RuntimeException> m_proxyShutdownTracker =
        new AsynchronousCloseableTracker<>(t -> LOGGER.debug("Exception during proxy shutdown.", t));

    /**
     * Constructor.
     *
     * @param proxyProvider provides the proxies for delegation
     * @param inputPorts The input ports of this node
     * @param outputPorts The output ports of this node
     * @param initialSettings of the node
     * @param extensionVersion the version of the extension
     */
    public DelegatingNodeModel(final NodeModelProxyProvider proxyProvider, final PortType[] inputPorts,
        final PortType[] outputPorts,
        final JsonNodeSettings initialSettings,
        final String extensionVersion) {
        super(inputPorts, outputPorts);
        m_proxyProvider = proxyProvider;
        m_settings = initialSettings;
        m_view = Optional.empty();
        m_extensionVersion = extensionVersion;
    }


    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return runWithProxy(m_proxyProvider::getConfigurationProxy, node -> {
            node.loadValidatedSettings(m_settings);
            var result = node.configure(inSpecs, this, this);
            // allows for auto-configure
            m_settings = node.getSettings(m_extensionVersion);
            return result;
        });
    }

    @Override
    protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        // Delete the old view file if it exists
        if (m_view.isPresent()) {
            PathUtils.deleteFileIfExists(m_view.get());
        }
        return runWithProxy(m_proxyProvider::getExecutionProxy, node -> {
            node.loadValidatedSettings(m_settings);
            var result = node.execute(inData, exec, this, this);
            m_settings = node.getSettings(m_extensionVersion);
            m_view = result.getView();
            return result.getPortObjects();
        });
    }

    @Override
    public void setWarning(final String message) {
        setWarningMessage(message);
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveTo(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        runWithProxyConsumer(m_proxyProvider::getConfigurationProxy,
            node -> node.validateSettings(node.getSettings(m_extensionVersion).createFromSettings(settings)));
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        runWithProxy(m_proxyProvider::getConfigurationProxy,
//            node -> m_settings = node.getSettings(m_extensionVersion).createFromSettings(settings));
            node -> m_settings = node.getSettingsSchema(JsonNodeSettingsSchema.readVersion(settings)).createFromSettings(settings));
    }

    private interface ThrowingFunction<S, T, X extends Exception> {
        T apply(S object) throws X;
    }

    private interface ThrowingConsumer<S, X extends Exception> extends ThrowingFunction<S, Void, X> {
        void accept(final S object) throws X;

        @Override
        default Void apply(final S object) throws X {
            accept(object);
            return null;
        }
    }

    private <P extends NodeModelProxy, X extends Exception, T> T runWithProxy(final Supplier<P> proxySupplier,
        final ThrowingFunction<P, T, X> function) throws X {
        try (var proxy = proxySupplier.get()) {
            var result = function.apply(proxy);
            m_proxyShutdownTracker.closeAsynchronously(proxy);
            return result;
        }
    }

    private <P extends NodeModelProxy, X extends Exception> void runWithProxyConsumer(final Supplier<P> proxySupplier,
        final ThrowingConsumer<P, X> consumer) throws X {
        runWithProxy(proxySupplier, consumer);
    }

    @Override
    protected void onDispose() {
        m_proxyShutdownTracker.waitForAllToClose();
    }

    @Override
    protected void reset() {
        m_view = Optional.empty();
        m_proxyShutdownTracker.waitForAllToClose();
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        final var viewPath = persistedViewPath(nodeInternDir);
        if (Files.isReadable(viewPath)) {
            m_view = Optional.of(viewPath);
        }
    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        if (m_view.isPresent()) {
            // Copy the view from the temporary file to the persisted internals directory
            Files.copy(m_view.get(), persistedViewPath(nodeInternDir));
        }
    }

    /** Path to the persisted view inside the internals directory */
    private static Path persistedViewPath(final File nodeInternDir) {
        return nodeInternDir.toPath().resolve("view.html");
    }

    /**
     * @return the path to the HTML document for the {@link NodeView}. {@link Optional#empty()} if the node did not
     *         return a view.
     */
    public Optional<Path> getPathToHtmlView() {
        return m_view;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void pushNewFlowVariable(final FlowVariable variable) {
        pushFlowVariable(variable.getName(), (VariableType)variable.getVariableType(),
            variable.getValue(variable.getVariableType()));
    }

    @Override
    public Map<String, Object> getFlowVariables() {
        return FlowVariableUtils.convertToMap(
            getAvailableFlowVariables(CloseablePythonNodeProxy.getCompatibleFlowVariableTypes()).values());
    }

    @Override
    public void setFlowVariables(final Map<String, Object> flowVariables) {
        final Map<String, FlowVariable> oldVariables =
            getAvailableFlowVariables(VariableTypeRegistry.getInstance().getAllTypes());

        final var newVariables = FlowVariableUtils.convertFromMap(flowVariables, LOGGER);
        for (final FlowVariable variable : newVariables) {
            if (!Objects.equals(oldVariables.get(variable.getName()), variable)) {
                pushNewFlowVariable(variable);
            }
        }
    }
}
