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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang3.ArrayUtils;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NodeView;
import org.knime.core.node.interactive.ReExecutable;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.ICredentials;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.node.workflow.VariableType;
import org.knime.core.node.workflow.VariableTypeRegistry;
import org.knime.core.node.workflow.virtual.AbstractPortObjectRepositoryNodeModel;
import org.knime.core.util.asynclose.AsynchronousCloseableTracker;
import org.knime.python3.nodes.proxy.NodeProxy;
import org.knime.python3.nodes.proxy.NodeProxyProvider;
import org.knime.python3.nodes.proxy.NodeViewProxy;
import org.knime.python3.nodes.proxy.NodeViewProxy.DataServiceProxy;
import org.knime.python3.nodes.proxy.PythonNodeModelProxy.PythonExecutionContext;
import org.knime.python3.nodes.proxy.PythonNodeViewProxy.PythonViewContext;
import org.knime.python3.nodes.proxy.model.NodeModelProxy;
import org.knime.python3.nodes.proxy.model.NodeModelProxy.CredentialsProviderProxy;
import org.knime.python3.nodes.proxy.model.NodeModelProxy.FlowVariablesProxy;
import org.knime.python3.nodes.proxy.model.NodeModelProxy.PortMapProvider;
import org.knime.python3.nodes.proxy.model.NodeModelProxy.WarningConsumer;
import org.knime.python3.nodes.proxy.model.NodeModelProxy.WorkflowPropertiesProxy;
import org.knime.python3.nodes.settings.JsonNodeSettings;
import org.knime.python3.nodes.settings.JsonNodeSettingsSchema;
import org.knime.python3.utils.FlowVariableUtils;
import org.knime.python3.views.PythonNodeViewStoragePath;

/**
 * NodeModel that delegates its operations to a proxy implemented in Python. Extends
 * {@link AbstractPortObjectRepositoryNodeModel} to enable the execution of WorkflowPortObjects.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class DelegatingNodeModel extends AbstractPortObjectRepositoryNodeModel
    implements CredentialsProviderProxy, WorkflowPropertiesProxy, FlowVariablesProxy, WarningConsumer, PortMapProvider,
    ReExecutable<String> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DelegatingNodeModel.class);

    private final NodeProxyProvider m_proxyProvider;

    private NodeViewProxy m_nodeViewProxy;

    private DataServiceProxy m_dataServiceProxy;

    /**
     * We're hiding the JsonNodeSettings behind a class for lazy initialization to prevent accidental wrong usage. The
     * settings are loaded lazily so that we can create an instance of this node (for caching in the NodeSpecCache)
     * without having to start a Python process -- which provides the initial settings.
     */
    private class LazyInitializedJsonNodeSettings {
        private JsonNodeSettings m_internalSettings;

        JsonNodeSettings get() {
            if (m_internalSettings == null) {
                try (var nodeProxy = m_proxyProvider.getConfigurationProxy()) {
                    m_internalSettings = nodeProxy.getSettings(m_extensionVersion);
                }
            }
            return m_internalSettings;
        }

        /**
         * If we are running configure or execute we start a Python process anyways, so we can use the node proxy from
         * that process to initialize the settings if needed.
         */
        JsonNodeSettings initializeFromProxy(final NodeProxy proxy) {
            if (m_internalSettings == null) {
                m_internalSettings = proxy.getSettings(m_extensionVersion);
            }
            return m_internalSettings;
        }

        void set(final JsonNodeSettings settings) {
            m_internalSettings = settings;
        }
    }

    private LazyInitializedJsonNodeSettings m_settings = new LazyInitializedJsonNodeSettings();

    private Optional<PythonNodeViewStoragePath> m_view;

    private String m_extensionVersion;

    private final PortType[] m_outputPorts;

    private final AsynchronousCloseableTracker<RuntimeException> m_proxyShutdownTracker =
        new AsynchronousCloseableTracker<>(t -> LOGGER.debug("Exception during proxy shutdown.", t));

    private Map<String, int[]> m_inputPortMap;

    private Map<String, int[]> m_outputPortMap;

    private PortObject[] m_internalInputPortObjects;

    private final boolean m_shouldHoldOutputs;

    private ViewData m_viewData;

    /**
     * Constructor with port maps
     *
     * @param proxyProvider provides the proxies for delegation
     * @param inputPorts The input ports of this node
     * @param outputPorts The output ports of this node
     * @param extensionVersion the version of the extension
     * @param inputPortMap Input Port Map for creating the node model
     * @param outputPortMap Output Port Map for creating the node model
     * @param shouldHoldOutputs indicates if the execution outputs should be saved as internal data to be used by a node
     *            view
     */
    public DelegatingNodeModel(final NodeProxyProvider proxyProvider, final PortType[] inputPorts,
        final PortType[] outputPorts, final String extensionVersion, final Map<String, int[]> inputPortMap,
        final Map<String, int[]> outputPortMap, final boolean shouldHoldOutputs) {
        super(inputPorts, outputPorts);
        m_proxyProvider = proxyProvider;
        m_view = Optional.empty();
        m_extensionVersion = extensionVersion;
        m_outputPorts = outputPorts;
        m_inputPortMap = inputPortMap;
        m_outputPortMap = outputPortMap;
        m_shouldHoldOutputs = shouldHoldOutputs;
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return runWithProxy(m_proxyProvider::getConfigurationProxy, node -> {
            node.loadValidatedSettings(m_settings.get());
            var result = node.configure(inSpecs, this, this, this, this);
            // allows for auto-configure
            m_settings.set(node.getSettings(m_extensionVersion));
            return result;
        });
    }

    @Override
    public void preReExecute(final String data, final boolean isNewDefault) {
        if (m_viewData != null) {
            m_viewData.setFrontendData(data);
            m_viewData.markToPersist();
        }
    }

    @Override
    protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        // Delete the old view file if it exists
        if (m_view.isPresent()) {
            m_view.get().deleteIfExists();
        }

        if (m_viewData != null && !m_viewData.isMarkedToPersist()) {
            m_viewData = null;
        }

        return runWithProxy(() -> m_proxyProvider.getExecutionProxy(inData), node -> {
            node.loadValidatedSettings(m_settings.get());
            var result = node.execute(inData, m_outputPorts, exec, this, this, this, this,
                m_viewData == null ? null : m_viewData.getBackendData());
            m_settings.set(node.getSettings(m_extensionVersion));
            m_view = result.getView();
            if (m_shouldHoldOutputs) {
                m_internalInputPortObjects = inData;
            }
            return result.getPortObjects();
        });
    }

    @Override
    public void setWarning(final String message) {
        setWarningMessage(message);
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.get().saveTo(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        runWithProxyConsumer(m_proxyProvider::getConfigurationProxy, node -> {
            var savedVersion = JsonNodeSettingsSchema.readVersion(settings);
            var jsonSettings = node.getSettingsSchema(savedVersion).createFromSettings(settings);
            node.validateSettings(jsonSettings);
        });
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        runWithProxyConsumer(m_proxyProvider::getConfigurationProxy, node -> {
            var savedVersion = JsonNodeSettingsSchema.readVersion(settings);
            m_settings.set(node.getSettingsSchema(savedVersion).createFromSettings(settings));

            // if the extension version the settings were saved with is different from
            // the installed extension version, we need to let the user know
            if (!savedVersion.equals(m_extensionVersion)) {
                node.determineCompatibility(savedVersion, m_extensionVersion, m_settings.get().getParameters());
            }
        });
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
            m_settings.initializeFromProxy(proxy);
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
        // The nodemodel exists as long as the node is in the WF, but the gateway is re-created on each execution.
        // So on dispose or reset we need to release the last-used gateway from the ProxyProvider.
        m_proxyProvider.cleanup();

        m_proxyShutdownTracker.waitForAllToClose();
    }

    @Override
    protected void reset() {
        if (m_dataServiceProxy != null) {
            m_viewData = new ViewData();
            m_viewData.setBackendData(m_dataServiceProxy.getViewData());
        } else {
            m_viewData = null;
        }
        m_proxyProvider.cleanup();
        m_view = Optional.empty();
        m_proxyShutdownTracker.waitForAllToClose();
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        m_view = PythonNodeViewStoragePath.loadFromInternals(nodeInternDir.toPath());
        m_viewData = ViewData.load(nodeInternDir);
    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        if (m_view.isPresent()) {
            // Copy the view from the temporary file to the persisted internals directory
            m_view.get().saveToInternals(nodeInternDir.toPath());
        }
        if (m_viewData != null) {
            m_viewData.save(nodeInternDir);
        }
    }

    /**
     * @return the path to the HTML document for the {@link NodeView}. {@link Optional#empty()} if the node did not
     *         return a view.
     */
    public Optional<Path> getPathToHtmlView() {
        return m_view.map(PythonNodeViewStoragePath::getPath);
    }

    /**
     * @return {@code true} if the node has a view that can be used in a report, {@code false} otherwise.
     */
    public boolean canViewBeUsedInReport() {
        return m_view.isPresent() && m_view.get().canBeUsedInReport();
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

    @Override
    public String getLocalWorkflowPath() {
        return NodeContext.getContext().getWorkflowManager().getContextV2().getExecutorInfo().getLocalWorkflowPath()
            .toFile().getAbsolutePath();
    }

    @Override
    public String[] getCredentialNames() {
        var credentialsProvider = getCredentialsProvider();
        return credentialsProvider.listNames().toArray(String[]::new);
    }

    @Override
    public ICredentials getCredentials(final String identifier) {
        var credentialsProvider = getCredentialsProvider();
        return credentialsProvider.get(identifier);

    }

    @Override
    public String getNodeNameWithID() {
        return NodeContext.getContext().getNodeContainer().getNameWithID();
    }

    /**
     * @return the inputPortMap
     */
    @Override
    public Map<String, int[]> getInputPortMap() {
        return m_inputPortMap;
    }

    /**
     * @return the outputPortMap
     */
    @Override
    public Map<String, int[]> getOutputPortMap() {
        return m_outputPortMap;
    }

    @Override
    public void setInternalPortObjects(final PortObject[] portObjects) {
        if (m_viewData == null) {
            m_internalInputPortObjects = portObjects;
        } else {
            var viewDataPortObjects = m_viewData.getBackendData().ports;
            m_internalInputPortObjects = new PortObject[getNrInPorts()];
            assert m_internalInputPortObjects.length + viewDataPortObjects.length == portObjects.length;
            System.arraycopy(portObjects, 0, m_internalInputPortObjects, 0, m_internalInputPortObjects.length);
            System.arraycopy(portObjects, m_internalInputPortObjects.length, viewDataPortObjects, 0,
                viewDataPortObjects.length);
        }
    }

    @Override
    public PortObject[] getInternalPortObjects() {
        if (m_viewData == null) {
            return m_internalInputPortObjects;
        } else {
            return ArrayUtils.addAll(m_internalInputPortObjects, m_viewData.getBackendData().ports);
        }
    }

    /**
     * Returns the frontend view data that is updated via {@link #preReExecute(String, boolean)} which is being called
     * when the node is re-executed (~widget re-execution) with new view data. The view data is persisted as 'internal
     * data' view data is persisted as node 'internals'.
     *
     * @return the frontend view data
     */
    public String getFrontendViewData() {
        return m_viewData == null ? null : m_viewData.getFrontendData();
    }

    /**
     * @return the data service proxy instance
     */
    public DataServiceProxy getDataServiceProxy() {
        if (m_nodeViewProxy == null) {
            m_nodeViewProxy = m_proxyProvider.getNodeViewProxy();
            m_dataServiceProxy = m_nodeViewProxy.getDataServiceProxy(m_settings.get(), m_internalInputPortObjects,
                m_viewData == null ? null : m_viewData.getBackendData(), this, this);
        }
        return m_dataServiceProxy;
    }

    /**
     * Disposes the data service proxy and associated resources.
     *
     * @throws Exception
     */
    public void disposeDataServiceProxy() throws Exception {
        if (m_nodeViewProxy != null) {
            m_nodeViewProxy.close();
            m_nodeViewProxy = null;
        }
        if (m_dataServiceProxy != null) {
            m_dataServiceProxy.close();
            m_dataServiceProxy = null;
        }
    }

    /**
     * Encapsulates the view data that is persisted with the node internals. The view data consists of two parts:
     * <ul>
     * <li>the frontend data: received from the frontend via {@link DelegatingNodeModel#preReExecute(String, boolean)}
     * on node re-execution; served back to the frontend via the ui-extension's 'initial data'</li>
     * <li>the backend data: fetched from python on {@link DelegatingNodeModel#reset()} via
     * {@link DataServiceProxy#getViewData()}; served back to python via {@link PythonViewContext#get_view_data()} (for
     * the view's data service) and via {@link PythonExecutionContext#get_view_data()} (for the node re-execution)</li>
     */
    public static class ViewData {

        private BackendViewData m_backendData;

        private String m_frontendData;

        private boolean m_doPersist = false;

        private void setFrontendData(final String frontendData) {
            m_frontendData = frontendData;
        }

        private ViewData() {
            //
        }

        private ViewData(final String frontendData, final BackendViewData pythonData) {
            setFrontendData(frontendData);
            setBackendData(pythonData);
            m_doPersist = true;
        }

        /**
         * @return the view data to be provided to the frontend
         */
        private String getFrontendData() {
            return m_frontendData;
        }

        private void setBackendData(final BackendViewData backendData) {
            m_backendData = backendData;
        }

        /**
         * @return the view data to be provided to the backend (python)
         */
        BackendViewData getBackendData() {
            return m_backendData;
        }

        private void markToPersist() {
            m_doPersist = true;
        }

        private boolean isMarkedToPersist() {
            return m_doPersist;
        }

        private void save(final File nodeInternDir) throws IOException {
            if (m_doPersist) {
                try (DataOutputStream out = new DataOutputStream(new GZIPOutputStream(new BufferedOutputStream(
                    new FileOutputStream(new File(nodeInternDir, "internal_view_data.gz")))))) {
                    out.writeUTF(m_frontendData);
                    out.writeUTF(m_backendData.data());
                    out.writeInt(m_backendData.ports.length);
                }
            }
        }

        private static ViewData load(final File nodeInternDir) throws FileNotFoundException, IOException {
            File f = new File(nodeInternDir, "internal_view_data.gz");
            if (f.exists()) {
                try (DataInputStream in =
                    new DataInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(f))))) {
                    return new ViewData(in.readUTF(), new BackendViewData(in.readUTF(), new PortObject[in.readInt()]));
                }
            }
            return null;
        }

        /**
         * The backend view data fetch from and provided to python.
         *
         * @param data any data that can be serialized as a string
         * @param ports the port objects to be persisted with the view data
         */
        public record BackendViewData(String data, PortObject[] ports) {
        }

    }

}
