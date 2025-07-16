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
 *   Jan 21, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.data.IDataRepository;
import org.knime.core.data.filestore.FileStore;
import org.knime.core.data.filestore.FileStoreFactory;
import org.knime.core.data.filestore.FileStoreKey;
import org.knime.core.data.filestore.FileStoreUtil;
import org.knime.core.data.filestore.internal.FileStoreProxy.FlushCallback;
import org.knime.core.data.filestore.internal.IFileStoreHandler;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.KNIMEConstants;
import org.knime.core.node.Node;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.ICredentials;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.node.workflow.VariableType;
import org.knime.core.util.FileUtil;
import org.knime.core.util.ThreadUtils;
import org.knime.core.util.asynclose.AsynchronousCloseable;
import org.knime.core.util.auth.CouldNotAuthorizeException;
import org.knime.core.webui.data.DataServiceException;
import org.knime.credentials.base.oauth.api.HttpAuthorizationHeaderCredentialValue;
import org.knime.python3.PythonFileStoreUtils;
import org.knime.python3.arrow.PythonArrowDataSink;
import org.knime.python3.arrow.PythonArrowDataUtils;
import org.knime.python3.arrow.PythonArrowTableConverter;
import org.knime.python3.nodes.CloseablePythonNodeProxyFactory.CloseableGatewayWithAttachments;
import org.knime.python3.nodes.callback.AuthCallbackUtils;
import org.knime.python3.nodes.extension.ExtensionNode;
import org.knime.python3.nodes.ports.PythonPortObjects.PurePythonTablePortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortTypeRegistry;
import org.knime.python3.nodes.ports.PythonTransientConnectionPortObject;
import org.knime.python3.nodes.ports.TableSpecSerializationUtils;
import org.knime.python3.nodes.ports.converters.PortObjectConversionContext;
import org.knime.python3.nodes.proxy.CloseableNodeFactoryProxy;
import org.knime.python3.nodes.proxy.NodeDialogProxy;
import org.knime.python3.nodes.proxy.NodeViewProxy;
import org.knime.python3.nodes.proxy.PythonNodeModelProxy;
import org.knime.python3.nodes.proxy.PythonNodeModelProxy.Callback;
import org.knime.python3.nodes.proxy.PythonNodeModelProxy.DialogCallback;
import org.knime.python3.nodes.proxy.PythonNodeModelProxy.ExpiryDate;
import org.knime.python3.nodes.proxy.PythonNodeModelProxy.FileStoreBasedFile;
import org.knime.python3.nodes.proxy.PythonNodeProxy;
import org.knime.python3.nodes.proxy.model.NodeConfigurationProxy;
import org.knime.python3.nodes.proxy.model.NodeExecutionProxy;
import org.knime.python3.nodes.settings.JsonNodeSettings;
import org.knime.python3.nodes.settings.JsonNodeSettingsSchema;
import org.knime.python3.utils.FlowVariableUtils;
import org.knime.python3.views.PythonNodeViewStoragePath;
import org.knime.python3.views.PythonNodeViewSink;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Manages the lifecycle of a Python based NodeProxy and its associated process. Invoking {@link Closeable#close()}
 * shuts down the Python process and the proxy is no longer usable.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class CloseablePythonNodeProxy
    implements NodeExecutionProxy, NodeConfigurationProxy, CloseableNodeFactoryProxy, NodeDialogProxy, NodeViewProxy {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(CloseablePythonNodeProxy.class);

    private IFileStoreHandler m_fileStoreHandler;

    private final PythonNodeProxy m_proxy;

    private final CloseableGatewayWithAttachments m_closeableGateway;

    private final AsynchronousCloseable<RuntimeException> m_closer =
        AsynchronousCloseable.createAsynchronousCloser(this::closeInternal);

    private final ExtensionNode m_nodeSpec;

    private static final ArrowColumnStoreFactory ARROW_STORE_FACTORY =
        PythonArrowDataUtils.getArrowColumnStoreFactory();

    private PythonArrowTableConverter m_tableManager;

    private final ExecutorService m_executorService = ThreadUtils.executorServiceWithContext(
        Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("python-node-%d").build()));

    private static final Set<Class<?>> KNOWN_FLOW_VARIABLE_TYPES = Set.of( //
        Boolean.class, //
        Boolean[].class, //
        Double.class, //
        Double[].class, //
        Integer.class, //
        Integer[].class, //
        Long.class, //
        Long[].class, //
        String.class, //
        String[].class //
    );

    /**
     * The {@link ConnectionType} defines whether the node uses a connection port object or not. This influences whether
     * the gateway should be reused (to maintain the connection) or is free to be discarded.
     */
    private enum ConnectionType {
            SOURCE, // If a node produces a connection, it must hold on to its gateway until the node gets reset
            CONNECTED, // If a node uses a connection, it should not close the gateway but also not hold on to it
            INDEPENDENT // The node does not use any type of connection. Gateway can be closed after execution.
    }

    CloseablePythonNodeProxy(final PythonNodeProxy proxy, final CloseableGatewayWithAttachments gateway,
        final ExtensionNode nodeSpec) {
        m_proxy = proxy;
        m_closeableGateway = gateway;
        m_nodeSpec = nodeSpec;
    }

    private void closeInternal() {
        try {
            m_closeableGateway.close();
            if (m_tableManager != null) {
                m_tableManager.close();
            }
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to shutdown Python gateway.", ex);
        }
    }

    @Override
    public void close() {
        m_closer.close();
        if (m_fileStoreHandler instanceof DelegatingAndNotInWorkflowWriteFileStoreHandler) {
            m_fileStoreHandler.clearAndDispose();
            m_fileStoreHandler = null;
        }
    }

    @Override
    public Future<Void> asynchronousClose() {
        return m_closer.asynchronousClose();
    }

    @Override
    public String getDialogRepresentation(final JsonNodeSettings settings, final PortObjectSpec[] specs,
        final String extensionVersion) {

        var failure = new FailureState();

        final PythonNodeModelProxy.DialogCallback dialogCallback = new DialogCallback() {

            private DefaultLogCallback m_logCallback = new DefaultLogCallback(LOGGER);

            @Override
            public void log(final String message, final String severity) {
                m_logCallback.log(message, severity);

            }

            @Override
            public Map<String, Object> get_flow_variables() {
                if (getNode().getFlowObjectStack() == null) {
                    return new HashMap<>();
                }

                return FlowVariableUtils.convertToMap(getNode().getFlowObjectStack()
                    .getAvailableFlowVariables(getCompatibleFlowVariableTypes()).values());
            }

            @SuppressWarnings("unused")
            public String get_auth_schema(final String serializedXMLString) throws CouldNotAuthorizeException, // NOSONAR
                ClassNotFoundException, InstantiationException, IllegalAccessException, IOException { // NOSONAR
                return getAuthSchema(serializedXMLString);

            }

            @SuppressWarnings("unused")
            public String get_auth_parameters(final String serializedXMLString) throws CouldNotAuthorizeException, // NOSONAR
                ClassNotFoundException, InstantiationException, IllegalAccessException, IOException { // NOSONAR
                return getAuthParameters(serializedXMLString);
            }

            @SuppressWarnings("unused")
            public ExpiryDate get_expires_after(final String serializedXMLString) throws CouldNotAuthorizeException, // NOSONAR
                ClassNotFoundException, InstantiationException, IllegalAccessException, IOException { // NOSONAR
                return getExpiresAfter(serializedXMLString);
            }

            @Override
            public void set_failure(final String message, final String details, final boolean invalidSettings) {
                failure.setFailure(message, details, invalidSettings);
            }

        };
        m_proxy.initializeJavaCallback(dialogCallback);

        final var pythonDialogContext = new PythonNodeModelProxy.PythonDialogCreationContext() {

            @Override
            public String[] get_credential_names() {
                return getNode().getCredentialsProvider().listNames().toArray(String[]::new);
            }

            @Override
            public String[] get_credentials(final String identifier) {
                CredentialsProvider credentialsProvider = getNode().getCredentialsProvider();
                ICredentials credentials = credentialsProvider.get(identifier);
                return new String[]{credentials.getLogin(), credentials.getPassword(), credentials.getName()};
            }

            @Override
            public PythonPortObjectSpec[] get_input_specs() {
                return Arrays.stream(specs).map(PythonPortTypeRegistry::convertPortObjectSpecToPython)
                    .toArray(PythonPortObjectSpec[]::new);
            }

            @Override
            public Map<String, int[]> get_input_port_map() {
                return ((DelegatingNodeModel)getNode().getNodeModel()).getInputPortMap();
            }

            @Override
            public Map<String, int[]> get_output_port_map() {
                return ((DelegatingNodeModel)getNode().getNodeModel()).getOutputPortMap();
            }
        };
        // extensionVersion must always be the version of the installed extension, since it is used
        // on the Python side to generate the schema and UI schema, which need to correspond to the
        // set of parameters available in the installed version of the extension.
        var dialogRepresentation =
            m_proxy.getDialogRepresentation(settings.getParameters(), extensionVersion, pythonDialogContext);
        try {
            failure.throwIfFailure();
        } catch (InvalidSettingsException ex) {
            // hides the stacktrace which isn't helpful to the user here.
            throw new DataServiceException(ex.getMessage(), ex);
        }
        return dialogRepresentation;
    }

    /**
     * @return returns node corresponding to the python context.
     **/
    public static Node getNode() {
        return ((NativeNodeContainer)NodeContext.getContext().getNodeContainer()).getNode();
    }

    public static String getAuthSchema(final String serializedXMLString) throws CouldNotAuthorizeException, // NOSONAR
        ClassNotFoundException, InstantiationException, IllegalAccessException, IOException { // NOSONAR
        return AuthCallbackUtils.getAuthSchema(serializedXMLString);
    }

    public static String getAuthParameters(final String serializedXMLString) throws CouldNotAuthorizeException, // NOSONAR
        ClassNotFoundException, InstantiationException, IllegalAccessException, IOException { // NOSONAR
        return AuthCallbackUtils.getAuthParameters(serializedXMLString);
    }

    public static ExpiryDate getExpiresAfter(final String serializedXMLString) throws CouldNotAuthorizeException, // NOSONAR
        ClassNotFoundException, InstantiationException, IllegalAccessException, IOException { // NOSONAR
        return AuthCallbackUtils.getExpiresAfter(serializedXMLString);
    }

    public static HttpAuthorizationHeaderCredentialValue getCredentialFromXMLString(final String serializedXMLString)
        throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
        return AuthCallbackUtils.getCredentialFromXMLString(serializedXMLString);
    }

    @Override
    public void validateSettings(final JsonNodeSettings settings) throws InvalidSettingsException {
        var error = m_proxy.validateParameters(settings.getParameters(), settings.getCreationVersion());
        CheckUtils.checkSetting(error == null, "%s", error);
    }

    @Override
    public void loadValidatedSettings(final JsonNodeSettings settings) {
        m_proxy.setParameters(settings.getParameters(), settings.getCreationVersion());
    }

    @Override
    public void determineCompatibility(final String savedVersion, final String extensionVersion,
        final String savedParams) {
        m_proxy.determineCompatibility(savedVersion, extensionVersion, savedParams);
    }

    private void initTableManager() {
        if (m_tableManager == null) {
            m_tableManager =
                new PythonArrowTableConverter(m_executorService, ARROW_STORE_FACTORY, getWriteFileStoreHandler());
        }
    }

    @Override
    public ExecutionResult execute(final PortObject[] inData, final PortType[] outputPortTypes,
        final ExecutionContext exec, final FlowVariablesProxy flowVariablesProxy,
        final CredentialsProviderProxy credentialsProviderProxy, final WorkflowPropertiesProxy workflowPropertiesProxy,
        final WarningConsumer warningConsumer) throws Exception {
        initTableManager();
        Map<String, FileStore> fileStoresByKey = new HashMap<>();
        final var executionResult = new PythonExecutionResult();
        final var failure = new FailureState();

        final PythonNodeModelProxy.Callback callback = new Callback() {
            private DefaultLogCallback m_logCallback = new DefaultLogCallback(LOGGER);

            @Override
            public PythonArrowDataSink create_sink() throws IOException {
                return m_tableManager.createSink();
            }

            @Override
            public FileStoreBasedFile create_filestore_file() throws IOException {
                final var fileStoreKey = UUID.randomUUID().toString();
                final var fileStore = FileStoreFactory.createFileStoreFactory(exec).createFileStore(fileStoreKey);
                fileStoresByKey.put(fileStoreKey, fileStore);
                return new FileStoreBasedFile(fileStore.getFile().getAbsolutePath(), fileStoreKey);
            }

            @Override
            public PythonNodeViewSink create_view_sink() throws IOException {
                if (executionResult.m_view == null) {
                    executionResult.m_view = new PythonNodeViewStoragePath();
                }
                return executionResult.m_view.getSink();
            }

            @Override
            public void log(final String msg, final String severity) {
                m_logCallback.log(msg, severity);
            }

            @Override
            public Map<String, Object> get_flow_variables() {
                return flowVariablesProxy.getFlowVariables();
            }

            @Override
            public void set_flow_variables(final Map<String, Object> flowVariables) {
                flowVariablesProxy.setFlowVariables(flowVariables);
            }

            @Override
            public void set_failure(final String message, final String details, final boolean invalidSettings) {
                failure.setFailure(message, details, invalidSettings);
            }

            @Override
            public String get_preferred_value_types_as_json(final String tableSchemaJson) {
                return TableSpecSerializationUtils.getPreferredValueTypesForSerializedSchema(tableSchemaJson);
            }

            @SuppressWarnings("unused")
            public String[] create_file_store() throws IOException { // NOSONAR
                final var fileStore = PythonFileStoreUtils.createFileStore(getWriteFileStoreHandler());
                return new String[]{fileStore.getFile().getAbsolutePath(),
                    FileStoreUtil.getFileStoreKey(fileStore).saveToString()};
            }

            @SuppressWarnings("unused")
            public String file_store_key_to_absolute_path(final String fileStoreKey) { // NOSONAR
                return PythonFileStoreUtils.getAbsolutePathForKey(getFileStoreHandler(),
                    FileStoreKey.load(fileStoreKey));
            }

            @Override
            @SuppressWarnings("unused")
            public String get_auth_schema(final String serializedXMLString) throws CouldNotAuthorizeException, // NOSONAR
                ClassNotFoundException, InstantiationException, IllegalAccessException, IOException { // NOSONAR
                return getAuthSchema(serializedXMLString);

            }

            @Override
            @SuppressWarnings("unused")
            public String get_auth_parameters(final String serializedXMLString) throws CouldNotAuthorizeException, // NOSONAR
                ClassNotFoundException, InstantiationException, IllegalAccessException, IOException { // NOSONAR
                return getAuthParameters(serializedXMLString);
            }

            @Override
            @SuppressWarnings("unused")
            public ExpiryDate get_expires_after(final String serializedXMLString) throws CouldNotAuthorizeException, // NOSONAR
                ClassNotFoundException, InstantiationException, IllegalAccessException, IOException { // NOSONAR
                return getExpiresAfter(serializedXMLString);
            }

        };
        m_proxy.initializeJavaCallback(callback);

        var knimeToPythonConversionContext = new PortObjectConversionContext(fileStoresByKey, m_tableManager, exec);
        final var pythonInputs =
            PythonPortTypeRegistry.convertPortObjectsToPython(Stream.of(inData), knimeToPythonConversionContext);

        exec.setProgress(0.1, "Sending data to Python");

        exec.setMessage(""); // Reset the message -> Only show the message from Python
        var progressMonitor = exec.createSubProgress(0.8);

        var nodeContainer = (NativeNodeContainer)NodeContext.getContext().getNodeContainer();

        final var pythonExecContext = new PythonNodeModelProxy.PythonExecutionContext() {

            ToolExecutor m_toolExecutor = new ToolExecutor(exec, nodeContainer, m_tableManager);

            @Override
            public void set_progress(final double progress, final String message) {
                progressMonitor.setProgress(progress, message);
            }

            @Override
            public void set_progress(final double progress) {
                progressMonitor.setProgress(progress);
            }

            @Override
            public boolean is_canceled() {
                try {
                    progressMonitor.checkCanceled();
                } catch (CanceledExecutionException e) { // NOSONAR we use the exception as an indicator for being
                                                         // cancelled
                    return true;
                }
                return false;
            }

            @Override
            public void set_warning(final String message) {
                warningConsumer.setWarning(message);
            }

            @Override
            public String get_workflow_temp_dir() {
                return FileUtil.getWorkflowTempDir().getAbsolutePath();
            }

            @Override
            public String get_workflow_dir() {
                return workflowPropertiesProxy.getLocalWorkflowPath();
            }

            @Override
            public String get_knime_home_dir() {
                return KNIMEConstants.getKNIMEHomeDir();
            }

            @Override
            public String[] get_credentials(final String identifier) {
                ICredentials credentials = credentialsProviderProxy.getCredentials(identifier);
                return new String[]{credentials.getLogin(), credentials.getPassword(), credentials.getName()};

            }

            @Override
            public String[] get_credential_names() {
                return credentialsProviderProxy.getCredentialNames();
            }

            @Override
            public String get_node_id() {
                return workflowPropertiesProxy.getNodeNameWithID();
            }

            @Override
            public Map<String, int[]> get_input_port_map() {
                return ((DelegatingNodeModel)getNode().getNodeModel()).getInputPortMap();
            }

            @Override
            public Map<String, int[]> get_output_port_map() {
                return ((DelegatingNodeModel)getNode().getNodeModel()).getOutputPortMap();
            }

            @Override
            public PythonToolResult execute_tool(final PurePythonTablePortObject toolTable, final String parameters,
                final List<PythonPortObject> inputs, final Map<String, String> executionHints) {
                return m_toolExecutor.executeTool(toolTable, parameters, inputs, executionHints);
            }

        };

        // Configure before execution whether the gateway should be left open, otherwise an exception thrown in Python
        // will always close the gateway.
        var connectionType = determineConnectionType(inData, outputPortTypes);
        m_closeableGateway.setLeaveGatewayOpen(connectionType != ConnectionType.INDEPENDENT);
        if (connectionType == ConnectionType.SOURCE) {
            m_closeableGateway.retainGateway();
        }

        final var pythonOutputs = m_proxy.execute(pythonInputs, pythonExecContext);

        failure.throwIfFailure();

        final var outputExec = exec.createSubExecutionContext(0.1);

        PortObjectConversionContext pythonToKnimeConversionContext =
            new PortObjectConversionContext(fileStoresByKey, m_tableManager, outputExec);
        executionResult.m_portObjects =
            PythonPortTypeRegistry.convertPortObjectsFromPython(pythonOutputs.stream(), pythonToKnimeConversionContext);

        return executionResult;
    }

    /**
     * 1. if no connectionPort in InData but there is one in OutData: -> This is a source. Don't close but remember
     * gateway for reset & dispose. 2. if connectionPort in inData -> the node is connected. Do not close gateway, will
     * be done by source. 3. no connection port at all -> independent -> close as usual
     *
     * @param inData
     * @param outData
     * @return
     */
    private static ConnectionType determineConnectionType(final PortObject[] inData, final PortType[] outputPortTypes) {
        for (var inputPort : inData) {
            if (inputPort instanceof PythonTransientConnectionPortObject) {
                return ConnectionType.CONNECTED;
            }
        }
        for (var outputPort : outputPortTypes) {
            if (outputPort.acceptsPortObjectClass(PythonTransientConnectionPortObject.class)) {
                return ConnectionType.SOURCE;
            }
        }
        return ConnectionType.INDEPENDENT;
    }

    public static class PythonExecutionResult implements ExecutionResult {
        private PortObject[] m_portObjects;

        private PythonNodeViewStoragePath m_view;

        @Override
        public PortObject[] getPortObjects() {
            return m_portObjects;
        }

        @Override
        public Optional<PythonNodeViewStoragePath> getView() {
            return Optional.ofNullable(m_view);
        }
    }

    @Override
    public PortObjectSpec[] configure(final PortObjectSpec[] inSpecs, final FlowVariablesProxy flowVariablesProxy,
        final CredentialsProviderProxy credentialsProviderProxy, final WorkflowPropertiesProxy workflowPropertiesProxy,
        final WarningConsumer warningConsumer) throws InvalidSettingsException {

        final var failure = new FailureState();

        final PythonNodeModelProxy.Callback callback = new Callback() {
            private DefaultLogCallback m_logCallback = new DefaultLogCallback(LOGGER);

            @Override
            public PythonArrowDataSink create_sink() throws IOException {
                throw new IllegalStateException("Cannot create arrow data sink in configure");
            }

            @Override
            public FileStoreBasedFile create_filestore_file() throws IOException {
                throw new IllegalStateException("Cannot create filestore in configure");
            }

            @Override
            public void log(final String msg, final String severity) {
                m_logCallback.log(msg, severity);
            }

            @Override
            public Map<String, Object> get_flow_variables() {
                return flowVariablesProxy.getFlowVariables();
            }

            @Override
            public void set_flow_variables(final Map<String, Object> flowVariables) {
                flowVariablesProxy.setFlowVariables(flowVariables);
            }

            @Override
            public PythonNodeViewSink create_view_sink() throws IOException {
                throw new IllegalStateException("Cannot create view in configure");
            }

            @Override
            public void set_failure(final String message, final String details, final boolean invalidSettings) {
                failure.setFailure(message, details, invalidSettings);
            }

            @Override
            public String get_preferred_value_types_as_json(final String tableSchemaJson) {
                return TableSpecSerializationUtils.getPreferredValueTypesForSerializedSchema(tableSchemaJson);
            }

            @Override
            @SuppressWarnings("unused")
            public String get_auth_schema(final String serializedXMLString) throws CouldNotAuthorizeException, // NOSONAR
                ClassNotFoundException, InstantiationException, IllegalAccessException, IOException { // NOSONAR
                return getAuthSchema(serializedXMLString);

            }

            @Override
            @SuppressWarnings("unused")
            public String get_auth_parameters(final String serializedXMLString) throws CouldNotAuthorizeException, // NOSONAR
                ClassNotFoundException, InstantiationException, IllegalAccessException, IOException { // NOSONAR
                return getAuthParameters(serializedXMLString);
            }

            @Override
            @SuppressWarnings("unused")
            public ExpiryDate get_expires_after(final String serializedXMLString) throws CouldNotAuthorizeException, // NOSONAR
                ClassNotFoundException, InstantiationException, IllegalAccessException, IOException { // NOSONAR
                return getExpiresAfter(serializedXMLString);
            }
        };
        m_proxy.initializeJavaCallback(callback);

        final var pythonConfigContext = new PythonNodeModelProxy.PythonConfigurationContext() {

            @Override
            public void set_warning(final String message) {
                warningConsumer.setWarning(message);
            }

            @Override
            public String[] get_credentials(final String identifier) {
                ICredentials credentials = credentialsProviderProxy.getCredentials(identifier);
                return new String[]{credentials.getLogin(), credentials.getPassword(), credentials.getName()};
            }

            @Override
            public String[] get_credential_names() {
                return credentialsProviderProxy.getCredentialNames();

            }

            @Override
            public String get_node_id() {
                return workflowPropertiesProxy.getNodeNameWithID();
            }

            @Override
            public Map<String, int[]> get_input_port_map() {
                return ((DelegatingNodeModel)getNode().getNodeModel()).getInputPortMap();
            }

            @Override
            public Map<String, int[]> get_output_port_map() {
                return ((DelegatingNodeModel)getNode().getNodeModel()).getOutputPortMap();
            }
        };

        final var serializedInSpecs = Stream.of(inSpecs)//
            .map(PythonPortTypeRegistry::convertPortObjectSpecToPython)//
            .toArray(PythonPortObjectSpec[]::new);

        final var serializedOutSpecs = m_proxy.configure(serializedInSpecs, pythonConfigContext);
        failure.throwIfFailure();

        // Get number of active ports
        final Map<String, int[]> portMap = ((DelegatingNodeModel)getNode().getNodeModel()).getOutputPortMap();
        final var activePortsNumber = portMap.values().stream().mapToInt(array -> array.length).sum();

        if (serializedOutSpecs.size() != activePortsNumber) {
            throw new IllegalStateException("Python node configure returned wrong number of output port specs");
        }

        return serializedOutSpecs.stream()//
            .map(PythonPortTypeRegistry::convertPortObjectSpecFromPython)//
            .toArray(PortObjectSpec[]::new);
    }

    private IWriteFileStoreHandler getWriteFileStoreHandler() {
        final IFileStoreHandler nodeFsHandler = getFileStoreHandler();
        if (nodeFsHandler instanceof IWriteFileStoreHandler writeFsHandler) {
            return writeFsHandler;
        } else {
            throw new IllegalStateException(
                "A WriteFileStoreHandler should be available during execution of Python Nodes");
        }
    }

    private synchronized IFileStoreHandler getFileStoreHandler() {
        if (m_fileStoreHandler == null) {
            if (NodeContext.getContextOptional().map(NodeContext::getNodeContainer)
                .orElse(null) instanceof NativeNodeContainer nnc) {
                var fileStoreHandler = nnc.getNode().getFileStoreHandler();
                if (nnc.getNodeContainerState().isExecuted()) {
                    // The “Agent Chat View” node executes tools/workflow while it is already executed.
                    // The result data of these tool calls are ignored (not part of the data).
                    // There is a pending discussion on how that node should behave
                    // (interacting with it while it is executing vs. executed), which is part of AP-24554.
                    // TODO AP-24555: let the python node explicitly define what file store handler to use?
                    m_fileStoreHandler = new DelegatingAndNotInWorkflowWriteFileStoreHandler(fileStoreHandler);
                } else {
                    m_fileStoreHandler = fileStoreHandler;
                }
            } else {
                throw new IllegalStateException("A NodeContext should be available during execution of Python Nodes");
            }
        }
        return m_fileStoreHandler;

    }

    /**
     * Delegates the <b>read</b> file-store operations to the passed one. All <b>write</b> operations are delegated to a
     * {@link NotInWorkflowWriteFileStoreHandler}.
     */
    private static class DelegatingAndNotInWorkflowWriteFileStoreHandler implements IWriteFileStoreHandler {

        private final IFileStoreHandler m_readFileStoreHandlerDelegate;

        private final NotInWorkflowWriteFileStoreHandler m_writeFileStoreHandlerDelegate;

        DelegatingAndNotInWorkflowWriteFileStoreHandler(final IFileStoreHandler readFileStoreHandlerDelegate) {
            m_readFileStoreHandlerDelegate = readFileStoreHandlerDelegate;
            m_writeFileStoreHandlerDelegate = new NotInWorkflowWriteFileStoreHandler(UUID.randomUUID(),
                readFileStoreHandlerDelegate.getDataRepository());
            m_writeFileStoreHandlerDelegate.open();
        }

        @Override
        public IDataRepository getDataRepository() {
            return m_readFileStoreHandlerDelegate.getDataRepository();
        }

        @Override
        public void clearAndDispose() {
            m_writeFileStoreHandlerDelegate.clearAndDispose();
        }

        @Override
        public FileStore getFileStore(final FileStoreKey key) {
            return m_readFileStoreHandlerDelegate.getFileStore(key);
        }

        @Override
        public FileStore createFileStore(final String name) throws IOException {
            return m_writeFileStoreHandlerDelegate.createFileStore(name);
        }

        @Override
        public FileStore createFileStore(final String name, final int[] nestedLoopPath, final int iterationIndex)
            throws IOException {
            return m_writeFileStoreHandlerDelegate.createFileStore(name, nestedLoopPath, iterationIndex);
        }

        @Override
        public void open(final ExecutionContext exec) {
            m_writeFileStoreHandlerDelegate.open(exec);
        }

        @Override
        public void addToRepository(final IDataRepository repository) {
            m_writeFileStoreHandlerDelegate.addToRepository(repository);
        }

        @Override
        public void close() {
            m_writeFileStoreHandlerDelegate.close();
        }

        @Override
        public void ensureOpenAfterLoad() throws IOException {
            m_writeFileStoreHandlerDelegate.ensureOpenAfterLoad();
        }

        @Override
        public FileStoreKey translateToLocal(final FileStore fs, final FlushCallback flushCallback) {
            return m_writeFileStoreHandlerDelegate.translateToLocal(fs, flushCallback);
        }

        @Override
        public boolean mustBeFlushedPriorSave(final FileStore fs) {
            return m_writeFileStoreHandlerDelegate.mustBeFlushedPriorSave(fs);
        }

        @Override
        public UUID getStoreUUID() {
            return m_writeFileStoreHandlerDelegate.getStoreUUID();
        }

        @Override
        public File getBaseDir() {
            return m_writeFileStoreHandlerDelegate.getBaseDir();
        }

        @Override
        public boolean isReference() {
            return m_writeFileStoreHandlerDelegate.isReference();
        }

    }

    @Override
    public JsonNodeSettings getSettings(final String version) {
        return new JsonNodeSettingsSchema(m_proxy.getSchema(version), version).createFromJson(m_proxy.getParameters());
    }

    @Override
    public JsonNodeSettingsSchema getSettingsSchema(final String version) {
        return new JsonNodeSettingsSchema(m_proxy.getSchema(version), version);
    }

    @Override
    public int getNumViews() {
        return m_nodeSpec.getNumViews();
    }

    /**
     * @return an array of flow variable types that can be understood by this Python backend.
     */
    public static VariableType<?>[] getCompatibleFlowVariableTypes() { //NOSONAR
        return FlowVariableUtils.convertToFlowVariableTypes(KNOWN_FLOW_VARIABLE_TYPES);
    }

    /** Hold the state of a failure which can be thrown as an exception later */
    private static final class FailureState {
        private String m_message;

        private String m_details;

        private boolean m_isInvalidSettings;

        private void setFailure(final String message, final String details, final boolean invalidSettings) {
            m_message = message;
            m_details = details;
            m_isInvalidSettings = invalidSettings;
        }

        private void throwIfFailure() throws InvalidSettingsException {
            // Log the details to the warning log
            if (m_details != null && !m_details.isBlank()) {
                LOGGER.warn(m_details);
            }
            // Throw an exception with the message
            if (m_message != null) {
                if (m_isInvalidSettings) {
                    throw new InvalidSettingsException(m_message);
                } else {
                    throw new PythonNodeRuntimeException(m_message);
                }
            }
        }
    }

    @Override
    public DataServiceProxy getDataServiceProxy(final JsonNodeSettings settings, final PortObject[] portObjects,
        final PortMapProvider portMapProvider, final CredentialsProviderProxy credentialsProvider) {

        loadValidatedSettings(settings);

        initTableManager();
        var callback = new DefaultViewCallback(m_tableManager, new DefaultLogCallback(LOGGER), getFileStoreHandler());

        m_proxy.initializeJavaCallback(callback);

        var nnc = (NativeNodeContainer)NodeContext.getContext().getNodeContainer();
        var exec = nnc.createExecutionContext();
        var toolExecutor = new ToolExecutor(exec, nnc, m_tableManager);
        var context = new DefaultViewContext(toolExecutor, portMapProvider, credentialsProvider);

        var fileStoresByKey = new HashMap<String, FileStore>();
        final var knimeToPythonConversionContext =
            new PortObjectConversionContext(fileStoresByKey, m_tableManager, exec);
        var pythonPortObjects =
            PythonPortTypeRegistry.convertPortObjectsToPython(Stream.of(portObjects), knimeToPythonConversionContext);

        var pythonDataService = m_proxy.getDataService(context, pythonPortObjects);

        return new DataServiceProxy() {
            @Override
            public String handleJsonRpcRequest(final String request) {
                return pythonDataService.handleJsonRpcRequest(request);
            }

            @Override
            public void close() throws Exception {
                toolExecutor.close();
            }
        };
    }

}
