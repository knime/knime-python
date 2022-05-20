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
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.data.filestore.FileStore;
import org.knime.core.data.filestore.FileStoreFactory;
import org.knime.core.data.filestore.internal.IFileStoreHandler;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.node.workflow.VariableType;
import org.knime.core.util.PathUtils;
import org.knime.core.util.ThreadUtils;
import org.knime.core.util.asynclose.AsynchronousCloseable;
import org.knime.python2.kernel.Python2KernelBackend;
import org.knime.python3.PythonGateway;
import org.knime.python3.arrow.PythonArrowDataSink;
import org.knime.python3.arrow.PythonArrowTableConverter;
import org.knime.python3.nodes.extension.ExtensionNode;
import org.knime.python3.nodes.ports.PythonPortObjectTypeRegistry;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObjectSpec;
import org.knime.python3.nodes.proxy.CloseableNodeDialogProxy;
import org.knime.python3.nodes.proxy.CloseableNodeFactoryProxy;
import org.knime.python3.nodes.proxy.CloseableNodeModelProxy;
import org.knime.python3.nodes.proxy.NodeProxy;
import org.knime.python3.nodes.proxy.PythonNodeModelProxy;
import org.knime.python3.nodes.proxy.PythonNodeModelProxy.Callback;
import org.knime.python3.nodes.proxy.PythonNodeModelProxy.FileStoreBasedFile;
import org.knime.python3.utils.FlowVariableUtils;
import org.knime.python3.views.PythonNodeViewSink;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Manages the lifecycle of a Python based NodeProxy and its associated process. Invoking {@link Closeable#close()}
 * shuts down the Python process and the proxy is no longer usable.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class CloseablePythonNodeProxy
    implements CloseableNodeModelProxy, CloseableNodeFactoryProxy, CloseableNodeDialogProxy {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(CloseablePythonNodeProxy.class);

    private final NodeProxy m_proxy;

    private final PythonGateway<?> m_gateway;

    private final AsynchronousCloseable<RuntimeException> m_closer =
        AsynchronousCloseable.createAsynchronousCloser(this::closeInternal);

    private final ExtensionNode m_nodeSpec;

    private static final ArrowColumnStoreFactory ARROW_STORE_FACTORY = new ArrowColumnStoreFactory();

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

    CloseablePythonNodeProxy(final NodeProxy proxy, final PythonGateway<?> gateway, final ExtensionNode nodeSpec) {
        m_proxy = proxy;
        m_gateway = gateway;
        m_nodeSpec = nodeSpec;
    }

    private void closeInternal() {
        try {
            m_gateway.close();
            if (m_tableManager != null) {
                m_tableManager.close();
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to shutdown Python gateway.", ex);
        }
    }

    @Override
    public void close() {
        m_closer.close();
    }

    @Override
    public Future<Void> asynchronousClose() {
        return m_closer.asynchronousClose();
    }

    @Override
    public String getParameters() {
        return m_proxy.getParameters();
    }

    @Override
    public String getDialogRepresentation(final String parameters, final String version, final PortObjectSpec[] specs) {
        final PythonPortObjectSpec[] serializedSpecs = Arrays.stream(specs)
            .map(PythonPortObjectTypeRegistry::convertToPythonPortObjectSpec).toArray(PythonPortObjectSpec[]::new);

        return m_proxy.getDialogRepresentation(parameters, version, serializedSpecs);
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
    public JsonNodeSettings saveSettings() {
        return new JsonNodeSettings(m_proxy.getParameters(), m_proxy.getSchema());
    }

    private void initTableManager() {
        if (m_tableManager == null) {
            m_tableManager =
                new PythonArrowTableConverter(m_executorService, ARROW_STORE_FACTORY, getWriteFileStoreHandler());
        }
    }

    @Override
    public ExecutionResult execute(final PortObject[] inData, final ExecutionContext exec,
        final FlowVariablesProxy flowVariablesProxy) throws IOException, CanceledExecutionException {
        initTableManager();
        Map<String, FileStore> fileStoresByKey = new HashMap<>();
        final PythonExecutionResult executionResult = new PythonExecutionResult();

        final PythonNodeModelProxy.Callback callback = new Callback() {

            @Override
            public String resolve_knime_url(final String knimeUrl) {
                return Python2KernelBackend.resolveKnimeUrl(knimeUrl, /* m_nodeContextManager */null);
            }

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
                    executionResult.m_view = PathUtils.createTempFile("python_node_view_", "html");
                }
                return new PythonNodeViewSink(executionResult.m_view.toAbsolutePath().toString());
            }

            @Override
            public void log(final String msg) {
                LOGGER.warn(msg);
            }

            @Override
            public Map<String, Object> get_flow_variables() {
                return flowVariablesProxy.getFlowVariables();
            }

            @Override
            public void set_flow_variables(final Map<String, Object> flowVariables) {
                flowVariablesProxy.setFlowVariables(flowVariables);
            }
        };
        m_proxy.initializeJavaCallback(callback);

        final var pythonInputs =
            Arrays.stream(inData).map(po -> PythonPortObjectTypeRegistry.convertToPythonPortObject(po, m_tableManager))
                .toArray(PythonPortObject[]::new);
        exec.setProgress(0.1, "Sending data to Python");

        var progressMonitor = exec.createSubProgress(0.8);

        final var pythonExecContext = new PythonNodeModelProxy.PythonExecutionContext() {
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
        };

        final var pythonOutputs = m_proxy.execute(pythonInputs, pythonExecContext);
        final var outputExec = exec.createSubExecutionContext(0.1);

        executionResult.m_portObjects = pythonOutputs.stream().map(ppo -> PythonPortObjectTypeRegistry
            .convertFromPythonPortObject(ppo, fileStoresByKey, m_tableManager, outputExec)).toArray(PortObject[]::new);
        return executionResult;
    }

    public static class PythonExecutionResult implements ExecutionResult {
        private PortObject[] m_portObjects;

        private Path m_view;

        @Override
        public PortObject[] getPortObjects() {
            return m_portObjects;
        }

        @Override
        public Optional<Path> getView() {
            return Optional.ofNullable(m_view);
        }
    }

    @Override
    public PortObjectSpec[] configure(final PortObjectSpec[] inSpecs, final FlowVariablesProxy flowVariablesProxy) {
        final PythonPortObjectSpec[] serializedInSpecs = Arrays.stream(inSpecs)
            .map(PythonPortObjectTypeRegistry::convertToPythonPortObjectSpec).toArray(PythonPortObjectSpec[]::new);

        final PythonNodeModelProxy.Callback callback = new Callback() {

            @Override
            public String resolve_knime_url(final String knimeUrl) {
                return Python2KernelBackend.resolveKnimeUrl(knimeUrl, /* m_nodeContextManager */null);
            }

            @Override
            public PythonArrowDataSink create_sink() throws IOException {
                throw new IllegalStateException("Cannot create arrow data sink in configure");
            }

            @Override
            public FileStoreBasedFile create_filestore_file() throws IOException {
                throw new IllegalStateException("Cannot create filestore in configure");
            }

            @Override
            public void log(final String msg) {
                LOGGER.warn(msg);
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
        };
        m_proxy.initializeJavaCallback(callback);

        final var pythonConfigContext = new PythonNodeModelProxy.PythonConfigurationContext() {
            // TODO: add flow variables
        };

        final var serializedOutSpecs = m_proxy.configure(serializedInSpecs, pythonConfigContext);

        final var outputPortTypeIdentifiers = m_nodeSpec.getOutputPortTypes();
        if (serializedOutSpecs.size() != outputPortTypeIdentifiers.length) {
            throw new IllegalStateException("Python node configure returned wrong number of output port specs");
        }

        return serializedOutSpecs.stream().map(PythonPortObjectTypeRegistry::convertFromPythonPortObjectSpec)
            .toArray(PortObjectSpec[]::new);
    }

    private static IWriteFileStoreHandler getWriteFileStoreHandler() {
        final IFileStoreHandler nodeFsHandler = getFileStoreHandler();
        IWriteFileStoreHandler fsHandler = null;
        if (nodeFsHandler instanceof IWriteFileStoreHandler) {
            fsHandler = (IWriteFileStoreHandler)nodeFsHandler;
        } else {
            throw new IllegalStateException("A NodeContext should be available during execution of Python Nodes");
        }
        return fsHandler;
    }

    private static IFileStoreHandler getFileStoreHandler() {
        return ((NativeNodeContainer)NodeContext.getContext().getNodeContainer()).getNode().getFileStoreHandler();
    }

    @Override
    public String getSchema() {
        return m_proxy.getSchema();
    }

    @Override
    public int getNumViews() {
        return m_nodeSpec.getNumViews();
    }

    /**
     * @return an array of flow variable types that can be understood by this Python backend.
     */
    public static VariableType<?>[] getCompatibleFlowVariableTypes() {
        return FlowVariableUtils.convertToFlowVariableTypes(KNOWN_FLOW_VARIABLE_TYPES);
    }
}
