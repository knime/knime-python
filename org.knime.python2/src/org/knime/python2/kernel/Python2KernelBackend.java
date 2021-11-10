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
 *   Jul 22, 2021 (marcel): created
 */
package org.knime.python2.kernel;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.SystemUtils;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.internal.ReferencedFile;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.util.Pair;
import org.knime.core.util.pathresolve.ResolverUtil;
import org.knime.python.typeextension.KnimeToPythonExtension;
import org.knime.python.typeextension.KnimeToPythonExtensions;
import org.knime.python.typeextension.PythonModuleExtensions;
import org.knime.python.typeextension.PythonToKnimeExtension;
import org.knime.python.typeextension.PythonToKnimeExtensions;
import org.knime.python2.Activator;
import org.knime.python2.PythonCommand;
import org.knime.python2.extensions.serializationlibrary.SentinelOption;
import org.knime.python2.extensions.serializationlibrary.SerializationLibraryExtensions;
import org.knime.python2.extensions.serializationlibrary.interfaces.Cell;
import org.knime.python2.extensions.serializationlibrary.interfaces.Row;
import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibrary;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableChunker;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableCreator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableCreatorFactory;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableIterator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.BufferedDataTableChunker;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.BufferedDataTableCreator;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.CellImpl;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.KeyValueTableCreator;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.KeyValueTableIterator;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.RowImpl;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.TableSpecImpl;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.TemporaryTableCreator;
import org.knime.python2.generic.ImageContainer;
import org.knime.python2.kernel.messaging.AbstractRequestHandler;
import org.knime.python2.kernel.messaging.DefaultMessage;
import org.knime.python2.kernel.messaging.DefaultMessage.PayloadDecoder;
import org.knime.python2.kernel.messaging.DefaultMessage.PayloadEncoder;
import org.knime.python2.kernel.messaging.Message;
import org.knime.python2.kernel.messaging.TaskHandler;
import org.knime.python2.port.PickledObjectFile;
import org.knime.python2.util.PythonUtils;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Legacy back end of {@link PythonKernel}. "Legacy" means that this back end is part of the soon-to-be-deprecated
 * version 2 of the KNIME Python integration (org.knime.python2). "Python2" in the name of this class also refers to
 * this version, not the version of the Python language (the back end supports both language versions Python 2 and
 * Python 3).
 *
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class Python2KernelBackend implements PythonKernelBackend {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(Python2KernelBackend.class);

    private static final String CONNECT_TIMEOUT_VM_OPT = "knime.python.connecttimeout";

    private static final String CLEANUP_TIMEOUT_VM_OPT = "knime.python.cleanuptimeout";

    /**
     * @return the duration, in milliseconds, to wait when trying to establish a connection to Python
     */
    public static int getConnectionTimeoutInMillis() {
        final String defaultTimeout = "30000";
        try {
            final String timeout = System.getProperty(CONNECT_TIMEOUT_VM_OPT, defaultTimeout);
            return Integer.parseInt(timeout);
        } catch (final NumberFormatException ex) {
            LOGGER.warn("The VM option -D" + CONNECT_TIMEOUT_VM_OPT +
                " was set to a non-integer value. This is invalid. It therefore defaults to " + defaultTimeout +
                " ms.");
            return Integer.parseInt(defaultTimeout);
        }
    }

    /**
     * @return the duration, in milliseconds, to wait when trying to cleanup resources (e.g., close database
     *         connections) upon Python kernel shutdown
     */
    public static int getCleanupTimeoutInMillis() {
        final String defaultTimeout = "30000";
        try {
            final String timeout = System.getProperty(CLEANUP_TIMEOUT_VM_OPT, defaultTimeout);
            return Integer.parseInt(timeout);
        } catch (final NumberFormatException ex) {
            LOGGER.warn("The VM option -D" + CLEANUP_TIMEOUT_VM_OPT +
                " was set to a non-integer value. This is invalid. It therefore defaults to " + defaultTimeout +
                " ms.");
            return Integer.parseInt(defaultTimeout);
        }
    }

    /**
     * Tries to locate the directory of the workflow associated with the given node context, for the sake of setting
     * Python's current working directory.
     *
     * @param nodeContext Required to identify the containing workflow.
     * @param logger Used to log a warning if retrieving the workflow directory failed.
     * @return The workflow directory. Empty if retrieving the workflow directory failed.
     */
    public static Optional<String> getWorkflowDirectoryForSettingWorkingDirectory(final NodeContext nodeContext,
        final NodeLogger logger) {
        try {
            if (nodeContext != null) {
                final var workflowManager = nodeContext.getWorkflowManager();
                if (workflowManager != null) {
                    final ReferencedFile workflowDirRef = workflowManager.getNodeContainerDirectory();
                    if (workflowDirRef != null) {
                        return Optional.of(workflowDirRef.getFile().toString());
                    }
                }
            }
        } catch (final Exception ex) {
            // Do not propagate exception since setting the CWD is merely for convenience.
            logger.warn("Python's current working directory could not be set to the workflow directory.", ex);
        }
        return Optional.empty();
    }

    private final PythonCommand m_command;

    private final Process m_process;

    private final Integer m_pid; // Nullable.

    private final ServerSocket m_serverSocket;

    private final Socket m_socket;

    private final PythonOutputListeners m_outputListeners;

    private final PythonCommands m_commands;

    private final boolean m_hasAutocomplete;

    /** Used to make kernel operations cancelable. */
    private final ExecutorService m_executorService =
        Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("python-worker-%d").build());

    /**
     * Initialized by {@link #setOptions(PythonKernelOptions)}.
     */
    private PythonKernelOptions m_kernelOptions;

    /**
     * Initialized by {@link #setOptions(PythonKernelOptions)}.
     */
    private SerializationLibrary m_serializer;

    /**
     * Properly initialized by {@link #setOptions(PythonKernelOptions)}. Holds the node context that was active at the
     * time when that method was called (if any).
     */
    private final NodeContextManager m_nodeContextManager = new NodeContextManager();

    private final AtomicBoolean m_closed = new AtomicBoolean(false);

    /**
     * Creates a new Python kernel back end by starting a Python process and connecting to it.
     * <P>
     * Important: Call the {@link #close()} method when this kernel is no longer needed to shut down the Python process
     * in the background.
     *
     * @param command The {@link PythonCommand} that is used to launch the Python process.
     * @throws PythonInstallationTestException If the Python environment represented by the given {@link PythonCommand}
     *             is not capable of running the Python kernel (e.g., because it misses essential Python modules or
     *             there are version mismatches).
     * @throws PythonIOException If the kernel could not be set up for any reason. This includes the
     *             {@link PythonInstallationTestException} described above which subclasses {@link PythonIOException}.
     *             Other possible cases include: process creation problems, socket connection problems, exceptions on
     *             Python side during setup, communication errors between the Java and the Python side.
     */
    public Python2KernelBackend(final PythonCommand command) throws PythonIOException {
        m_command = command;

        PythonKernel.testInstallation(command, Collections.emptyList());

        try {
            // Setup Python kernel:

            // Start socket creation. The created socket is used to communicate with the Python process that is created below.
            m_serverSocket = new ServerSocket(0);
            m_serverSocket.setSoTimeout(getConnectionTimeoutInMillis());
            final Future<Socket> socketBeingSetup = setupSocket();

            // Create Python process.
            m_process = setupPythonProcess(command);

            // Start listening to stdout and stderror.
            m_outputListeners =
                new PythonOutputListeners(m_process.getInputStream(), m_process.getErrorStream(), m_nodeContextManager);
            m_outputListeners.startListening();

            try {
                // Wait for Python to connect.
                m_socket = socketBeingSetup.get();
            } catch (final ExecutionException e) {
                if (e.getCause() instanceof SocketTimeoutException) {
                    // Under some circumstances, the Python process may crash while we're trying to establish a socket
                    // connection which causes the attempt to time out. We should not misinterpret that as a real time out.
                    if (!isPythonProcessAlive()) {
                        throw new PythonIOException(
                            "The external Python process crashed for unknown reasons while KNIME " +
                                "set up the Python environment. See log for details.",
                            e);
                    } else {
                        throw new PythonIOException(
                            "The connection attempt timed out. Please consider increasing the socket " +
                                "timeout using the VM option '-D" + CONNECT_TIMEOUT_VM_OPT + "=<value-in-ms>'.\n" +
                                "Also make sure that the communication between KNIME and Python is not blocked by a " +
                                "firewall and that your hosts configuration is correct.",
                            e);
                    }
                } else {
                    throw e;
                }
            }

            // Setup command/message system.
            m_commands = new PythonCommands(m_socket.getOutputStream(), m_socket.getInputStream(),
                new PythonKernelExecutionMonitor());

            // Setup request handlers.
            setupRequestHandlers();

            // Start commands/messaging system once everything is set up.
            m_commands.start();

            // PID of Python process.
            m_pid = m_commands.getPid().get();
            LOGGER.debug("Python PID: " + m_pid);

            m_hasAutocomplete = checkHasAutoComplete();
        } catch (Throwable t) {
            // Close is not called by try-with-resources if an exception occurs during construction.
            PythonKernelCleanupException suppressed = null;
            try {
                close();
            } catch (final PythonKernelCleanupException ex) {
                suppressed = ex;
            }

            // Unwrap exception that occurred during any async setup.
            t = PythonUtils.Misc.unwrapExecutionException(t).orElse(t);
            if (suppressed != null) {
                t.addSuppressed(suppressed);
            }

            if (t instanceof PythonIOException) {
                throw (PythonIOException)t;
            } else if (t instanceof Error) {
                throw (Error)t;
            } else {
                throw new PythonIOException("An exception occurred while setting up the Python kernel. " //
                    + "See log for details.", t);
            }
        }
    }

    // Initial setup methods:

    private Future<Socket> setupSocket() {
        return m_executorService.submit(m_serverSocket::accept);
    }

    private Process setupPythonProcess(final PythonCommand command) throws IOException {
        final String kernelScriptPath = PythonKernelOptions.KERNEL_SCRIPT_PATH;
        final String port = Integer.toString(m_serverSocket.getLocalPort());
        // Build and start Python kernel that listens to the given port:
        final ProcessBuilder pb = command.createProcessBuilder();
        // Use the -u options to force Python to not buffer stdout and stderror.
        Collections.addAll(pb.command(), "-u", kernelScriptPath, port);
        // Add all python modules to PYTHONPATH variable.
        String existingPath = pb.environment().get("PYTHONPATH");
        existingPath = existingPath == null ? "" : existingPath;
        String externalPythonPath = PythonModuleExtensions.getPythonPath();
        externalPythonPath += File.pathSeparator + Activator.getFile(Activator.PLUGIN_ID, "py").getAbsolutePath();
        if (!externalPythonPath.isEmpty()) {
            if (existingPath.isEmpty()) {
                existingPath = externalPythonPath;
            } else {
                existingPath = existingPath + File.pathSeparator + externalPythonPath;
            }
        }
        existingPath = existingPath + File.pathSeparator;
        pb.environment().put("PYTHONPATH", existingPath);
        // Set JAVA_HOME. This is needed by the Python database/hive nodes that make use of JDBC.
        // TODO: It would probably be better to let clients (such as the DB nodes) specify which environment variables
        // they need to have set instead of doing this here. But this would require extending PythonCommand or
        // PythonKernelOptions.
        pb.environment().computeIfAbsent("JAVA_HOME", k -> {
            try {
                return System.getProperty("java.home");
            } catch (final Exception ex) {
                // Ignore - setting JAVA_HOME is not mandatory in general but only needed in some specific cases.
                LOGGER.debug(ex);
                return null;
            }
        });

        pb.redirectOutput(ProcessBuilder.Redirect.PIPE);
        pb.redirectError(ProcessBuilder.Redirect.PIPE);

        // Start Python.
        return pb.start();
    }

    private void setupRequestHandlers() {
        registerTaskHandler("serializer_request", new AbstractRequestHandler() {

            @Override
            protected Message respond(final Message request, final int responseMessageId) throws Exception {
                final String payload = new PayloadDecoder(request.getPayload()).getNextString();
                for (final PythonToKnimeExtension extension : PythonToKnimeExtensions.getExtensions()) {
                    if (extension.getType().equals(payload) || extension.getId().equals(payload)) {
                        final byte[] responsePayload = new PayloadEncoder() //
                            .putString(extension.getId()) //
                            .putString(extension.getType()) //
                            .putString(extension.getPythonSerializerPath()) //
                            .get();
                        return createResponse(request, responseMessageId, true, responsePayload, null);
                    }
                }
                // TODO: Change to failure response without a payload (requires changes on Python side).
                final byte[] emptyResponsePayload = new PayloadEncoder() //
                    .putString("") //
                    .putString("") //
                    .putString("") //
                    .get();
                return createResponse(request, responseMessageId, true, emptyResponsePayload, null);
            }
        });

        registerTaskHandler("deserializer_request", new AbstractRequestHandler() {

            @Override
            protected Message respond(final Message request, final int responseMessageId) throws ExecutionException {
                final String payload = new PayloadDecoder(request.getPayload()).getNextString();
                for (final KnimeToPythonExtension extension : KnimeToPythonExtensions.getExtensions()) {
                    if (extension.getId().equals(payload)) {
                        final byte[] responsePayload = new PayloadEncoder() //
                            .putString(extension.getId()) //
                            .putString(extension.getPythonDeserializerPath()) //
                            .get();
                        return createResponse(request, responseMessageId, true, responsePayload, null);
                    }
                }
                // TODO: Change to failure response without a payload (requires changes on Python side).
                final byte[] emptyResponsePayload = new PayloadEncoder() //
                    .putString("") //
                    .putString("") //
                    .get();
                return createResponse(request, responseMessageId, true, emptyResponsePayload, null);
            }
        });

        registerTaskHandler("resolve_knime_url", new AbstractRequestHandler() {

            @Override
            protected Message respond(final Message request, final int responseMessageId) {
                String uriString = new PayloadDecoder(request.getPayload()).getNextString();
                // Node context may be required to resolve KNIME URL.
                try (NodeContextManager m = m_nodeContextManager.pushNodeContext()) {
                    uriString = fixWindowsUri(uriString);
                    final URI uri = new URI(uriString);
                    final File file = ResolverUtil.resolveURItoLocalOrTempFile(uri);
                    final String path = file.getAbsolutePath();
                    final byte[] responsePayload = new PayloadEncoder().putString(path).get();
                    return createResponse(request, responseMessageId, true, responsePayload, null);
                } catch (InvalidSettingsException | URISyntaxException | IOException | SecurityException ex) {
                    final String errorMessage =
                        "Failed to resolve KNIME URL '" + uriString + "'. Details: " + ex.getMessage();
                    LOGGER.debug(errorMessage, ex);
                    final byte[] errorPayload = new PayloadEncoder().putString(errorMessage).get();
                    return createResponse(request, responseMessageId, false, errorPayload, null);
                }
            }
        });
    }

    private static String fixWindowsUri(String uriString) throws InvalidSettingsException {
        try {
            CheckUtils.checkSourceFile(uriString);
        } catch (InvalidSettingsException ex) {
            if (SystemUtils.IS_OS_WINDOWS && uriString != null) {
                uriString = uriString.replace("\\", "/");
                CheckUtils.checkSourceFile(uriString);
            } else {
                throw ex;
            }
        }
        return uriString;
    }

    private boolean checkHasAutoComplete() {
        try {
            // Check if Python kernel supports auto-completion (this depends on the optional module Jedi).
            return m_commands.hasAutoComplete().get();
        } catch (final Exception ex) {
            LOGGER
                .debug("An exception occurred while checking the auto-completion capabilities of the Python kernel. " +
                    "Auto-completion will not be available.");
            return false;
        }
    }

    private boolean isPythonProcessAlive() {
        return m_process != null && m_process.isAlive();
    }
    // End of initial setup methods.

    @Override
    public PythonCommand getPythonCommand() {
        return m_command;
    }

    @Override
    public PythonOutputListeners getOutputListeners() {
        return m_outputListeners;
    }

    @Override
    public PythonKernelOptions getOptions() {
        return m_kernelOptions;
    }

    // Option setup methods:

    @Override
    public void setOptions(final PythonKernelOptions options) throws PythonIOException {
        m_kernelOptions = options;
        PythonKernel.testInstallation(m_command, options.getAdditionalRequiredModules());
        try {
            m_serializer = findConfiguredSerializationLibrary(options);
            m_nodeContextManager.setNodeContext(NodeContext.getContext());
            // TODO: we should eventually combine all these commands into one to reduce communication/interpretation
            // overhead (cf. AP-14028).
            setSerializationLibrary(options);
            setExternalCustomPath(options);
            setSentinelConstants(options);
            setCurrentWorkingDirToWorkflowDir();
        } catch (final Exception ex) {
            final Throwable t = PythonUtils.Misc.unwrapExecutionException(ex).orElse(ex);
            if (t instanceof PythonIOException) {
                throw (PythonIOException)t;
            } else if (t instanceof Error) {
                throw (Error)t;
            } else {
                throw new PythonIOException("An exception occurred while setting up the Python kernel. " //
                    + "See log for details.", t);
            }
        }
    }

    private static SerializationLibrary findConfiguredSerializationLibrary(final PythonKernelOptions options)
        throws PythonIOException {
        final String serializerId = options.getSerializationOptions().getSerializerId();
        final String serializerName = SerializationLibraryExtensions.getNameForId(serializerId);
        if (serializerName == null) {
            final String message;
            if (serializerId == null) {
                message =
                    "No serialization library was found. Please make sure to install at least one plugin containing one.";
            } else {
                message = "The selected serialization library with id " + serializerId + " was not found. " +
                    "Please make sure to install the correspondent plugin or select a different serialization " +
                    "library in the Python preference page.";
            }
            throw new PythonIOException(message);
        }
        LOGGER.debug("Using serialization library: " + serializerName + ".");
        return SerializationLibraryExtensions.getSerializationLibrary(serializerId);
    }

    private void setSerializationLibrary(final PythonKernelOptions options)
        throws InterruptedException, ExecutionException {
        final String pathToSerializationLibraryPythonModule = SerializationLibraryExtensions
            .getSerializationLibraryPath(options.getSerializationOptions().getSerializerId());
        m_commands.setSerializationLibrary(pathToSerializationLibraryPythonModule).get();
    }

    private void setExternalCustomPath(final PythonKernelOptions options)
        throws InterruptedException, ExecutionException {
        final String externalCustomPath = options.getExternalCustomPath();
        if (!Strings.isNullOrEmpty(externalCustomPath)) {
            m_commands.addToPythonPath(externalCustomPath).get();
        }
    }

    private void setSentinelConstants(final PythonKernelOptions options)
        throws InterruptedException, ExecutionException {
        final SentinelOption sentinelOption = options.getSerializationOptions().getSentinelOption();
        if (sentinelOption == SentinelOption.MAX_VAL) {
            m_commands.execute("INT_SENTINEL = 2**31 - 1; LONG_SENTINEL = 2**63 - 1").get();
        } else if (sentinelOption == SentinelOption.MIN_VAL) {
            m_commands.execute("INT_SENTINEL = -2**31; LONG_SENTINEL = -2**63").get();
        } else {
            final int sentinelValue = options.getSerializationOptions().getSentinelValue();
            m_commands.execute("INT_SENTINEL = " + sentinelValue + "; LONG_SENTINEL = " + sentinelValue).get();
        }
    }

    private void setCurrentWorkingDirToWorkflowDir() {
        final Optional<String> workflowDirOptional =
            getWorkflowDirectoryForSettingWorkingDirectory(m_nodeContextManager.getNodeContext(), LOGGER);
        if (workflowDirOptional.isPresent()) {
            final String workflowDir = "r'" + workflowDirOptional.get() + "'";
            m_commands.execute("import os\n" + //
                "import sys\n" + //
                "os.chdir(" + workflowDir + ")\n" + //
                "sys.path.insert(0, " + workflowDir + ")");
        }
    }

    // End of option setup methods.

    public boolean registerTaskHandler(final String taskCategory, final TaskHandler<?> handler) {
        return m_commands.getMessageHandlers().registerMessageHandler(taskCategory,
            m_commands.createTaskFactory(handler));
    }

    public boolean unregisterTaskHandler(final String taskCategory) {
        return m_commands.getMessageHandlers().unregisterMessageHandler(taskCategory);
    }

    @Override
    public void putFlowVariables(final String name, final Collection<FlowVariable> flowVariables)
        throws PythonIOException {
        try {
            final byte[] bytes = flowVariablesToBytes(flowVariables);
            m_commands.putFlowVariables(name, bytes).get();
        } catch (final ExecutionException ex) {
            throw getMostSpecificPythonKernelException(ex);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    /**
     * Serialize a collection of flow variables to a {@link Row}.
     *
     * @param flowVariables
     * @return the serialized flow variables
     * @throws PythonIOException If an error occurred while communicating with the python kernel or while executing the
     *             task
     */
    private byte[] flowVariablesToBytes(final Collection<FlowVariable> flowVariables) throws PythonIOException {
        final Type[] types = new Type[flowVariables.size()];
        final String[] columnNames = new String[flowVariables.size()];
        final RowImpl row = new RowImpl("0", flowVariables.size());
        int i = 0;
        for (final FlowVariable flowVariable : flowVariables) {
            final String key = flowVariable.getName();
            columnNames[i] = key;
            switch (flowVariable.getType()) {
                case INTEGER:
                    types[i] = Type.INTEGER;
                    final int iValue = flowVariable.getIntValue();
                    row.setCell(new CellImpl(iValue), i);
                    break;
                case DOUBLE:
                    types[i] = Type.DOUBLE;
                    final double dValue = flowVariable.getDoubleValue();
                    row.setCell(new CellImpl(dValue), i);
                    break;
                case STRING:
                    types[i] = Type.STRING;
                    final String sValue = flowVariable.getStringValue();
                    if (sValue != null) {
                        row.setCell(new CellImpl(sValue), i);
                    } else {
                        row.setCell(new CellImpl(), i);
                    }
                    break;
                default:
                    types[i] = Type.STRING;
                    final String defValue = flowVariable.getValueAsString();
                    if (defValue != null) {
                        row.setCell(new CellImpl(defValue), i);
                    } else {
                        row.setCell(new CellImpl(), i);
                    }
                    break;
            }
            i++;
        }
        final TableSpec spec = new TableSpecImpl(types, columnNames, new HashMap<String, String>());
        final TableIterator tableIterator = new KeyValueTableIterator(spec, row);
        try {
            return m_serializer.tableToBytes(tableIterator, m_kernelOptions.getSerializationOptions(),
                PythonCancelable.NOT_CANCELABLE);
        } catch (final PythonCanceledExecutionException ignore) {
            // Does not happen.
            throw new IllegalStateException("Implementation error.");
        }
    }

    @Override
    public Collection<FlowVariable> getFlowVariables(final String name) throws PythonIOException {
        try {
            final byte[] bytes = m_commands.getFlowVariables(name).get();
            return bytesToFlowVariables(bytes);
        } catch (final ExecutionException ex) {
            throw getMostSpecificPythonKernelException(ex);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    /**
     * Deserialize a collection of flow variables received from the python workspace.
     *
     * @param bytes the serialized representation of the flow variables
     * @return a collection of {@link FlowVariable}s
     * @throws PythonIOException If an error occurred while communicating with the python kernel or while executing the
     *             task
     */
    private Collection<FlowVariable> bytesToFlowVariables(final byte[] bytes) throws PythonIOException {
        try {
            final TableSpec spec = m_serializer.tableSpecFromBytes(bytes, PythonCancelable.NOT_CANCELABLE);
            final KeyValueTableCreator tableCreator = new KeyValueTableCreator(spec);
            m_serializer.bytesIntoTable(tableCreator, bytes, m_kernelOptions.getSerializationOptions(),
                PythonCancelable.NOT_CANCELABLE);
            // Use LinkedHashSet for preserving insertion order.
            final Set<FlowVariable> flowVariables = new LinkedHashSet<>();
            if (tableCreator.getTable() == null) {
                return flowVariables;
            }
            int i = 0;
            for (final Cell cell : tableCreator.getTable()) {
                final String columnName = tableCreator.getTableSpec().getColumnNames()[i++];
                switch (cell.getColumnType()) {
                    case INTEGER:
                        if (isValidFlowVariableName(columnName)) {
                            flowVariables.add(new FlowVariable(columnName, cell.getIntegerValue()));
                        }
                        break;
                    case DOUBLE:
                        if (isValidFlowVariableName(columnName)) {
                            flowVariables.add(new FlowVariable(columnName, cell.getDoubleValue()));
                        }
                        break;
                    case FLOAT:
                        if (isValidFlowVariableName(columnName)) {
                            flowVariables.add(new FlowVariable(columnName, cell.getFloatValue()));
                        }
                        break;
                    case STRING:
                        if (isValidFlowVariableName(columnName)) {
                            flowVariables.add(new FlowVariable(columnName, cell.getStringValue()));
                        }
                        break;
                    default:
                        break;
                }
            }
            return flowVariables;
        } catch (final PythonCanceledExecutionException ignore) {
            // Does not happen.
            throw new IllegalStateException("Implementation error.");
        }
    }

    /**
     * Check if input is a valid flow variable name.
     *
     * @param name a potential flow variable name
     * @return valid
     */
    private static boolean isValidFlowVariableName(final String name) {
        return !(name.startsWith(FlowVariable.Scope.Global.getPrefix()) ||
            name.startsWith(FlowVariable.Scope.Local.getPrefix()));
    }

    @Override
    public void putDataTable(final String name, final BufferedDataTable table, final ExecutionMonitor executionMonitor,
        final int rowLimit) throws PythonIOException, CanceledExecutionException {
        // TODO: Use #putData(..) internally.
        if (table == null) {
            throw new PythonIOException("Table " + name + " is not available.");
        }
        try {
            final PythonCancelable cancelable = new PythonExecutionMonitorCancelable(executionMonitor);
            final ExecutionMonitor serializationMonitor = executionMonitor.createSubProgress(0.5);
            final ExecutionMonitor deserializationMonitor = executionMonitor.createSubProgress(0.5);
            final int chunkSize = m_kernelOptions.getSerializationOptions().getChunkSize();
            try (final CloseableRowIterator iterator = table.iterator()) {
                if (table.size() > Integer.MAX_VALUE) {
                    throw new PythonIOException(
                        "Number of rows exceeds maximum of " + Integer.MAX_VALUE + " rows for input table!");
                }
                final int rowCount = (int)table.size();
                final int numberRows = Math.min(rowLimit, rowCount);
                int numberChunks = (int)Math.ceil(numberRows / (double)chunkSize);
                if (numberChunks == 0) {
                    numberChunks = 1;
                }
                int rowsDone = 0;
                final TableChunker tableChunker =
                    new BufferedDataTableChunker(table.getDataTableSpec(), iterator, rowCount);
                RunnableFuture<Void> putChunkTask = null;
                for (int i = 0; i < numberChunks; i++) {
                    final int rowsInThisIteration = Math.min(numberRows - rowsDone, chunkSize);
                    final ExecutionMonitor chunkProgress =
                        serializationMonitor.createSubProgress(rowsInThisIteration / (double)numberRows);
                    final TableIterator tableIterator =
                        ((BufferedDataTableChunker)tableChunker).nextChunk(rowsInThisIteration, chunkProgress);
                    final byte[] bytes =
                        m_serializer.tableToBytes(tableIterator, m_kernelOptions.getSerializationOptions(), cancelable);
                    chunkProgress.setProgress(1);
                    rowsDone += rowsInThisIteration;
                    serializationMonitor.setProgress(rowsDone / (double)numberRows);
                    if (i == 0) {
                        putChunkTask = m_commands.putTable(name, bytes);
                        putChunkTask.run();
                    } else {
                        waitForFutureCancelable(putChunkTask, cancelable);
                        deserializationMonitor.setProgress(rowsDone / (double)numberRows);
                        putChunkTask = m_commands.appendToTable(name, bytes);
                        putChunkTask.run();
                    }
                }
                waitForFutureCancelable(putChunkTask, cancelable);
                deserializationMonitor.setProgress(rowsDone / (double)numberRows);
            }
        } catch (final PythonCanceledExecutionException ex) {
            throw new CanceledExecutionException(ex.getMessage());
        } catch (final Exception ex) {
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    @Override
    public void putDataTable(final String name, final BufferedDataTable table, final ExecutionMonitor executionMonitor)
        throws PythonIOException, CanceledExecutionException {
        if (table.size() > Integer.MAX_VALUE) {
            throw new PythonIOException(
                "Number of rows exceeds maximum of " + Integer.MAX_VALUE + " rows for input table!");
        }
        putDataTable(name, table, executionMonitor, (int)table.size());
    }

    /**
     * Put the data underlying the given {@link TableChunker} into the workspace while still checking whether the
     * execution has been canceled.
     *
     * The data will be available as a pandas.DataFrame.
     *
     * @param name The name of the table
     * @param tableChunker A {@link TableChunker}
     * @param rowsPerChunk The number of rows to send per chunk
     * @param cancelable The cancelable to check if execution has been canceled
     * @throws PythonIOException If an error occurred while communicating with the python kernel or while executing the
     *             task
     * @throws PythonCanceledExecutionException if canceled. This instance must not be used after a cancellation
     *             occurred and must be {@link #close() closed}.
     */
    public void putData(final String name, final TableChunker tableChunker, final int rowsPerChunk,
        final PythonCancelable cancelable) throws PythonIOException, PythonCanceledExecutionException {
        try {
            final int numberRows = Math.min(rowsPerChunk, tableChunker.getNumberRemainingRows());
            final int chunkSize = m_kernelOptions.getSerializationOptions().getChunkSize();
            int numberChunks = (int)Math.ceil(numberRows / (double)chunkSize);
            if (numberChunks == 0) {
                numberChunks = 1;
            }
            int rowsDone = 0;
            RunnableFuture<Void> putChunkTask = null;
            for (int i = 0; i < numberChunks; i++) {
                final int rowsInThisIteration = Math.min(numberRows - rowsDone, chunkSize);
                final TableIterator tableIterator = tableChunker.nextChunk(rowsInThisIteration);
                final byte[] bytes =
                    m_serializer.tableToBytes(tableIterator, m_kernelOptions.getSerializationOptions(), cancelable);
                rowsDone += rowsInThisIteration;
                if (i == 0) {
                    putChunkTask = m_commands.putTable(name, bytes);
                    putChunkTask.run();
                } else {
                    waitForFutureCancelable(putChunkTask, cancelable);
                    putChunkTask = m_commands.appendToTable(name, bytes);
                    putChunkTask.run();
                }
            }
            waitForFutureCancelable(putChunkTask, cancelable);
        } catch (final PythonCanceledExecutionException ex) {
            throw ex;
        } catch (final Exception ex) {
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    @Override
    public BufferedDataTable getDataTable(final String name, final ExecutionContext exec,
        final ExecutionMonitor executionMonitor) throws PythonIOException, CanceledExecutionException {
        // TODO: Use #getData(..) internally.
        try {
            final PythonCancelable cancelable = new PythonExecutionMonitorCancelable(executionMonitor);
            final ExecutionMonitor serializationMonitor = executionMonitor.createSubProgress(0.5);
            final ExecutionMonitor deserializationMonitor = executionMonitor.createSubProgress(0.5);
            final int tableSize = m_commands.getTableSize(name).get();
            final int chunkSize = m_kernelOptions.getSerializationOptions().getChunkSize();
            int numberChunks = (int)Math.ceil(tableSize / (double)chunkSize);
            if (numberChunks == 0) {
                numberChunks = 1;
            }
            BufferedDataTableCreator tableCreator = null;
            for (int i = 0; i < numberChunks; i++) {
                final int start = chunkSize * i;
                final int end = Math.min(tableSize, (start + chunkSize) - 1);
                final byte[] bytes = waitForFutureCancelable(m_commands.getTableChunk(name, start, end), cancelable);
                serializationMonitor.setProgress((end + 1) / (double)tableSize);
                if (tableCreator == null) {
                    final TableSpec spec = m_serializer.tableSpecFromBytes(bytes, cancelable);
                    tableCreator = new BufferedDataTableCreator(spec, exec, deserializationMonitor, tableSize);
                }
                m_serializer.bytesIntoTable(tableCreator, bytes, m_kernelOptions.getSerializationOptions(), cancelable);
                deserializationMonitor.setProgress((end + 1) / (double)tableSize);
            }
            if (tableCreator != null) {
                return tableCreator.getTable();
            }
            throw new PythonIOException("Invalid serialized table received.");
        } catch (final PythonCanceledExecutionException ex) {
            throw new CanceledExecutionException(ex.getMessage());
        } catch (final Exception ex) {
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    /**
     * Get an object from the workspace while still checking whether the execution has been canceled.
     *
     * @param name The name of the object to get
     * @return The object
     * @param tableCreatorFactory A {@link TableCreatorFactory} that can be used to create the requested
     *            {@link TableCreator}
     * @param cancelable The cancelable to check if execution has been canceled
     * @throws PythonIOException If an error occurred while communicating with the python kernel or while executing the
     *             task
     * @throws PythonCanceledExecutionException if canceled. This instance must not be used after a cancellation
     *             occurred and must be {@link #close() closed}.
     */
    public TableCreator<?> getData(final String name, final TableCreatorFactory tableCreatorFactory,
        final PythonCancelable cancelable) throws PythonIOException, PythonCanceledExecutionException {
        try {
            final int tableSize = m_commands.getTableSize(name).get();
            final int chunkSize = m_kernelOptions.getSerializationOptions().getChunkSize();
            int numberChunks = (int)Math.ceil(tableSize / (double)chunkSize);
            if (numberChunks == 0) {
                numberChunks = 1;
            }
            TableCreator<?> tableCreator = null;
            for (int i = 0; i < numberChunks; i++) {
                final int start = chunkSize * i;
                final int end = Math.min(tableSize, (start + chunkSize) - 1);
                final byte[] bytes = waitForFutureCancelable(m_commands.getTableChunk(name, start, end), cancelable);
                if (tableCreator == null) {
                    final TableSpec spec = m_serializer.tableSpecFromBytes(bytes, cancelable);
                    tableCreator = tableCreatorFactory.createTableCreator(spec, tableSize);
                }
                m_serializer.bytesIntoTable(tableCreator, bytes, m_kernelOptions.getSerializationOptions(), cancelable);
            }
            return tableCreator;
        } catch (final PythonCanceledExecutionException ex) {
            throw ex;
        } catch (final Exception ex) {
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    @Override
    public void putObject(final String name, final PickledObjectFile object) throws PythonIOException {
        try {
            m_commands.putObject(name, object.getFile().getAbsolutePath()).get();
        } catch (InterruptedException | ExecutionException ex) {
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    @Override
    public void putObject(final String name, final PickledObjectFile object, final ExecutionMonitor executionMonitor)
        throws PythonIOException, CanceledExecutionException {
        try {
            PythonUtils.Misc.executeCancelable(() -> {
                putObject(name, object);
                return null;
            }, m_executorService::submit, new PythonExecutionMonitorCancelable(executionMonitor));
        } catch (final PythonCanceledExecutionException ex) {
            throw new CanceledExecutionException(ex.getMessage());
        } catch (final Exception ex) {
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    @Override
    public PickledObjectFile getObject(final String name, final File file, final ExecutionMonitor executionMonitor)
        throws PythonIOException, CanceledExecutionException {
        final PythonCancelable cancelable = new PythonExecutionMonitorCancelable(executionMonitor);
        try {
            Pair<String, String> typeAndRepresentation = PythonUtils.Misc.executeCancelable(
                () -> m_commands.getObject(name, file.getAbsolutePath()).get(), m_executorService::submit, cancelable);
            return new PickledObjectFile(file, typeAndRepresentation.getFirst(), typeAndRepresentation.getSecond());
        } catch (final PythonCanceledExecutionException ex) {
            throw new CanceledExecutionException(ex.getMessage());
        } catch (final Exception ex) {
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    /**
     * Send a "SQL-Table" to the python workspace that is used to connect to a database.
     *
     * @param name the name of the variable in the python workspace
     * @param conn the database connection to use
     * @param cp a credential provider for username and password
     * @param jars a list of jar files needed for invoking the jdbc driver on python side
     * @throws PythonIOException If an error occurred while communicating with the python kernel or while executing the
     *             task
     */
    public void putSql(final String name, final DatabaseQueryConnectionSettings conn, final CredentialsProvider cp,
        final Collection<String> jars) throws PythonIOException {
        final Type[] types = new Type[]{Type.STRING, Type.STRING, Type.STRING, Type.STRING, Type.STRING, Type.STRING,
            Type.INTEGER, Type.BOOLEAN, Type.STRING, Type.STRING_LIST};
        final String[] columnNames = new String[]{"driver", "jdbcurl", "username", "password", "query", "dbidentifier",
            "connectiontimeout", "autocommit", "timezone", "jars"};
        final RowImpl row = new RowImpl("0", 10);
        row.setCell(new CellImpl(conn.getDriver()), 0);
        row.setCell(new CellImpl(conn.getJDBCUrl()), 1);
        row.setCell(new CellImpl(conn.getUserName(cp)), 2);
        row.setCell(new CellImpl(conn.getPassword(cp)), 3);
        row.setCell(new CellImpl(conn.getQuery()), 4);
        row.setCell(new CellImpl(conn.getDatabaseIdentifier()), 5);
        row.setCell(new CellImpl(DatabaseConnectionSettings.getDatabaseTimeout()), 6);
        row.setCell(new CellImpl(false), 7);
        row.setCell(new CellImpl(conn.getTimezone()), 8);
        row.setCell(new CellImpl(jars.toArray(new String[jars.size()]), false), 9);
        final TableSpec spec = new TableSpecImpl(types, columnNames, new HashMap<String, String>());
        final TableIterator tableIterator = new KeyValueTableIterator(spec, row);
        try {
            final byte[] bytes = m_serializer.tableToBytes(tableIterator, m_kernelOptions.getSerializationOptions(),
                PythonCancelable.NOT_CANCELABLE);
            m_commands.putSql(name, bytes).get();
        } catch (final PythonCanceledExecutionException ignore) {
            // Does not happen.
            throw new IllegalStateException("Implementation error.");
        } catch (final Exception ex) {
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    /**
     * Gets a SQL query from the python workspace.
     *
     * @param name the name of the DBUtil variable in the python workspace
     * @return a SQL query string
     * @throws PythonIOException If an error occurred while communicating with the python kernel or while executing the
     *             task
     */
    public String getSql(final String name) throws PythonIOException {
        try {
            return m_commands.getSql(name).get();
        } catch (final Exception ex) {
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    @Override
    public ImageContainer getImage(final String name) throws PythonIOException {
        try {
            final byte[] bytes = m_commands.getImage(name).get();
            if (bytes != null) {
                return PythonKernelBackendUtils.createImage(() -> new ByteArrayInputStream(bytes));
            } else {
                return null;
            }
        } catch (final Exception ex) {
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    @Override
    public ImageContainer getImage(final String name, final ExecutionMonitor executionMonitor)
        throws PythonIOException, CanceledExecutionException {
        try {
            return PythonUtils.Misc.executeCancelable(() -> getImage(name), m_executorService::submit,
                new PythonExecutionMonitorCancelable(executionMonitor));
        } catch (final PythonCanceledExecutionException ex) {
            throw new CanceledExecutionException(ex.getMessage());
        } catch (final Exception ex) {
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    @Override
    public List<Map<String, String>> listVariables() throws PythonIOException {
        try {
            final byte[] bytes = m_commands.listVariables().get();
            final TableSpec spec = m_serializer.tableSpecFromBytes(bytes, PythonCancelable.NOT_CANCELABLE);
            final TemporaryTableCreator tableCreator = new TemporaryTableCreator(spec);
            m_serializer.bytesIntoTable(tableCreator, bytes, m_kernelOptions.getSerializationOptions(),
                PythonCancelable.NOT_CANCELABLE);
            final int nameIndex = spec.findColumn("name");
            final int typeIndex = spec.findColumn("type");
            final int valueIndex = spec.findColumn("value");
            final List<Map<String, String>> variables = new ArrayList<>();
            for (final Row variable : tableCreator.getTable()) {
                final Map<String, String> map = new HashMap<>();
                map.put("name", variable.getCell(nameIndex).getStringValue());
                map.put("type", variable.getCell(typeIndex).getStringValue());
                map.put("value", variable.getCell(valueIndex).getStringValue());
                variables.add(map);
            }
            return variables;
        } catch (final PythonCanceledExecutionException ignore) {
            // Does not happen.
            throw new IllegalStateException("Implementation error.");
        } catch (final Exception ex) {
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    @Override
    public List<Map<String, String>> autoComplete(final String sourceCode, final int line, final int column)
        throws PythonIOException {
        try {
            final List<Map<String, String>> suggestions = new ArrayList<>();
            if (m_hasAutocomplete) {
                final byte[] bytes = m_commands.autoComplete(sourceCode, line, column).get();
                final TableSpec spec = m_serializer.tableSpecFromBytes(bytes, PythonCancelable.NOT_CANCELABLE);
                final TemporaryTableCreator tableCreator = new TemporaryTableCreator(spec);
                m_serializer.bytesIntoTable(tableCreator, bytes, m_kernelOptions.getSerializationOptions(),
                    PythonCancelable.NOT_CANCELABLE);
                final int nameIndex = spec.findColumn("name");
                final int typeIndex = spec.findColumn("type");
                final int docIndex = spec.findColumn("doc");
                for (final Row suggestion : tableCreator.getTable()) {
                    final Map<String, String> map = new HashMap<>();
                    map.put("name", suggestion.getCell(nameIndex).getStringValue());
                    map.put("type", suggestion.getCell(typeIndex).getStringValue());
                    map.put("doc", suggestion.getCell(docIndex).getStringValue());
                    suggestions.add(map);
                }
            }
            return suggestions;
        } catch (final PythonCanceledExecutionException ex) {
            // Does not happen.
            throw new IllegalStateException("Implementation error.");
        } catch (final Exception ex) {
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    /**
     * Returns a task that executes the given source code and handles messages coming from Python that concern the
     * execution of the source code using the given handler.
     *
     * @param handler the handler that handles the source code execution
     * @param sourceCode the source code to execute
     *
     * @return the result obtained by the handler
     */
    public <T> RunnableFuture<T> createExecutionTask(final TaskHandler<T> handler, final String sourceCode) {
        // TODO: Execution & warning listeners, see #execute(String).
        return m_commands.createTask(handler, m_commands.createExecuteCommand(sourceCode));
    }

    @Override
    public String[] execute(final String sourceCode) throws PythonIOException {
        return executeCommand(m_commands.execute(sourceCode));
    }

    @Override
    public String[] execute(final String sourceCode, final PythonCancelable cancelable)
        throws PythonIOException, CanceledExecutionException {
        return executeCommandCancelable(() -> execute(sourceCode), cancelable);
    }

    @Override
    public String[] executeAsync(final String sourceCode) throws PythonIOException {
        return executeCommand(m_commands.executeAsync(sourceCode));
    }

    @Override
    public String[] executeAsync(final String sourceCode, final PythonCancelable cancelable)
        throws PythonIOException, CanceledExecutionException {
        return executeCommandCancelable(() -> executeAsync(sourceCode), cancelable);
    }

    private String[] executeCommand(final RunnableFuture<String[]> executeCommand) throws PythonIOException {
        try {
            return executeCommand.get();
        } catch (final Exception ex) {
            final PythonIOException exception = getMostSpecificPythonKernelException(ex);
            // Append Python trace back to error message to allow the user to instantly see the faulty parts of their
            // code.
            final Optional<String> formattedPythonTracebackOptional = exception.getFormattedPythonTraceback();
            if (formattedPythonTracebackOptional.isPresent()) {
                final String formattedPythonTraceback = formattedPythonTracebackOptional.get();
                // Strip the lines of the trace back that do not refer to user code but kernel code.
                String beautifiedPythonTraceback = null;
                // Keep first line which is the trace back heading.
                final int headingEndIndex = formattedPythonTraceback.indexOf('\n');
                if (headingEndIndex != -1) {
                    final String heading = formattedPythonTraceback.substring(0, headingEndIndex);
                    final int userCodeBeginIndex = formattedPythonTraceback.indexOf("File \"<string>\"");
                    if (userCodeBeginIndex != -1) {
                        beautifiedPythonTraceback =
                            heading + "\n" + formattedPythonTraceback.substring(userCodeBeginIndex);
                    }
                }
                if (beautifiedPythonTraceback == null) {
                    // Fall trough
                    beautifiedPythonTraceback = formattedPythonTraceback;
                }
                throw new PythonIOException(exception.getMessage() + "\n" + beautifiedPythonTraceback,
                    exception.getMessage(), formattedPythonTraceback, exception.getPythonTraceback().orElse(null));
            } else {
                throw exception;
            }
        }
    }

    private String[] executeCommandCancelable(final Callable<String[]> executeCommand,
        final PythonCancelable cancelable) throws PythonIOException, CanceledExecutionException {
        try {
            return PythonUtils.Misc.executeCancelable(executeCommand, m_executorService::submit, cancelable);
        } catch (final PythonCanceledExecutionException ex) {
            throw new CanceledExecutionException(ex.getMessage());
        }
    }

    @Override
    public void close() throws PythonKernelCleanupException {
        if (m_closed.compareAndSet(false, true)) {
            // Closing the database connections must be done synchronously. Otherwise Python database testflows fail
            // because the test framework's database janitors try to clean up the databases before the connections are
            // closed. Exceptions that occur during cleanup should be propagated to the user since external resources
            // (e.g., databases) could be affected.
            PythonKernelCleanupException cleanupException = null;
            try {
                if (m_commands != null) {
                    m_commands.cleanUp().get(getCleanupTimeoutInMillis(), TimeUnit.MILLISECONDS);
                }
            } catch (TimeoutException ex) {
                cleanupException = new PythonKernelCleanupException("An attempt to clean up Python timed out. " +
                    "Please consider increasing the cleanup timeout using the VM option '-D" + CLEANUP_TIMEOUT_VM_OPT +
                    "=<value-in-ms>'.", ex);
            } catch (Throwable t) {
                t = PythonUtils.Misc.unwrapExecutionException(t).orElse(t);
                cleanupException =
                    new PythonKernelCleanupException("Failed to clean up Python. See log for details.", t);
            }

            // Async. closing.
            new Thread(() -> {
                PythonUtils.Misc.closeSafely(LOGGER::debug, m_outputListeners);
                PythonUtils.Misc.invokeSafely(LOGGER::debug, ExecutorService::shutdownNow, m_executorService);
                PythonUtils.Misc.closeSafely(LOGGER::debug, m_commands, m_serverSocket, m_socket, m_serializer);
                // If the original process was a script, we have to kill the actual Python process by PID.
                if (m_pid != null) {
                    try {
                        ProcessBuilder pb;
                        if (System.getProperty("os.name").toLowerCase().contains("win")) {
                            pb = new ProcessBuilder("taskkill", "/F", "/PID", "" + m_pid);
                        } else {
                            pb = new ProcessBuilder("kill", "-KILL", "" + m_pid);
                        }
                        final Process p = pb.start();
                        p.waitFor();
                    } catch (final InterruptedException ex) {
                        // Closing the kernel should not be interrupted.
                        Thread.currentThread().interrupt();
                    } catch (final Exception ignore) {
                        // Ignore.
                    }
                }
                if (m_process != null) {
                    m_process.destroy();
                }
            }).start();

            // (Re-)Throw exception after the rest of the kernel shutdown was initiated.
            if (cleanupException != null) {
                throw cleanupException;
            }
        }
    }

    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }

    /**
     * Grants access to the underlying command-based communication mechanism, only used in tests.
     *
     * @return The command mechanism.
     * @noreference Not to be used by 3rd party code.
     */
    public PythonCommands getCommands() {
        return m_commands;
    }

    private PythonIOException getMostSpecificPythonKernelException(final Exception exception) {
        // Unwrap exceptions that occurred during any async execution.
        final Throwable exc = PythonUtils.Misc.unwrapExecutionException(exception).orElse(exception);
        if (exc instanceof PythonIOException) {
            return (PythonIOException)exc;
        }
        if (exc instanceof PythonException) {
            return new PythonIOException(exc.getMessage(), exc);
        }
        // No known exception. Could be caused by ungraceful process termination due to segfault.
        if (!m_process.isAlive()) {
            final int exitCode = m_process.exitValue();
            // Arrow and CSV exit with segfault (exit code 139) on oversized buffer allocation,
            // flatbuffers with exit code 0.
            if (exitCode == 139) {
                return new PythonIOException(
                    "Python process ended unexpectedly with a SEGFAULT. This might be caused by" +
                        " an oversized buffer allocation. Please consider lowering the 'Rows per chunk' parameter in" +
                        " the 'Options' tab of the configuration dialog.");
            }
        }
        return new PythonIOException(exc);
    }

    private <T> T waitForFutureCancelable(final Future<T> future, final PythonCancelable cancelable)
        throws PythonIOException, PythonCanceledExecutionException {
        try {
            return PythonUtils.Misc.executeCancelable(future::get, m_executorService::submit, cancelable);
        } catch (final PythonCanceledExecutionException ex) {
            future.cancel(true);
            throw ex;
        }
    }

    private static final class PythonKernelExecutionMonitor implements PythonExecutionMonitor {

        private static final Message POISON_PILL = new DefaultMessage(1, "", null, null);

        private final List<Exception> m_reportedExceptions = Collections.synchronizedList(new ArrayList<>());

        @Override
        public Message getPoisonPill() {
            return POISON_PILL;
        }

        @Override
        public void reportException(final Exception exception) {
            m_reportedExceptions.add(exception);
        }

        @Override
        public void checkExceptions() throws Exception {
            synchronized (m_reportedExceptions) {
                if (!m_reportedExceptions.isEmpty()) {
                    final Iterator<Exception> i = m_reportedExceptions.iterator();
                    final Exception ex = i.next();
                    while (i.hasNext()) {
                        ex.addSuppressed(i.next());
                    }
                    throw ex;
                }
            }
        }
    }
}
