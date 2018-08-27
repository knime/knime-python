/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 *
 * History
 *   Sep 25, 2014 (Patrick Winter): created
 */
package org.knime.python2.kernel;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.imageio.ImageIO;

import org.apache.batik.dom.svg.SAXSVGDocumentFactory;
import org.apache.batik.transcoder.TranscoderException;
import org.apache.batik.util.XMLResourceDescriptor;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.util.ThreadUtils;
import org.knime.python.typeextension.KnimeToPythonExtension;
import org.knime.python.typeextension.KnimeToPythonExtensions;
import org.knime.python.typeextension.PythonModuleExtensions;
import org.knime.python.typeextension.PythonToKnimeExtension;
import org.knime.python.typeextension.PythonToKnimeExtensions;
import org.knime.python2.Activator;
import org.knime.python2.PythonKernelTester;
import org.knime.python2.PythonKernelTester.PythonKernelTestResult;
import org.knime.python2.PythonPreferencePage;
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
import org.knime.python2.generic.ScriptingNodeUtils;
import org.knime.python2.kernel.messaging.AbstractRequestHandler;
import org.knime.python2.kernel.messaging.DefaultMessage;
import org.knime.python2.kernel.messaging.DefaultMessage.PayloadDecoder;
import org.knime.python2.kernel.messaging.DefaultMessage.PayloadEncoder;
import org.knime.python2.kernel.messaging.Message;
import org.knime.python2.kernel.messaging.TaskHandler;
import org.knime.python2.port.PickledObject;
import org.knime.python2.util.PythonUtils;
import org.w3c.dom.svg.SVGDocument;

/**
 * Provides operations on a Python kernel running in another process.
 *
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public class PythonKernel implements AutoCloseable {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonKernel.class);

    private static final int WAIT_TIMEOUT_MILLISECONDS = 1000;

    /**
     * @return the duration, in milliseconds, to wait when trying to establish a connection to Python
     */
    public static int getConnectionTimeoutInMillis() {
        final String defaultTimeout = "30000";
        try {
            final String timeout = System.getProperty("knime.python.connecttimeout", defaultTimeout);
            return Integer.parseInt(timeout);
        } catch (final NumberFormatException ex) {
            LOGGER.warn(
                "The VM option -Dknime.python.connecttimeout is set to a non-integer value. The connecttimeout is "
                    + "set to the default value of " + defaultTimeout + " ms.");
            return Integer.parseInt(defaultTimeout);
        }
    }

    private final PythonKernelOptions m_kernelOptions;

    private final Process m_process;

    private final Integer m_pid; // Nullable.

    private final ServerSocket m_serverSocket;

    private final Socket m_socket;

    private final PythonCommands m_commands;

    private final SerializationLibrary m_serializer;

    private final InputStream m_stdoutStream;

    private final InputStream m_stderrStream;

    private final List<PythonOutputListener> m_stdoutListeners = new ArrayList<>();

    private final List<PythonOutputListener> m_stderrListeners = new ArrayList<>();

    private final PythonOutputListener m_defaultStdoutListener;

    private final ConfigurablePythonOutputListener m_defaultStderrListener;

    private final List<ProcessEndAction> m_processEndActions = new ArrayList<>();;

    private final ProcessEndAction m_segfaultDuringSerializationAction;

    private final Future<PythonIOException> m_pythonKernelMonitorResult;

    private final boolean m_hasAutocomplete;

    private final AtomicBoolean m_closed = new AtomicBoolean(false);

    // TODO use this executor service where possible
    // TODO can we use a multithread pool?
    private final ExecutorService m_executorService = Executors.newSingleThreadExecutor();

    /**
     * Creates a new Python kernel by starting a Python process and connecting to it.
     * <P>
     * Important: Call the {@link #close()} method when this kernel is no longer needed to shut down the Python process
     * in the background.
     *
     * @param kernelOptions all configurable options
     * @throws IOException if failed to setup the Python kernel
     */
    public PythonKernel(final PythonKernelOptions kernelOptions) throws IOException {
        m_kernelOptions = kernelOptions;

        testInstallation();

        try {
            // Setup Python kernel:

            // Create serialization library instance.
            m_serializer = setupSerializationLibrary();

            // Start socket creation. The created socket is used to communicate with the Python process that is created below.
            m_serverSocket = new ServerSocket(0);
            m_serverSocket.setSoTimeout(getConnectionTimeoutInMillis());
            final Future<Socket> socketBeingSetup = setupSocket();

            // Create Python process.
            m_process = setupPythonProcess();

            // Start listening to stdout and stderror pipes.
            m_stdoutStream = m_process.getInputStream();
            m_stderrStream = m_process.getErrorStream();
            startPipeListeners();

            // Monitor process termination and report possible errors.
            m_segfaultDuringSerializationAction = setupSegfaultAction(); // Used in "getData" methods.
            m_pythonKernelMonitorResult = setupProcessEndActions();

            // Log output and errors to console.
            m_defaultStdoutListener = new PythonOutputListener() {

                private boolean m_silenced = false;

                @Override
                public void setSilenced(final boolean silenced) {
                    m_silenced = silenced;
                }

                @Override
                public void messageReceived(final String message) {
                    if (!m_silenced) {
                        LOGGER.info(message);
                    } else {
                        LOGGER.debug(message);
                    }
                }
            };
            addStdoutListener(m_defaultStdoutListener);

            m_defaultStderrListener = new ConfigurablePythonOutputListener();
            addStderrorListener(m_defaultStderrListener);

            try {
                // Wait for Python to connect.
                m_socket = socketBeingSetup.get();
            } catch (final ExecutionException e) {
                if (e.getCause() instanceof SocketTimeoutException) {
                    // Under some circumstances, the Python process may crash while we're trying to establish a socket
                    // connection which causes the attempt to time out. We should not misinterpret that as a real time out.
                    if (!isPythonProcessAlive()) {
                        throw new PythonIOException(
                            "The external Python process crashed for unknown reasons while KNIME "
                                + "set up the Python environment. See log for details.",
                            e);
                    } else {
                        throw new PythonIOException(
                            "The connection attempt timed out. Please consider increasing the socket "
                                + "timeout using the VM option '-Dknime.python.connecttimeout=<value-in-ms>'.\n"
                                + "Also make sure that the communication between KNIME and Python is not blocked by a "
                                + "firewall and that your hosts configuration is correct.",
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
            setupSerializerRequestHandlers();

            // Start commands/messaging system once everything is set up.
            m_commands.start();

            // PID of Python process.
            m_pid = m_commands.getPid().get();
            LOGGER.debug("Python PID: " + m_pid);

            m_hasAutocomplete = checkHasAutoComplete();

            // Add custom module directories to the PYTHONPATH in the Python workspace.
            setupCustomModules();

            setupSentinelConstants();
        } catch (Throwable t) {
            // Close is not called by try-with-resources if an exception occurs during construction.
            close();

            // Unwrap exception that occurred during any async setup.
            if (t instanceof ExecutionException && t.getCause() != null) {
                t = t.getCause();
            }

            if (t instanceof PythonIOException) {
                throw (PythonIOException)t;
            } else if (t instanceof Error) {
                throw (Error)t;
            } else {
                throw new IOException("An exception occurred while setting up the Python kernel. " //
                    + "See log for details.", t);
            }
        }
    }

    // Setup methods:

    private void testInstallation() throws PythonIOException {
        final PythonKernelTestResult testResult = m_kernelOptions.getUsePython3()
            ? PythonKernelTester.testPython3Installation(m_kernelOptions.getAdditionalRequiredModules(), false)
            : PythonKernelTester.testPython2Installation(m_kernelOptions.getAdditionalRequiredModules(), false);
        if (testResult.hasError()) {
            throw new PythonIOException("Could not start Python kernel. Error during Python installation test: "
                + testResult.getErrorLog() + ". See log for details.");
        }
    }

    private SerializationLibrary setupSerializationLibrary() throws PythonIOException {
        final SerializationLibraryExtensions serializationLibraryExtension = new SerializationLibraryExtensions();
        final String serializerId = getSerializerId();
        final String serializerName = SerializationLibraryExtensions.getNameForId(serializerId);
        if (serializerName == null) {
            final String message;
            if (serializerId == null) {
                message =
                    "No serialization library was found. Please make sure to install at least one plugin containing one.";
            } else {
                message = "The selected serialization library with id " + serializerId + " was not found. "
                    + "Please make sure to install the correspondent plugin or select a different serialization "
                    + "library in the Python preference page.";
            }
            throw new PythonIOException(message);
        }
        LOGGER.debug("Using serialization library: " + serializerName + ".");
        return serializationLibraryExtension.getSerializationLibrary(serializerId);
    }

    /**
     * Get the id of the configured serialization library.
     *
     * @return a serialization library id
     */
    private String getSerializerId() {
        if (m_kernelOptions.getOverrulePreferencePage()) {
            return m_kernelOptions.getSerializerId();
        }
        return PythonPreferencePage.getSerializerId();
    }

    private Future<Socket> setupSocket() {
        return Executors.newSingleThreadExecutor().submit(m_serverSocket::accept);
    }

    private Process setupPythonProcess() throws IOException {
        final String kernelScriptPath = m_kernelOptions.getKernelScriptPath();
        final String port = Integer.toString(m_serverSocket.getLocalPort());
        final String serializationLibraryPath =
            SerializationLibraryExtensions.getSerializationLibraryPath(getSerializerId());
        // Start Python kernel that listens to the given port.
        // Use the -u options to force Python to not buffer stdout and stderror.
        final ProcessBuilder pb;
        if (!m_kernelOptions.getUsePython3()) {
            // Python2 start without site to set default encoding to utf-8.
            pb = new ProcessBuilder(Activator.getPython2Command(), "-u", /*"-S",*/ kernelScriptPath, port,
                serializationLibraryPath);
        } else {
            pb = new ProcessBuilder(Activator.getPython3Command(), "-u", kernelScriptPath, port,
                serializationLibraryPath);
        }
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

        pb.redirectOutput(ProcessBuilder.Redirect.PIPE);
        pb.redirectError(ProcessBuilder.Redirect.PIPE);

        // Start Python.
        return pb.start();
    }

    private ProcessEndAction setupSegfaultAction() {
        return exitCode -> {
            // If an error was raised in Python we already have a specific clue about the error. If not we should
            // have a look at the exit code of the process.
            if (m_defaultStderrListener.wasErrorLogged()) {
                return;
            }
            // Arrow and CSV exit with segfault (exit code 139) on oversized buffer allocation,
            // flatbuffers with exit code 0.
            if (exitCode == 139) {
                throw new PythonIOException("Python process ended unexpectedly with a SEGFAULT. This might be caused by"
                    + " an oversized buffer allocation. Please consider lowering the 'Rows per chunk' parameter in"
                    + " the 'Options' tab of the configuration dialog.");
            } else if (exitCode != 0) {
                throw new PythonIOException(
                    "Python process ended unexpectedly with exit code " + exitCode + ". This might be"
                        + " caused by an oversized buffer allocation. Please consider lowering the 'Rows per chunk'"
                        + " parameter in the 'Options' tab of the configuration dialog.");
            }
        };
    }

    private Future<PythonIOException> setupProcessEndActions() {
        // Capture process end and run registered actions.
        final ExecutorService service = Executors.newSingleThreadExecutor();
        return service.submit(() -> {
            final int exitCode = m_process.waitFor();
            synchronized (m_processEndActions) {
                for (final ProcessEndAction action : m_processEndActions) {
                    action.runForExitCode(exitCode);
                }
            }
            return null;
        });
    }

    private void setupSerializerRequestHandlers() {
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
    }

    private boolean checkHasAutoComplete() {
        try {
            // Check if Python kernel supports auto-completion (this depends on the optional module Jedi).
            return m_commands.hasAutoComplete().get();
        } catch (final Exception ex) {
            LOGGER.debug("An exception occurred while checking the auto-completion capabilities of the Python kernel. "
                + "Auto-completion will not be available.");
            return false;
        }
    }

    private void setupCustomModules() throws InterruptedException, ExecutionException {
        final String pythonpath = PythonModuleExtensions.getPythonPath();
        if (!pythonpath.isEmpty()) {
            m_commands.addToPythonPath(pythonpath).get();
        }
    }

    private void setupSentinelConstants() throws InterruptedException, ExecutionException {
        if (m_kernelOptions.getSentinelOption() == SentinelOption.MAX_VAL) {
            m_commands.execute("INT_SENTINEL = 2**31 - 1; LONG_SENTINEL = 2**63 - 1").get();
        } else if (m_kernelOptions.getSentinelOption() == SentinelOption.MIN_VAL) {
            m_commands.execute("INT_SENTINEL = -2**31; LONG_SENTINEL = -2**63").get();
        } else {
            m_commands.execute("INT_SENTINEL = " + m_kernelOptions.getSentinelValue() + "; LONG_SENTINEL = "
                + m_kernelOptions.getSentinelValue()).get();
        }
    }

    private boolean isPythonProcessAlive() {
        return m_process != null && m_process.isAlive();
    }
    // End of setup methods.

    /**
     * Registers the given handler for the given task category if it is not yet covered by another handler.
     *
     * @param taskCategory the {@link Message#getCategory() category} for which to register the handler
     * @param handler the handler to register
     * @return <code>true</code> if the task handler could be registered, <code>false</code> otherwise
     */
    public boolean registerTaskHandler(final String taskCategory, final TaskHandler<?> handler) {
        return m_commands.getMessageHandlers().registerMessageHandler(taskCategory,
            m_commands.createTaskFactory(handler));
    }

    /**
     * Unregisters the handler for the given task category if one is present.
     *
     * @param taskCategory the {@link Message#getCategory() category} for which to unregister the handler
     * @return <code>true</code> if a task handler had been present and was unregistered, <code>false</code> otherwise
     */
    public boolean unregisterTaskHandler(final String taskCategory) {
        return m_commands.getMessageHandlers().unregisterMessageHandler(taskCategory);
    }

    /**
     * Add an action to run when the python process ends.
     *
     * @param ac the action to run
     */
    public void addProcessEndAction(final ProcessEndAction ac) {
        synchronized (m_processEndActions) {
            m_processEndActions.add(ac);
        }
    }

    /**
     * Remove an action from the list of actions to run when the python process ends.
     *
     * @param ac the action to remove
     */
    public void removeProcessEndAction(final ProcessEndAction ac) {
        synchronized (m_processEndActions) {
            m_processEndActions.remove(ac);
        }
    }

    /**
     * Put the given flow variables into the workspace.
     *
     * The given flow variables will be available as a dict with the given name
     *
     * @param name The name of the dict
     * @param flowVariables The flow variables to put
     * @throws IOException If an error occurred while communicating with the python kernel or while executing the task
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void putFlowVariables(final String name, final Collection<FlowVariable> flowVariables)
        throws IOException, InterruptedException, ExecutionException {
        final byte[] bytes = flowVariablesToBytes(flowVariables);
        m_commands.putFlowVariables(name, bytes).get();
    }

    /**
     * Serialize a collection of flow variables to a {@link Row}.
     *
     * @param flowVariables
     * @return the serialized flow variables
     * @throws IOException If an error occurred while communicating with the python kernel or while executing the task
     */
    private byte[] flowVariablesToBytes(final Collection<FlowVariable> flowVariables) throws IOException {
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
                    row.setCell(new CellImpl(sValue), i);
                    break;
                default:
                    types[i] = Type.STRING;
                    final String defValue = flowVariable.getValueAsString();
                    row.setCell(new CellImpl(defValue), i);
                    break;
            }
            i++;
        }
        final TableSpec spec = new TableSpecImpl(types, columnNames, new HashMap<String, String>());
        final TableIterator tableIterator = new KeyValueTableIterator(spec, row);
        return m_serializer.tableToBytes(tableIterator, m_kernelOptions.getSerializationOptions());
    }

    /**
     * Returns the list of defined flow variables
     *
     * @param name Variable name of the flow variable dict in Python
     * @return Collection of flow variables
     * @throws IOException If an error occurred while communicating with the python kernel or while executing the task
     */
    public Collection<FlowVariable> getFlowVariables(final String name) throws IOException {
        try {
            final byte[] bytes = m_commands.getFlowVariables(name).get();
            return bytesToFlowVariables(bytes);
        } catch (EOFException | InterruptedException | ExecutionException ex) {
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    /**
     * Deserialize a collection of flow variables received from the python workspace.
     *
     * @param bytes the serialized representation of the flow variables
     * @return a collection of {@link FlowVariable}s
     * @throws IOException If an error occurred while communicating with the python kernel or while executing the task
     */
    private Collection<FlowVariable> bytesToFlowVariables(final byte[] bytes) throws IOException {
        final TableSpec spec = m_serializer.tableSpecFromBytes(bytes);
        final KeyValueTableCreator tableCreator = new KeyValueTableCreator(spec);
        m_serializer.bytesIntoTable(tableCreator, bytes, m_kernelOptions.getSerializationOptions());
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
    }

    /**
     * Check if input is a valid flow variable name.
     *
     * @param name a potential flow variable name
     * @return valid
     */
    private boolean isValidFlowVariableName(final String name) {
        return !(name.startsWith(FlowVariable.Scope.Global.getPrefix())
            || name.startsWith(FlowVariable.Scope.Local.getPrefix()));
    }

    /**
     * Put the given {@link BufferedDataTable} into the workspace.
     *
     * The table will be available as a pandas.DataFrame.
     *
     * @param name The name of the table
     * @param table The table
     * @param executionMonitor The monitor that will be updated about progress
     * @param rowLimit The amount of rows that will be transfered
     * @throws IOException If an error occurred while communicating with the python kernel or while executing the task
     */
    public void putDataTable(final String name, final BufferedDataTable table, final ExecutionMonitor executionMonitor,
        final int rowLimit) throws IOException {
        if (table == null) {
            throw new IOException("Table " + name + " is not available.");
        }
        final ExecutionMonitor serializationMonitor = executionMonitor.createSubProgress(0.5);
        final ExecutionMonitor deserializationMonitor = executionMonitor.createSubProgress(0.5);
        try (final CloseableRowIterator iterator = table.iterator()) {
            if (table.size() > Integer.MAX_VALUE) {
                throw new IOException(
                    "Number of rows exceeds maximum of " + Integer.MAX_VALUE + " rows for input table!");
            }
            final int rowCount = (int)table.size();
            final int numberRows = Math.min(rowLimit, rowCount);
            int numberChunks = (int)Math.ceil(numberRows / (double)m_kernelOptions.getChunkSize());
            if (numberChunks == 0) {
                numberChunks = 1;
            }
            int rowsDone = 0;
            final TableChunker tableChunker =
                new BufferedDataTableChunker(table.getDataTableSpec(), iterator, rowCount);
            Future<Void> lastChunkCompleted = null;
            for (int i = 0; i < numberChunks; i++) {
                final int rowsInThisIteration = Math.min(numberRows - rowsDone, m_kernelOptions.getChunkSize());
                final ExecutionMonitor chunkProgress =
                    serializationMonitor.createSubProgress(rowsInThisIteration / (double)numberRows);
                final TableIterator tableIterator =
                    ((BufferedDataTableChunker)tableChunker).nextChunk(rowsInThisIteration, chunkProgress);
                final byte[] bytes =
                    m_serializer.tableToBytes(tableIterator, m_kernelOptions.getSerializationOptions());
                chunkProgress.setProgress(1);
                rowsDone += rowsInThisIteration;
                serializationMonitor.setProgress(rowsDone / (double)numberRows);
                if (i == 0) {
                    lastChunkCompleted = m_commands.putTable(name, bytes);
                } else {
                    lastChunkCompleted.get();
                    lastChunkCompleted = m_commands.appendToTable(name, bytes);
                }
                deserializationMonitor.setProgress(rowsDone / (double)numberRows);
                try {
                    executionMonitor.checkCanceled();
                } catch (final CanceledExecutionException ex) {
                    throw new IOException(ex.getMessage(), ex);
                }
            }
            lastChunkCompleted.get();
        } catch (InterruptedException | ExecutionException ex) {
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    /**
     * Put the given {@link BufferedDataTable} into the workspace.
     *
     * The table will be available as a pandas.DataFrame.
     *
     * @param name The name of the table
     * @param table The table
     * @param executionMonitor The monitor that will be updated about progress
     * @throws IOException If an error occurred while communicating with the python kernel or while executing the task
     */
    public void putDataTable(final String name, final BufferedDataTable table, final ExecutionMonitor executionMonitor)
        throws IOException {
        if (table.size() > Integer.MAX_VALUE) {
            throw new IOException("Number of rows exceeds maximum of " + Integer.MAX_VALUE + " rows for input table!");
        }
        putDataTable(name, table, executionMonitor, (int)table.size());
    }

    /**
     * Put the data underlying the given {@link TableChunker} into the workspace.
     *
     * The data will be available as a pandas.DataFrame.
     *
     * @param name The name of the table
     * @param tableChunker A {@link TableChunker}
     * @param rowsPerChunk The number of rows to send per chunk
     * @throws IOException If an error occurred while communicating with the python kernel or while executing the task
     */
    public void putData(final String name, final TableChunker tableChunker, final int rowsPerChunk) throws IOException {
        final int numberRows = Math.min(rowsPerChunk, tableChunker.getNumberRemainingRows());
        int numberChunks = (int)Math.ceil(numberRows / (double)m_kernelOptions.getChunkSize());
        if (numberChunks == 0) {
            numberChunks = 1;
        }
        int rowsDone = 0;
        Future<Void> lastChunkCompleted = null;
        try {
            for (int i = 0; i < numberChunks; i++) {
                final int rowsInThisIteration = Math.min(numberRows - rowsDone, m_kernelOptions.getChunkSize());
                final TableIterator tableIterator = tableChunker.nextChunk(rowsInThisIteration);
                final byte[] bytes =
                    m_serializer.tableToBytes(tableIterator, m_kernelOptions.getSerializationOptions());
                rowsDone += rowsInThisIteration;
                if (i == 0) {
                    lastChunkCompleted = m_commands.putTable(name, bytes);
                } else {
                    lastChunkCompleted.get();
                    lastChunkCompleted = m_commands.appendToTable(name, bytes);
                }
            }
            lastChunkCompleted.get();
        } catch (InterruptedException | ExecutionException ex) {
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    /**
     * Get a {@link BufferedDataTable} from the workspace.
     *
     * @param name The name of the table to get
     * @param exec The calling node's execution context
     * @return The table
     * @param executionMonitor The monitor that will be updated about progress
     * @throws IOException If an error occurred while communicating with the python kernel or while executing the task
     *
     */
    public BufferedDataTable getDataTable(final String name, final ExecutionContext exec,
        final ExecutionMonitor executionMonitor) throws IOException {
        final ExecutionMonitor serializationMonitor = executionMonitor.createSubProgress(0.5);
        final ExecutionMonitor deserializationMonitor = executionMonitor.createSubProgress(0.5);
        final ProcessEndAction pea = m_segfaultDuringSerializationAction;
        m_defaultStderrListener.resetErrorLoggedFlag();
        try {
            addProcessEndAction(pea);
            final int tableSize = m_commands.getTableSize(name).get();
            int numberChunks = (int)Math.ceil(tableSize / (double)m_kernelOptions.getChunkSize());
            if (numberChunks == 0) {
                numberChunks = 1;
            }
            BufferedDataTableCreator tableCreator = null;
            for (int i = 0; i < numberChunks; i++) {
                final int start = m_kernelOptions.getChunkSize() * i;
                final int end = Math.min(tableSize, (start + m_kernelOptions.getChunkSize()) - 1);
                final byte[] bytes = m_commands.getTableChunk(name, start, end).get();
                serializationMonitor.setProgress((end + 1) / (double)tableSize);
                if (tableCreator == null) {
                    final TableSpec spec = m_serializer.tableSpecFromBytes(bytes);
                    tableCreator = new BufferedDataTableCreator(spec, exec, deserializationMonitor, tableSize);
                }
                m_serializer.bytesIntoTable(tableCreator, bytes, m_kernelOptions.getSerializationOptions());
                deserializationMonitor.setProgress((end + 1) / (double)tableSize);
            }
            if (tableCreator != null) {
                final BufferedDataTable table = tableCreator.getTable();
                return table;
            }
            throw new PythonIOException("Invalid serialized table received.");
        } catch (final InterruptedException | ExecutionException ex) {
            throw getMostSpecificPythonKernelException(ex);
        } finally {
            removeProcessEndAction(pea);
        }
    }

    /**
     * Get an object from the workspace.
     *
     * @param name The name of the object to get
     * @return The object
     * @param tableCreatorFactory A {@link TableCreatorFactory} that can be used to create the requested
     *            {@link TableCreator}
     * @throws IOException If an error occurred while communicating with the python kernel or while executing the task
     */
    public TableCreator<?> getData(final String name, final TableCreatorFactory tableCreatorFactory)
        throws IOException {
        final ProcessEndAction pea = m_segfaultDuringSerializationAction;
        try {
            addProcessEndAction(pea);
            final int tableSize = m_commands.getTableSize(name).get();
            int numberChunks = (int)Math.ceil(tableSize / (double)m_kernelOptions.getChunkSize());
            if (numberChunks == 0) {
                numberChunks = 1;
            }
            TableCreator<?> tableCreator = null;
            for (int i = 0; i < numberChunks; i++) {
                final int start = m_kernelOptions.getChunkSize() * i;
                final int end = Math.min(tableSize, (start + m_kernelOptions.getChunkSize()) - 1);
                final byte[] bytes = m_commands.getTableChunk(name, start, end).get();
                if (tableCreator == null) {
                    final TableSpec spec = m_serializer.tableSpecFromBytes(bytes);
                    tableCreator = tableCreatorFactory.createTableCreator(spec, tableSize);
                }
                m_serializer.bytesIntoTable(tableCreator, bytes, m_kernelOptions.getSerializationOptions());
            }
            return tableCreator;
        } catch (InterruptedException | ExecutionException ex) {
            throw getMostSpecificPythonKernelException(ex);
        } finally {
            removeProcessEndAction(pea);
        }
    }

    /**
     * Put a {@link PickledObject} into the python workspace.
     *
     * @param name the name of the variable in the python workspace
     * @param object the {@link PickledObject}
     * @throws IOException If an error occurred while communicating with the python kernel or while executing the task
     */
    public void putObject(final String name, final PickledObject object) throws IOException {
        try {
            m_commands.putObject(name, object.getPickledObject()).get();
        } catch (InterruptedException | ExecutionException ex) {
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    /**
     * Put a {@link PickledObject} into the python workspace in an extra thread and monitor the progress.
     *
     * @param name the name of the variable in the python workspace
     * @param object the {@link PickledObject}
     * @param exec the {@link ExecutionContext} of the calling node
     * @throws IOException If an error occurred while communicating with the python kernel or while executing the task
     * @throws CanceledExecutionException if canceled
     */
    public void putObject(final String name, final PickledObject object, final ExecutionContext exec)
        throws IOException, CanceledExecutionException {
        // Remember exceptions
        final AtomicReference<Exception> exception = new AtomicReference<>(null);
        // Start the thread
        final Future<?> future = m_executorService.submit(() -> {
            try {
                putObject(name, object);
            } catch (final Exception ex) {
                exception.set(ex);
            }
        });

        // Wait for the thread
        boolean done = false;
        while (!done) {
            try {
                future.get(WAIT_TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS);
                done = true;
            } catch (final ExecutionException ex) {
                // Can't happen because we catch every exception
            } catch (final InterruptedException ex) {
                // The thread has been interrupted
                exec.checkCanceled();
            } catch (final TimeoutException ex) {
                // Not done yet -> Check if the execution has been canceled
                exec.checkCanceled();
            }
        }

        // If there was an exception in the execution thread throw it here
        if (exception.get() != null) {
            throw getMostSpecificPythonKernelException(exception.get());
        }
    }

    /**
     * Get a {@link PickledObject} from the python workspace.
     *
     * @param name the name of the variable in the python workspace
     * @param exec the {@link ExecutionContext} of the calling KNIME node
     * @return a {@link PickledObject} containing the pickled object representation, the objects type and a string
     *         representation of the object
     * @throws IOException If an error occurred while communicating with the python kernel or while executing the task
     */
    public PickledObject getObject(final String name, final ExecutionContext exec) throws IOException {
        try {
            final byte[] bytes = m_commands.getObject(name).get();
            final TableSpec spec = m_serializer.tableSpecFromBytes(bytes);
            final KeyValueTableCreator tableCreator = new KeyValueTableCreator(spec);
            m_serializer.bytesIntoTable(tableCreator, bytes, m_kernelOptions.getSerializationOptions());
            final Row row = tableCreator.getTable();
            final int bytesIndex = spec.findColumn("bytes");
            final int typeIndex = spec.findColumn("type");
            final int representationIndex = spec.findColumn("representation");
            final byte[] objectBytes = row.getCell(bytesIndex).getBytesValue();
            return new PickledObject(objectBytes, row.getCell(typeIndex).getStringValue(),
                row.getCell(representationIndex).getStringValue());
        } catch (final InterruptedException | ExecutionException ex) {
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
     * @throws IOException If an error occurred while communicating with the python kernel or while executing the task
     */
    public void putSql(final String name, final DatabaseQueryConnectionSettings conn, final CredentialsProvider cp,
        final Collection<String> jars) throws IOException {
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
        final byte[] bytes = m_serializer.tableToBytes(tableIterator, m_kernelOptions.getSerializationOptions());
        try {
            m_commands.putSql(name, bytes).get();
        } catch (final InterruptedException | ExecutionException ex) {
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    /**
     * Gets a SQL query from the python workspace.
     *
     * @param name the name of the DBUtil variable in the python workspace
     * @return a SQL query string
     * @throws IOException If an error occurred while communicating with the python kernel or while executing the task
     */
    public String getSql(final String name) throws IOException {
        try {
            return m_commands.getSql(name).get();
        } catch (final InterruptedException | ExecutionException ex) {
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    /**
     * Get an image from the workspace.
     *
     * The variable on the python site has to hold a byte string representing an image.
     *
     * @param name The name of the image
     * @return the image
     * @throws IOException If an error occurred while communicating with the python kernel or while executing the task
     */
    public ImageContainer getImage(final String name) throws IOException {
        final byte[] bytes;
        try {
            bytes = m_commands.getImage(name).get();
        } catch (final InterruptedException | ExecutionException ex) {
            throw getMostSpecificPythonKernelException(ex);
        }
        if (bytes != null) {
            final String string = new String(bytes, "UTF-8");
            if (string.startsWith("<?xml")) {
                try {
                    return new ImageContainer(stringToSVG(string));
                } catch (final TranscoderException ex) {
                    throw new IOException(ex.getMessage(), ex);
                }
            } else {
                return new ImageContainer(ImageIO.read(new ByteArrayInputStream(bytes)));
            }
        } else {
            return null;
        }
    }

    /**
     * Convert a string containing the XML content of a svg image to a {@link SVGDocument}.
     *
     * @param svgString a string containing the XML content of a svg image
     * @return a {@link SVGDocument}
     * @throws IOException if the svg file cannot be created or written
     */
    private SVGDocument stringToSVG(final String svgString) throws IOException {
        SVGDocument doc = null;
        final StringReader reader = new StringReader(svgString);
        try {
            final String parser = XMLResourceDescriptor.getXMLParserClassName();
            final SAXSVGDocumentFactory f = new SAXSVGDocumentFactory(parser);
            doc = f.createSVGDocument("file:/file.svg", reader);
        } finally {
            reader.close();
        }
        return doc;
    }

    /**
     * Returns the list of all defined variables, functions, classes and loaded modules.
     *
     * Each variable map contains the fields 'name', 'type' and 'value'.
     *
     * @return List of variables currently defined in the workspace
     * @throws IOException If an error occurred while communicating with the python kernel or while executing the task
     */
    public List<Map<String, String>> listVariables() throws IOException {
        try {
            final byte[] bytes = m_commands.listVariables().get();
            final TableSpec spec = m_serializer.tableSpecFromBytes(bytes);
            final TemporaryTableCreator tableCreator = new TemporaryTableCreator(spec);
            m_serializer.bytesIntoTable(tableCreator, bytes, m_kernelOptions.getSerializationOptions());
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
        } catch (final InterruptedException | ExecutionException ex) {
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    /**
     * Returns the list of possible auto completions to the given source at the given position.
     *
     * Each auto completion contains the fields 'name', 'type' and 'doc'.
     *
     * @param sourceCode The source code
     * @param line Cursor position (line)
     * @param column Cursor position (column)
     * @return Possible auto completions.
     * @throws IOException If an error occurred while communicating with the python kernel or while executing the task
     */
    public List<Map<String, String>> autoComplete(final String sourceCode, final int line, final int column)
        throws IOException {
        try {
            final List<Map<String, String>> suggestions = new ArrayList<>();
            if (m_hasAutocomplete) {
                final byte[] bytes = m_commands.autoComplete(sourceCode, line, column).get();
                final TableSpec spec = m_serializer.tableSpecFromBytes(bytes);
                final TemporaryTableCreator tableCreator = new TemporaryTableCreator(spec);
                m_serializer.bytesIntoTable(tableCreator, bytes, m_kernelOptions.getSerializationOptions());
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
        } catch (InterruptedException | ExecutionException ex) {
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

    /**
     * Execute the given source code.
     *
     * @param sourceCode The source code to execute
     * @return Standard console output
     * @throws IOException If an error occurred while communicating with the python kernel or while executing the task
     */
    public String[] execute(final String sourceCode) throws IOException {
        // In execution mode only the warnings are logged to stdout.
        // If an error occurs it is transferred via the socket and available at position 1 of the returned string array.
        routeErrorMessagesToWarningLog(true);
        try {
            final String[] output = m_commands.execute(sourceCode).get();
            routeErrorMessagesToWarningLog(false);
            if (output[0].length() > 0) {
                LOGGER.debug(ScriptingNodeUtils.shortenString(output[0], 1000));
            }
            return output;
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("Python execution was interrupted. See log for details.", ex);
        } catch (final ExecutionException ex) {
            throw new IOException("Python execution failed. See log for details.", ex);
        }
    }

    public String[] executeAsync(final String sourceCode) throws IOException {
        // In execution mode only the warnings are logged to stdout.
        // If an error occurs it is transferred via the socket and available at position 1 of the returned string array.
        routeErrorMessagesToWarningLog(true);
        try {
            final String[] output = m_commands.executeAsync(sourceCode).get();
            routeErrorMessagesToWarningLog(false);
            if (output[0].length() > 0) {
                LOGGER.debug(ScriptingNodeUtils.shortenString(output[0], 1000));
            }
            return output;
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("Python execution was interrupted. See log for details.", ex);
        } catch (final ExecutionException ex) {
            throw new IOException("Python execution failed. See log for details.", ex);
        }
    }

    /**
     * Execute the given source code while still checking if the given execution context has been canceled
     *
     * @param sourceCode The source code to execute
     * @param cancelable The cancelable to check if execution has been canceled
     * @return Standard console output
     * @throws IOException If an error occurred while communicating with the python kernel or while executing the task
     * @throws CanceledExecutionException if canceled
     */
    public String[] execute(final String sourceCode, final PythonCancelable cancelable)
        throws IOException, CanceledExecutionException {

        final AtomicReference<Exception> exception = new AtomicReference<>(null);
        final AtomicReference<String[]> output = new AtomicReference<>();

        // Thread running the execute
        final Future<?> future = m_executorService.submit(ThreadUtils.runnableWithContext(() -> {
            String[] out;
            try {
                out = execute(sourceCode);
                output.set(out);
                // If the error log has content throw it as exception
                if (!out[1].isEmpty()) {
                    throw new PythonIOException(out[1]);
                }
            } catch (final Exception ex) {
                exception.set(ex);
            }
        }));

        // Wait until execution is done
        boolean done = false;
        while (!done) {
            try {
                future.get(WAIT_TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS);
                done = true;
            } catch (final ExecutionException ex) {
                // Can't happen because we catch every exception
            } catch (final InterruptedException | TimeoutException ex) {
                // The thread has been interrupted or is not done yet
                try {
                    // TODO: Refactor to use Python-specific canceled exception everywhere.
                    cancelable.checkCanceled();
                } catch (PythonCanceledExecutionException ex1) {
                    throw new CanceledExecutionException(ex.getMessage());
                }
            }
        }

        // If there was an exception in the execution thread throw it here
        if (exception.get() != null) {
            throw getMostSpecificPythonKernelException(exception.get());
        }
        return output.get();
    }

    public String[] executeAsync(final String sourceCode, final PythonCancelable cancelable)
        throws IOException, CanceledExecutionException {

        final AtomicReference<Exception> exception = new AtomicReference<>(null);
        final AtomicReference<String[]> output = new AtomicReference<>();

        // Thread running the execute
        final Future<?> future = m_executorService.submit(ThreadUtils.runnableWithContext(() -> {
            String[] out;
            try {
                out = executeAsync(sourceCode);
                output.set(out);
                // If the error log has content throw it as exception
                if (!out[1].isEmpty()) {
                    throw new PythonIOException(out[1]);
                }
            } catch (final Exception ex) {
                exception.set(ex);
            }
        }));

        // Wait until execution is done
        boolean done = false;
        while (!done) {
            try {
                future.get(WAIT_TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS);
                done = true;
            } catch (final ExecutionException ex) {
                // Can't happen because we catch every exception
            } catch (final InterruptedException | TimeoutException ex) {
                // The thread has been interrupted or is not done yet
                try {
                    // TODO: Refactor to use Python-specific canceled exception everywhere.
                    cancelable.checkCanceled();
                } catch (PythonCanceledExecutionException ex1) {
                    throw new CanceledExecutionException(ex.getMessage());
                }
            }
        }
        // If there was an exception in the execution thread throw it here
        if (exception.get() != null) {
            throw getMostSpecificPythonKernelException(exception.get());
        }
        return output.get();
    }

    /**
     * Resets the workspace of the python kernel.
     *
     * @throws IOException If an error occured
     */
    public void resetWorkspace() throws IOException {
        try {
            m_commands.reset().get();
        } catch (InterruptedException | ExecutionException ex) {
            throw getMostSpecificPythonKernelException(ex);
        }
    }

    /**
     * Shuts down the python kernel.
     *
     * This shuts down the python background process and closes the sockets used for communication.
     */
    @Override
    public void close() {
        if (m_closed.compareAndSet(false, true)) {
            new Thread(() -> {
                // Order is intended.
                PythonUtils.Misc.invokeSafely(LOGGER::debug,
                    listeners -> listeners.forEach(
                        listener -> PythonUtils.Misc.invokeSafely(LOGGER::debug, l -> l.setSilenced(true), listener)),
                    m_stdoutListeners, m_stderrListeners);
                PythonUtils.Misc.closeSafely(LOGGER::debug, m_commands, m_serverSocket, m_socket);
                PythonUtils.Misc.invokeSafely(LOGGER::debug, List<PythonOutputListener>::clear, m_stdoutListeners,
                    m_stderrListeners);
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
                    } catch (final IOException | SecurityException ex) {
                        // Ignore.
                    } catch (final InterruptedException ex) {
                        // Closing the kernel should not be interrupted.
                        Thread.currentThread().interrupt();
                    }
                }
                if (m_process != null) {
                    m_process.destroy();
                }
            }).start();

            // Shutdown the executor pool
            m_executorService.shutdown();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }

    /**
     * Add a listener receiving live messages from the python stdout stream.
     *
     * @param listener a {@link PythonOutputListener}
     */
    public synchronized void addStdoutListener(final PythonOutputListener listener) {
        m_stdoutListeners.add(listener);
    }

    /**
     * Add a listener receiving live messages from the python stderror stream.
     *
     * @param listener a {@link PythonOutputListener}
     */
    public synchronized void addStderrorListener(final PythonOutputListener listener) {
        m_stderrListeners.add(listener);
    }

    /**
     * Remove a listener receiving live messages from the python stdout stream.
     *
     * @param listener a {@link PythonOutputListener}
     */
    public synchronized void removeStdoutListener(final PythonOutputListener listener) {
        m_stdoutListeners.remove(listener);
    }

    /**
     * Remove a listener receiving live messages from the python stderror stream.
     *
     * @param listener a {@link PythonOutputListener}
     */
    public synchronized void removeStderrorListener(final PythonOutputListener listener) {
        m_stderrListeners.remove(listener);
    }

    /**
     * @return the default stdout listener which logs to the info log by default
     * @since 3.6.0
     */
    public PythonOutputListener getDefaultStdoutListener() {
        return m_defaultStdoutListener;
    }

    PythonCommands getCommands() {
        return m_commands;
    }

    public void routeErrorMessagesToWarningLog(final boolean routeToWarningLog) {
        for (PythonOutputListener listener : m_stderrListeners) {
            if (listener instanceof ConfigurablePythonOutputListener) {
                ((ConfigurablePythonOutputListener)listener).setAllWarnings(routeToWarningLog);
            }
        }
    }

    private synchronized void distributeStdoutMsg(final String msg) {
        for (final PythonOutputListener listener : m_stdoutListeners) {
            listener.messageReceived(msg);
        }
    }

    private synchronized void distributeStderrorMsg(final String msg) {
        for (final PythonOutputListener listener : m_stderrListeners) {
            listener.messageReceived(msg);
        }
    }

    private void startPipeListeners() {
        new Thread(() -> {
            String message;
            final BufferedReader reader = new BufferedReader(new InputStreamReader(m_stdoutStream));
            try {
                while ((message = reader.readLine()) != null) {
                    distributeStdoutMsg(message);
                }
            } catch (final IOException ex) {
                LOGGER.warn("Exception during interactive logging: " + ex.getMessage(), ex);
            }

        }).start();

        new Thread(() -> {
            String message;
            final BufferedReader reader = new BufferedReader(new InputStreamReader(m_stderrStream));
            try {
                while ((message = reader.readLine()) != null) {
                    distributeStderrorMsg(message);
                }
            } catch (final IOException ex) {
                LOGGER.debug("Exception during interactive logging: " + ex.getMessage(), ex);
            }

        }).start();
    }

    private PythonIOException getMostSpecificPythonKernelException(final Exception exception) {
        if (!m_pythonKernelMonitorResult.isDone()) {
            try {
                // Sleep a bit to give the monitor thread time to finish.
                // TODO: Investigate if we can wait for the thread as well here?!
                Thread.sleep(100);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            if (m_pythonKernelMonitorResult.isDone() && m_pythonKernelMonitorResult.get() != null) {
                return m_pythonKernelMonitorResult.get();
            }
        } catch (InterruptedException | ExecutionException ex) {
            exception.addSuppressed(ex);
            return new PythonIOException("An exception occured while running the Python kernel. See log for details.",
                exception);
        }
        if (exception instanceof PythonIOException) {
            return (PythonIOException)exception;
        }
        if (exception instanceof PythonException) {
            return new PythonIOException(exception.getMessage(), exception);
        }
        return new PythonIOException("An exception occured while running the Python kernel. See log for details.",
            exception);
    }

    /**
     * An action to run as soon as the python process exits. Allows to examine custom exit codes.
     */
    public static interface ProcessEndAction {

        /**
         * @param exitCode the exit code of the Python process
         * @throws PythonIOException if the process end action detected a erroneous process end
         */
        void runForExitCode(int exitCode) throws PythonIOException;
    }

    private static class ConfigurablePythonOutputListener implements PythonOutputListener {

        private boolean m_silenced = false;

        private boolean m_allWarning = false;

        private boolean m_lastStackTrace = false;

        private final AtomicBoolean m_errorWasLogged = new AtomicBoolean(false);

        /**
         * Reset the flag that is set if an error was logged.
         */
        public void resetErrorLoggedFlag() {
            m_errorWasLogged.set(false);
        }

        /**
         * Get a flag indicating if an error was logged.
         *
         * @return error was logged yes / no
         */
        public boolean wasErrorLogged() {
            return m_errorWasLogged.get();
        }

        @Override
        public void setSilenced(final boolean silenced) {
            m_silenced = silenced;
        }

        /**
         * Enables special handling of the stderror stream when custom source code is executed.
         *
         * @param on turn handling on / off
         */
        private void setAllWarnings(final boolean on) {
            m_allWarning = on;
        }

        @Override
        public void messageReceived(final String msg) {
            if (!m_silenced) {
                if (!m_allWarning) {
                    if (!msg.startsWith("Traceback") && !msg.startsWith(" ")) {
                        LOGGER.error(msg);
                        m_errorWasLogged.set(true);
                        m_lastStackTrace = false;
                    } else {
                        if (!m_lastStackTrace) {
                            LOGGER.debug("Python error with stacktrace:\n");
                            m_lastStackTrace = true;
                        }
                        LOGGER.debug(msg);
                    }
                } else {
                    LOGGER.warn(msg);
                }
            } else {
                LOGGER.debug(msg);
            }
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
