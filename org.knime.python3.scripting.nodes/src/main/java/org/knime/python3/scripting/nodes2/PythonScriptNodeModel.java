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
 *   Jun 30, 2022 (benjamin): created
 */
package org.knime.python3.scripting.nodes2;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.data.filestore.FileStoreKey;
import org.knime.core.data.filestore.internal.IFileStoreHandler;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.KNIMEConstants;
import org.knime.core.node.KNIMEException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.node.workflow.VariableType;
import org.knime.core.node.workflow.VariableTypeRegistry;
import org.knime.core.util.PathUtils;
import org.knime.core.util.asynclose.AsynchronousCloseableTracker;
import org.knime.python3.PythonCommand;
import org.knime.python3.scripting.nodes2.ConsoleOutputUtils.ConsoleOutputStorage;
import org.knime.python3.scripting.nodes2.PythonScriptingSession.ExecutionInfo;
import org.knime.python3.scripting.nodes2.PythonScriptingSession.ExecutionStatus;
import org.knime.python3.scripting.nodes2.PythonScriptingSession.FileStoreHandlerSupplier;
import org.knime.python3.utils.FlowVariableUtils;
import org.knime.scripting.editor.ScriptingService.ConsoleText;

import py4j.Py4JException;
import py4j.Py4JNetworkException;

/**
 * The node model of a Python scripting node.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class PythonScriptNodeModel extends NodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonScriptNodeModel.class);

    private static final String SEND_LAST_OUTPUT_PREFIX =
        "------------ START - Console output of the last execution ------------\n\n";

    private static final String SEND_LAST_OUTPUT_SUFFIX =
        "\n\n------------- END - Console output of the last execution -------------\n\n";

    private final boolean m_hasView;

    private final PythonScriptNodeSettings m_settings;

    private final PythonScriptPortsConfiguration m_ports;

    private final AsynchronousCloseableTracker<IOException> m_sessionShutdownTracker =
        new AsynchronousCloseableTracker<>(t -> LOGGER.debug("Kernel shutdown failed.", t));

    private ConsoleOutputStorage m_consoleOutputStorage;

    private Optional<Path> m_view;

    static final VariableType<?>[] KNOWN_FLOW_VARIABLE_TYPES = FlowVariableUtils.convertToFlowVariableTypes(Set.of( //
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
    ));

    static final Set<VariableType<?>> KNOWN_FLOW_VARIABLE_TYPES_SET = Set.of(KNOWN_FLOW_VARIABLE_TYPES);

    /**
     * @param portsConfiguration the configured ports
     * @param hasView if the node has a view
     */
    public PythonScriptNodeModel(final PortsConfiguration portsConfiguration, final boolean hasView) {
        super(portsConfiguration.getInputPorts(), portsConfiguration.getOutputPorts());
        m_hasView = hasView;
        m_ports = PythonScriptPortsConfiguration.fromPortsConfiguration(portsConfiguration, hasView);
        m_settings = new PythonScriptNodeSettings(m_ports);
        m_view = Optional.empty();
    }

    /**
     * @return the path to the HTML view file
     * @throws IllegalStateException if no view is available
     */
    public Path getPathToHtmlView() {
        return m_view
            .orElseThrow(() -> new IllegalStateException("View is not present. This is an implementation error."));
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return null; // NOSONAR
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec)
        throws IOException, InterruptedException, CanceledExecutionException, KNIMEException {
        final PythonCommand pythonCommand =
            ExecutableSelectionUtils.getPythonCommand(m_settings.getExecutableSelection());
        m_consoleOutputStorage = null;

        final var consoleConsumer = ConsoleOutputUtils.createConsoleConsumer();
        try (final var session =
            new PythonScriptingSession(pythonCommand, consoleConsumer, new ModelFileStoreHandlerSupplier())) {

            exec.setProgress(0.0, "Setting up inputs");
            session.setupIO(inObjects, getAvailableFlowVariables(KNOWN_FLOW_VARIABLE_TYPES).values(),
                m_ports.getNumOutTables(), m_ports.getNumOutImages(), m_ports.getNumOutObjects(), m_hasView,
                exec.createSubProgress(0.3));
            exec.setProgress(0.3, "Running script");

            runUserScript(session);

            exec.setProgress(0.7, "Processing output");
            final var outputs = session.getOutputs(exec.createSubExecutionContext(0.3));
            final var flowVars = session.getFlowVariables();
            addNewFlowVariables(flowVars);
            collectViewFromSession(session);

            m_sessionShutdownTracker.closeAsynchronously(session);
            return outputs;
        } catch (final Py4JException ex) {
            handleNewCommunicationChannelError(ex);
            throw new KNIMEException(StringUtils.removeStart(ex.getMessage(),
                "An exception was raised by the Python Proxy. Return Message: knime.scripting._backend."), ex);
        } finally {
            m_consoleOutputStorage = consoleConsumer.finish();
        }
    }

    /**
     * Throw a nicer error message if the exception we are seeing is an "Error while obtaining a new communication
     * channel"
     *
     * @param ex The exception
     * @throws KNIMEException A more human-readable exception
     */
    private void handleNewCommunicationChannelError(final Py4JException ex) throws KNIMEException {
        if (ex.getCause() instanceof ConnectException) {
            var messageBuilder = createMessageBuilder();
            messageBuilder.withSummary("Connecting to prefetched Python process failed.");
            messageBuilder.addResolutions(
                "The Python process we prepared in the background got killed. Try again to start a new one.");
            throw KNIMEException.of(messageBuilder.build().orElseThrow(), ex);
        }
    }

    /**
     * Throw a nicer error message if the exception we are seeing is an "error while sending a command".
     *
     * If the provided {@link PythonScriptingSession} knows a reason for termination, we show that. Otherwise we just
     * say that the Python process got terminated.
     *
     * @param session The current Python scripting session
     * @param exception The exception we're seeing
     * @throws KNIMEException
     */
    private void handleErrorWhileSendingCommandError(final PythonScriptingSession session, final Throwable exception)
        throws KNIMEException {
        if (exception.getCause() instanceof Py4JNetworkException) {
            var message = session.getTerminationReason() != null ? session.getTerminationReason()
                : "The Python process got terminated.";

            var messageBuilder = createMessageBuilder();
            messageBuilder.withSummary(message);
            // This resolution is true in any case that the Python process got killed, be it from the outside or our
            // Watchdog
            messageBuilder.addResolutions(
                "This can happen if the system ran out of memory, increase the system resources and try again.");
            throw KNIMEException.of(messageBuilder.build().orElseThrow(), exception);
        }
    }

    private void runUserScript(final PythonScriptingSession session)
        throws Py4JException, InterruptedException, KNIMEException {
        ExecutionInfo ans;
        try {
            ans = KNIMEConstants.GLOBAL_THREAD_POOL.enqueue(() -> session.execute(m_settings.getScript(), true)).get();
        } catch (ExecutionException ex) { // NOSONAR - we either log or re-throw the cause
            // We only expect Py4J exceptions to happen
            // everything else we just unwrap from the ExecutionException and wrap in a KNIMEException
            var cause = ex.getCause();
            if (cause instanceof Py4JException py4jException) {
                handleErrorWhileSendingCommandError(session, cause);

                throw py4jException;
            } else {
                throw new KNIMEException(cause.getMessage(), cause);
            }
        }
        checkExecutionAnswer(ans);
    }

    private static void checkExecutionAnswer(final ExecutionInfo ans) throws KNIMEException {
        var error = ans.getStatus();

        if (ExecutionStatus.KNIME_ERROR.equals(error) || ExecutionStatus.EXECUTION_ERROR.equals(error)) {
            var tracebackArray = ans.getTraceback();
            if (tracebackArray.length > 0) {
                var traceback = Stream.of(tracebackArray).collect(Collectors.joining("\n"));
                LOGGER.warn(traceback);
            }
            throw new KNIMEException("Execute failed: " + ans.getDescription());
        }
    }

    private void addNewFlowVariables(final Collection<FlowVariable> newVariables) {
        final Map<String, FlowVariable> oldVariables =
            getAvailableFlowVariables(VariableTypeRegistry.getInstance().getAllTypes());
        for (final FlowVariable variable : newVariables) {
            if (!Objects.equals(oldVariables.get(variable.getName()), variable)) {
                pushNewFlowVariable(variable);
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void pushNewFlowVariable(final FlowVariable variable) {
        pushFlowVariable(variable.getName(), (VariableType)variable.getVariableType(),
            variable.getValue(variable.getVariableType()));
    }

    /** Get the output view from the session if the node has a view and remember the path */
    private void collectViewFromSession(final PythonScriptingSession session) throws IOException, KNIMEException {
        if (m_hasView) {
            // Delete the last view if it is still present
            if (m_view.isPresent()) {
                PathUtils.deleteFileIfExists(m_view.get());
            }
            m_view = session.getOutputView();
            if (m_view.isEmpty()) {
                // NB: This should not happen because this is checked before
                throw new KNIMEException("No output view available.");
            }
        }
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        // Nothing to validate
        // Add a validate method to the m_settings object for validation
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadValidatedSettingsFrom(settings);
    }

    @Override
    protected void onDispose() {
        m_sessionShutdownTracker.waitForAllToClose();
    }

    @Override
    protected void reset() {
        if (m_consoleOutputStorage != null) {
            m_consoleOutputStorage.close();
            m_consoleOutputStorage = null;
        }
        m_view = Optional.empty();
        m_sessionShutdownTracker.waitForAllToClose();
    }

    void sendLastConsoleOutputs(final Consumer<ConsoleText> consumer) throws IOException {
        if (m_consoleOutputStorage != null) {
            consumer.accept(new ConsoleText(SEND_LAST_OUTPUT_PREFIX, false));
            m_consoleOutputStorage.sendConsoleOutputs(consumer);
            consumer.accept(new ConsoleText(SEND_LAST_OUTPUT_SUFFIX, false));
        }
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        m_consoleOutputStorage = ConsoleOutputUtils.openConsoleOutput(nodeInternDir.toPath());
        final var viewPath = persistedViewPath(nodeInternDir);
        if (Files.isReadable(viewPath)) {
            m_view = Optional.of(viewPath);
        }
    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        if (m_consoleOutputStorage != null) {
            // Copy the console output from the temporary files
            final var nodeInternPath = nodeInternDir.toPath();
            m_consoleOutputStorage.saveTo(nodeInternPath);
            m_consoleOutputStorage.close();

            // Re-open the new files
            m_consoleOutputStorage = ConsoleOutputUtils.openConsoleOutput(nodeInternPath);
        }
        if (m_view.isPresent()) {
            // Copy the view from the temporary file to the persisted internals directory
            Files.copy(m_view.get(), persistedViewPath(nodeInternDir));
        }
    }

    /** Path to the persisted view inside the internals directory */
    private static Path persistedViewPath(final File nodeInternDir) {
        return nodeInternDir.toPath().resolve("view.html");
    }

    private static final class ModelFileStoreHandlerSupplier implements FileStoreHandlerSupplier {

        @Override
        public IWriteFileStoreHandler getWriteFileStoreHandler() {
            final IFileStoreHandler nodeFsHandler = getFileStoreHandlerFromContext();
            if (nodeFsHandler instanceof IWriteFileStoreHandler fsHandler) {
                return fsHandler;
            } else {
                // This cannot happen
                throw new IllegalStateException(
                    "A NodeContext should be available during execution of the Python scrpting node.");
            }
        }

        @Override
        public IFileStoreHandler getFileStoreHandler(final FileStoreKey key) {
            return getFileStoreHandlerFromContext();
        }

        private static IFileStoreHandler getFileStoreHandlerFromContext() {
            return ((NativeNodeContainer)NodeContext.getContext().getNodeContainer()).getNode().getFileStoreHandler();
        }

        @Override
        public void close() {
            // Nothing to do
        }
    }
}
