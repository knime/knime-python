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
 *   Jul 25, 2022 (benjamin): created
 */
package org.knime.python3.scripting.nodes2;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.knime.conda.CondaEnvironmentDirectory;
import org.knime.core.data.IDataRepository;
import org.knime.core.data.filestore.FileStoreKey;
import org.knime.core.data.filestore.internal.IFileStoreHandler;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.FlowObjectStack;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.util.ThreadUtils;
import org.knime.core.webui.data.DataServiceContext;
import org.knime.python3.scripting.nodes2.PythonScriptingService.ExecutableOption.ExecutableOptionType;
import org.knime.python3.scripting.nodes2.PythonScriptingSession.ExecutionInfo;
import org.knime.python3.scripting.nodes2.PythonScriptingSession.ExecutionStatus;
import org.knime.python3.scripting.nodes2.PythonScriptingSession.FileStoreHandlerSupplier;
import org.knime.python3.views.PythonNodeViewStoragePath;
import org.knime.scripting.editor.CodeGenerationRequest;
import org.knime.scripting.editor.InputOutputModel;
import org.knime.scripting.editor.ScriptingService;

/**
 * A special {@link ScriptingService} for the Python scripting node.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction")
final class PythonScriptingService extends ScriptingService {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonScriptingService.class);

    private final boolean m_hasView;

    private final PythonScriptPortsConfiguration m_ports;

    private Map<String, ExecutableOption> m_executableOptions = Collections.emptyMap();

    // indicates that killSession has been called
    private AtomicBoolean m_expectCancel;

    private String m_executableSelection = "";

    private PythonScriptingSession m_interactiveSession;

    private Optional<PythonNodeViewStoragePath> m_view;

    /**
     * Create a new {@link PythonScriptingService}.
     *
     * @param hasView if the node has an output view
     */
    PythonScriptingService(final boolean hasView) {
        super(PythonLanguageServer::startLanguageServer, PythonScriptNodeModel.KNOWN_FLOW_VARIABLE_TYPES_SET::contains);

        m_hasView = hasView;
        m_view = Optional.empty();
        m_ports = PythonScriptPortsConfiguration.fromCurrentNodeContext();
        m_expectCancel = new AtomicBoolean(false);
    }

    private ExecutableOption getExecutableOption(final String id) {
        if (!getExecutableOptions().containsKey(id)) {
            var allFlowVars = Optional.ofNullable(getWorkflowControl().getFlowObjectStack()) //
                .map(FlowObjectStack::getAllAvailableFlowVariables) //
                .orElseGet(Collections::emptyMap);
            if (allFlowVars.containsKey(id)) {
                return getExecutableOptionFromVariable(id, allFlowVars.get(id).getStringValue());
            } else {
                // Missing variable selected
                return new ExecutableOption(ExecutableOptionType.MISSING_VAR, id, null, null, null);
            }
        }
        return getExecutableOptions().get(id);
    }

    /** Get the {@link ExecutableOption} for the variable value (which might be <code>null</code>) */
    private static ExecutableOption getExecutableOptionFromVariable(final String id, final String value) {
        if (value != null) {
            // String variable selected
            if (ExecutableSelectionUtils.isPathToCondaEnv(value)) {
                // Points to a conda environment
                return new ExecutableOption(ExecutableOptionType.STRING_VAR, id,
                    CondaEnvironmentDirectory.getPythonExecutableString(value),
                    Paths.get(value).getFileName().toString(), value);
            } else {
                // Points to single Python executable
                return new ExecutableOption(ExecutableOptionType.STRING_VAR, id, value, null, null);
            }
        } else {
            // Variable selected but it is not a string -> not usable
            return new ExecutableOption(ExecutableOptionType.MISSING_VAR, id, null, null, null);
        }
    }

    @Override
    public PythonRpcService getJsonRpcService() {
        return new PythonRpcService();
    }

    private synchronized Map<String, ExecutableOption> getExecutableOptions() {
        if (m_executableOptions == null || m_executableOptions.isEmpty()) {
            // Set the executable options with the currently available flow variables
            m_executableOptions =
                ExecutableSelectionUtils.getExecutableOptions(getWorkflowControl().getFlowObjectStack());
        }
        return m_executableOptions;
    }

    @Override
    public void onDeactivate() {
        // Call the parent to close the language server
        super.onDeactivate();

        // Close the interactive Python session
        clearSession();

        // Reset the executable selection and options
        m_executableOptions = null;
        m_executableSelection = "";

        // Clear the preview
        clearView();
    }

    public InputStream openHtmlPreview() {
        if (m_view.isPresent() && Files.exists(m_view.get().getPath())) {
            try {
                return Files.newInputStream(m_view.get().getPath());
            } catch (IOException ex) {
                LOGGER.error("Failed to open preview.", ex);
                return InputStream.nullInputStream();
            }
        }
        // No preview available yet - show a placeholder
        // NB: This page will not be shown in frontend, instead a placeholder will be shown
        return InputStream.nullInputStream();
    }

    private void clearView() {
        m_view.ifPresent(v -> {
            try {
                v.deleteIfExists();
            } catch (IOException ex) {
                LOGGER.error("Failed to delete the preview.", ex);
            }
        });
        m_view = Optional.empty();
    }

    private void clearSession() {
        if (m_interactiveSession != null) {
            try {
                m_interactiveSession.close();
            } catch (IOException ex) {
                LOGGER.error("Failed to close interactive Python session.", ex);
            }
            m_interactiveSession = null;
        }
    }

    /**
     * An extension of the {@link org.knime.scripting.editor.ScriptingService.RpcService} that provides additional
     * methods to the frontend of the Python scripting node.
     *
     * NB: Must be public for the JSON-RPC server
     */
    public final class PythonRpcService extends RpcService {

        public void sendLastConsoleOutput() {
            // Send the console output of the last execution to the dialog
            try {
                ((PythonScriptNodeModel)getWorkflowControl().getNodeModel())
                    .sendLastConsoleOutputs(PythonScriptingService.this::addConsoleOutputEvent);
            } catch (final Exception e) {
                final var message = "Sending the console output of the last execution to the dialog failed.";
                LOGGER.warn(message, e);
                DataServiceContext.get().addWarningMessage(message);
            }
        }

        private void startNewInteractiveSession() throws IOException, InterruptedException, CanceledExecutionException {
            // Clear the last session if there is one
            clearSession();

            // Start the interactive Python session and setup the IO
            final var workflowControl = getWorkflowControl();
            final var pythonCommand =
                ExecutableSelectionUtils.getPythonCommand(getExecutableOption(m_executableSelection));

            // TODO report the progress of converting the tables using the ExecutionMonitor?
            m_interactiveSession = new PythonScriptingSession(pythonCommand,
                PythonScriptingService.this::addConsoleOutputEvent, new DialogFileStoreHandlerSupplier());
            m_interactiveSession.setupIO(workflowControl.getInputData(), getSupportedFlowVariables(),
                m_ports.getNumOutTables(), m_ports.getNumOutImages(), m_ports.getNumOutObjects(), m_hasView,
                new ExecutionMonitor());
        }

        private synchronized void executeScriptInternal(final String script, final boolean newSession,
            final boolean checkOutputs) {
            try {
                // Restart the session if necessary
                if (m_interactiveSession == null || newSession) {
                    startNewInteractiveSession();
                }

                // Run the script
                var execInfo = m_interactiveSession.execute(script, checkOutputs);
                if (m_hasView) {
                    clearView();
                    // NB: If no view is assigned in the current session m_view will be empty
                    m_view = m_interactiveSession.getOutputView();
                }

                // if view is present, update preview
                execInfo.setHasValidView(m_view.isPresent());

                // Done with executing
                sendExecutionFinishedEvent(execInfo);
            } catch (Exception ex) { // NOSONAR - we want to handle all exceptions
                if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt(); // Re-interrupt
                }

                if (m_expectCancel.get()) {
                    sendExecutionFinishedEvent(
                        new ExecutionInfo(ExecutionStatus.CANCELLED, "Script execution was cancelled by user"));
                } else {
                    var message = "Execution failed: " + ex.getMessage();
                    LOGGER.error(message, ex);
                    sendExecutionFinishedEvent(new ExecutionInfo(ExecutionStatus.FATAL_ERROR, message));
                }
            } finally {
                m_expectCancel.set(false);
            }
        }

        void sendExecutionFinishedEvent(final ExecutionInfo info) {
            sendEvent("python-execution-finished", info);
        }

        /**
         * Runs the script in a new session and checks the output.
         *
         * @param script the user script
         */
        public void runScript(final String script) {
            m_expectCancel.set(false);
            ThreadUtils.threadWithContext(() -> executeScriptInternal(script, true, true), "python-execution").start();
        }

        /**
         * Runs the script in the existing Python session. Does not check the output.
         *
         * @param script the user script
         */
        public void runInExistingSession(final String script) {
            m_expectCancel.set(false);
            ThreadUtils.threadWithContext(() -> executeScriptInternal(script, false, false), "python-execution")
                .start();
        }

        /**
         * Called from frontend during execution. Stops execution and eventually throws py4j error in runInteractive.
         *
         * @return whether the session has been killed successfully or not
         */
        public KillSessionInfo killSession() {
            if (m_interactiveSession == null) {
                // No session to kill
                return new KillSessionInfo(KillSessionStatus.ERROR, "There is no active python session in progress");
            }

            try {
                m_expectCancel.set(true);
                m_interactiveSession.close();
                return new KillSessionInfo(KillSessionStatus.SUCCESS, "Stopped execution of python session.");
            } catch (Exception ex) { // NOSONAR - we want to handle all exceptions
                var message = "Error while stopping the Python execution: " + ex.getMessage();
                LOGGER.error(message, ex);
                return new KillSessionInfo(KillSessionStatus.ERROR, message);
            } finally {
                m_interactiveSession = null;
            }
        }

        /**
         * Update the executable selection. If a session is running, it is cleared. The next session will use the new
         * executable selection.
         *
         * @param executableSelection the identifier for the new executable
         */
        public void updateExecutableSelection(final String executableSelection) {
            if (!m_executableSelection.equals(executableSelection)) {
                m_executableSelection = executableSelection;
                clearSession();
            }
        }

        /**
         * @param executableSelection the identifier of the active executable option
         * @return the path to the Python executable. Only to be used for configuring the LSP server.
         */
        public String getLanguageServerConfig(final String executableSelection) {
            var executableOption = getExecutableOption(executableSelection);
            String executablePath = null;
            if (executableOption.type != ExecutableOptionType.MISSING_VAR) {
                executablePath = ExecutableSelectionUtils.getPythonCommand(executableOption).getPythonExecutablePath()
                    .toAbsolutePath().toString();
            }
            var extraPaths = PythonScriptingSession.getExtraPythonPaths().stream() //
                .map(Path::toAbsolutePath) //
                .map(Path::toString) //
                .toList();
            return PythonLanguageServer.getConfig(executablePath, extraPaths);
        }

        @Override
        protected CodeGenerationRequest getCodeSuggestionRequest(final String userPrompt, final String currentCode,
            final InputOutputModel[] inputOutputModels) {
            return PythonCodeAssistant.createCodeGenerationRequest(userPrompt, currentCode, inputOutputModels, m_hasView);
        }
    }

    enum StartSessionStatus {
            SUCCESS, ERROR
    }

    static record StartSessionInfo(StartSessionStatus status, String description) {
    }

    enum KillSessionStatus {
            SUCCESS, ERROR
    }

    static record KillSessionInfo(KillSessionStatus status, String description) {
    }

    /** Information about an installed Conda package */
    public static class CondaPackageInfo {
        public final String name; // NOSONAR

        public final String version; // NOSONAR

        public final String build; // NOSONAR

        public final String channel; // NOSONAR

        @SuppressWarnings("hiding")
        CondaPackageInfo(final String name, final String version, final String build, final String channel) {
            this.name = name;
            this.version = version;
            this.build = build;
            this.channel = channel;
        }
    }

    /** An option to set as a Python executable */
    public static class ExecutableOption {
        /** The type of the option */
        public final ExecutableOptionType type; // NOSONAR

        /** "" for the preference options or the name of the flow variable otherwise */
        public final String id; // NOSONAR

        public final String pythonExecutable; // NOSONAR

        public final String condaEnvName; // NOSONAR

        public final String condaEnvDir; // NOSONAR

        @SuppressWarnings("hiding")
        ExecutableOption(final ExecutableOptionType type, final String id, final String pythonExecutable,
            final String condaEnvName, final String condaEnvDir) {
            this.type = type;
            this.id = id;
            this.pythonExecutable = pythonExecutable;
            this.condaEnvName = condaEnvName;
            this.condaEnvDir = condaEnvDir;
        }

        public enum ExecutableOptionType {
                /** Bundled on the preference page */
                PREF_BUNDLED,

                /** Conda on the preference page */
                PREF_CONDA,

                /** Manual on the preference page */
                PREF_MANUAL,

                /** Conda environment variable */
                CONDA_ENV_VAR,

                /** String variable that contains the path to a Python executable */
                STRING_VAR,

                /** A variable of any type that is missing now */
                MISSING_VAR,
        }
    }

    private static final class DialogFileStoreHandlerSupplier implements FileStoreHandlerSupplier {

        private IWriteFileStoreHandler m_temporaryWriteFileStoreHandler;

        @Override
        public IWriteFileStoreHandler getWriteFileStoreHandler() {
            if (m_temporaryWriteFileStoreHandler == null) {
                m_temporaryWriteFileStoreHandler =
                    new NotInWorkflowWriteFileStoreHandler(UUID.randomUUID(), getDataRepositoryFromContext());
            }
            return m_temporaryWriteFileStoreHandler;
        }

        private static IDataRepository getDataRepositoryFromContext() {
            return NodeContext.getContext().getWorkflowManager().getWorkflowDataRepository();
        }

        @Override
        public IFileStoreHandler getFileStoreHandler(final FileStoreKey key) {
            // The file store can be coming from a previously written table -- then it is part of
            // the WorkflowDataRepository or it could have been created during execution of the Python
            // script, then it was using the NotInWorkflowWriteFileStoreHandler

            var handler = getDataRepositoryFromContext().getHandler(key.getStoreUUID());

            if (handler != null) {
                return handler;
            } else {
                return m_temporaryWriteFileStoreHandler;
            }
        }

        @Override
        public void close() {
            if (m_temporaryWriteFileStoreHandler != null) {
                m_temporaryWriteFileStoreHandler.clearAndDispose();
            }
        }
    }
}
