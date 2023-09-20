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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.knime.conda.CondaEnvironmentDirectory;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.filestore.FileStoreKey;
import org.knime.core.data.filestore.internal.IFileStoreHandler;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.flowvariable.FlowVariablePortObject;
import org.knime.core.node.port.image.ImagePortObject;
import org.knime.core.node.port.image.ImagePortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.node.workflow.VariableType;
import org.knime.core.util.PathUtils;
import org.knime.core.util.ThreadUtils;
import org.knime.core.webui.data.DataServiceContext;
import org.knime.python2.port.PickledObjectFileStorePortObject;
import org.knime.python2.port.PickledObjectPortObjectSpec;
import org.knime.python3.scripting.nodes2.PythonScriptingService.ExecutableOption.ExecutableOptionType;
import org.knime.python3.scripting.nodes2.PythonScriptingSession.ExecutionInfo;
import org.knime.python3.scripting.nodes2.PythonScriptingSession.ExecutionStatus;
import org.knime.python3.scripting.nodes2.PythonScriptingSession.FileStoreHandlerSupplier;
import org.knime.scripting.editor.ScriptingService;

/**
 * A special {@link ScriptingService} for the Python scripting node.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
final class PythonScriptingService extends ScriptingService {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonScriptingService.class);

    private static final String INPUT_OUTPUT_TYPE_TABLE = "Table";

    private static final String INPUT_OUTPUT_TYPE_IMAGE = "Image";

    private static final String INPUT_OUTPUT_TYPE_OBJECT = "Object";

    private static final HashSet<VariableType<?>> KNOWN_FLOW_VARIABLE_SET =
        new HashSet<>(Arrays.asList(PythonScriptNodeModel.KNOWN_FLOW_VARIABLE_TYPES));

    private static final Predicate<FlowVariable> FLOW_VARIABLE_FILTER =
        x -> KNOWN_FLOW_VARIABLE_SET.contains(x.getVariableType());

    private final boolean m_hasView;

    private final PythonScriptPortsConfiguration m_ports;

    private Map<String, ExecutableOption> m_executableOptions = Collections.emptyMap();

    // indicates that killSession has been called
    private AtomicBoolean m_expectCancel;

    private String m_executableSelection = "";

    private PythonScriptingSession m_interactiveSession;

    private Optional<Path> m_view;

    /**
     * Create a new {@link PythonScriptingService}.
     *
     * @param hasView if the node has an output view
     */
    PythonScriptingService(final boolean hasView) {
        super(PythonLanguageServer::startLanguageServer, FLOW_VARIABLE_FILTER);
        m_hasView = hasView;
        m_view = Optional.empty();
        m_ports = PythonScriptPortsConfiguration.fromCurrentNodeContext();
        m_expectCancel = new AtomicBoolean(false);
    }

    private ExecutableOption getExecutableOption(final String id) {
        if (!getExecutableOptions().containsKey(id)) {
            final Map<String, FlowVariable> allFlowVars =
                getWorkflowControl().getFlowObjectStack().getAllAvailableFlowVariables();
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
        if (m_view.isPresent() && Files.exists(m_view.get())) {
            try {
                return Files.newInputStream(m_view.get());
            } catch (IOException ex) {
                LOGGER.error("Failed to open preview.", ex);
                var message = "Opening the preview failed: " + ex.getMessage();
                return new ByteArrayInputStream(message.getBytes(StandardCharsets.UTF_8));
            }
        }
        // No preview available yet - show a placeholder
        // TODO - Design a better placeholder (load it from a resource file if necessary)
        return new ByteArrayInputStream(
            "Execute the script and assign a view to the knio.output_view variable.".getBytes(StandardCharsets.UTF_8));
    }

    private void clearView() {
        m_view.ifPresent(v -> {
            try {
                PathUtils.deleteFileIfExists(v);
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

        /**
         * @return true if the node has a node view which should be shown in a preview tab
         */
        public boolean hasPreview() {
            return m_hasView;
        }

        @SuppressWarnings("restriction") // the DataServiceContext is still restricted API
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
            m_interactiveSession.setupIO(workflowControl.getInputData(), getFlowVariables(), m_ports.getNumOutTables(),
                m_ports.getNumOutImages(), m_ports.getNumOutObjects(), m_hasView, new ExecutionMonitor());
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

        @Override
        public InputOutputModel getFlowVariableInputs() {
            var subItems = getFlowVariables().stream().map(f -> { // NOSONAR
                return new InputOutputModelSubItem( //
                    f.getName(), //
                    f.getVariableType().toString(), //
                    PythonCodeAliasProvider.getFlowVariableCodeAlias(f.getName()));
            }).toArray(InputOutputModelSubItem[]::new);
            return new InputOutputModel("Flow Variables", PythonCodeAliasProvider.getFlowVariableCodeAlias(null),
                subItems);
        }

        @Override
        public List<InputOutputModel> getInputObjects() {
            try {
                return getConnectedInputPortInfo();
            } catch (Exception ex) {
                return getDefaultInputPortInfo();
            }
        }

        @Override
        public List<InputOutputModel> getOutputObjects() {
            return getDefaultOutputPortInfo();
        }

        private List<InputOutputModel> getConnectedInputPortInfo() {
            final var inputSpec = getWorkflowControl().getInputSpec();
            final var inputInfos = new ArrayList<InputOutputModel>();

            int tableIdx = 0;
            int objectIdx = 0;
            int imageIdx = 0;
            for (int i = 0; i < inputSpec.length; i++) {
                final var spec = inputSpec[i];
                if (spec instanceof DataTableSpec dataTableSpec) {
                    inputInfos.add(createFromTableSpec(tableIdx, dataTableSpec,
                        PythonCodeAliasProvider::getInputObjectCodeAlias, INPUT_OUTPUT_TYPE_TABLE));
                    tableIdx++;
                } else if (spec instanceof PickledObjectPortObjectSpec) {
                    inputInfos.add(createFromPortSpec(objectIdx, INPUT_OUTPUT_TYPE_OBJECT));
                    objectIdx++;
                } else if (spec instanceof ImagePortObjectSpec) {
                    inputInfos.add(createFromPortSpec(imageIdx, INPUT_OUTPUT_TYPE_IMAGE));
                    imageIdx++;
                } else {
                    throw new IllegalStateException("Unsupported input port. This is an implementation error.");
                }
            }
            return inputInfos;
        }

        private List<InputOutputModel> getDefaultInputPortInfo() {
            return getDefaultPortInfo("Input", getWorkflowControl().getInputPortTypes());
        }

        private List<InputOutputModel> getDefaultOutputPortInfo() {
            var outputPortInfos = getDefaultPortInfo("Output", getWorkflowControl().getOutputPortTypes());
            if (m_hasView) {
                outputPortInfos = new ArrayList<>(outputPortInfos);
                outputPortInfos
                    .add(new InputOutputModel("Output View", PythonCodeAliasProvider.getOutputViewCodeAlias(), null));
            }
            return outputPortInfos;
        }

        private List<InputOutputModel> getDefaultPortInfo(final String namePrefix, final PortType... portTypes) {
            var relevantPortTypes = Stream.of(portTypes).filter(PythonRpcService::isNoFlowVariablePort).toList();
            var portTypeCounter = new HashMap<String, Integer>();
            return IntStream.range(0, relevantPortTypes.size()).mapToObj(i -> { // NOSONAR
                var type = portTypeToInputOutputType(relevantPortTypes.get(i));
                var index = portTypeCounter.computeIfAbsent(type, t -> 0);
                portTypeCounter.put(type, index + 1);
                var inputName = String.format("%s %s %d", namePrefix, type, index + 1);
                String codeAlias;
                if (namePrefix.equals("Input")) {
                    codeAlias = PythonCodeAliasProvider.getInputObjectCodeAlias(index, type, null);
                } else {
                    codeAlias = PythonCodeAliasProvider.getOutputObjectCodeAlias(index, type, null);
                }
                return new InputOutputModel(inputName, codeAlias, null);
            }).toList();
        }

        private String portTypeToInputOutputType(final PortType portType) {
            if (portType.acceptsPortObjectClass(BufferedDataTable.class)) {
                return INPUT_OUTPUT_TYPE_TABLE;
            } else if (portType.acceptsPortObjectClass(PickledObjectFileStorePortObject.class)) {
                return INPUT_OUTPUT_TYPE_OBJECT;
            } else if (portType.acceptsPortObjectClass(ImagePortObject.class)) {
                return INPUT_OUTPUT_TYPE_IMAGE;
            } else {
                throw new IllegalArgumentException("Unsupported port type: " + portType.getName());
            }
        }

        private static boolean isNoFlowVariablePort(final PortType portType) {
            return !portType.acceptsPortObjectClass(FlowVariablePortObject.class);
        }

        /**
         * @param executableSelection the id of the selected executable option (which might not be available anymore)
         * @return a sorted list of executable options that the user can select from
         */
        public List<ExecutableOption> getExecutableOptionsList(final String executableSelection) {
            final var availableOptions = getExecutableOptions().values().stream().sorted((o1, o2) -> {
                if (o1.type != o2.type) {
                    return o1.type.compareTo(o2.type);
                }
                return o1.id.compareTo(o2.id);
            }).collect(Collectors.toList());

            if (!getExecutableOptions().containsKey(executableSelection)) {
                // The selected option is not available: String variable selected or variable missing
                // We add the option to the available options such that the frontend can display it nicely
                availableOptions.add(getExecutableOption(executableSelection));
            }

            return availableOptions;
        }

        /**
         * @param id the identifier of the executable option
         * @return information about the executable
         */
        public ExecutableInfo getExecutableInfo(final String id) {
            return ExecutableSelectionUtils.getExecutableInfo(getExecutableOption(id));
        }

        /**
         * @param executableSelection the identifier of the active executable option
         * @return the path to the Python executable. Only to be used for configuring the LSP server.
         */
        public String getLanguageServerConfig(final String executableSelection) {
            var executablePath = ExecutableSelectionUtils.getPythonCommand(getExecutableOption(executableSelection))
                .getPythonExecutablePath().toAbsolutePath().toString();
            var extraPaths = PythonScriptingSession.getExtraPythonPaths().stream() //
                .map(Path::toAbsolutePath) //
                .map(Path::toString) //
                .toList();
            return PythonLanguageServer.getConfig(executablePath, extraPaths);
        }

        /**
         * Ask the AI assistant to generate new code based on the user prompt and the current code.
         *
         * The result will be sent to JS as event with identifier "codeSuggestion".
         *
         * @param userPrompt Description of what the user wants the code to do
         * @param currentCode The current code
         */
        public void suggestCode(final String userPrompt, final String currentCode) {
            new Thread(() -> suggestCodeAsync(userPrompt, currentCode)).start();

        }

        private void suggestCodeAsync(final String userPrompt, final String currentCode) {
            try {
                var response = PythonCodeAssistant.generateCode(//
                    userPrompt, //
                    currentCode, //
                    getWorkflowControl().getInputSpec(), //
                    getWorkflowControl().getOutputPortTypes(), //
                    getFlowVariables(), //
                    m_hasView);
                sendEvent("codeSuggestion", new CodeSuggestion(CodeSuggestionStatus.SUCCESS, response, null));
            } catch (IOException ex) { // NOSONAR
                sendEvent("codeSuggestion", new CodeSuggestion(CodeSuggestionStatus.ERROR, null, ex.getMessage()));
            }
        }

        private InputOutputModel createFromPortSpec(final int index, final String displayName) {
            var name = String.format("Input %s %d", displayName, index + 1);
            return new InputOutputModel(name, PythonCodeAliasProvider.getInputObjectCodeAlias(index, displayName, null),
                null);
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

    /** Information about an Python executable */
    public static class ExecutableInfo {
        public final String pythonVersion; // NOSONAR

        public final List<CondaPackageInfo> packages; // NOSONAR

        @SuppressWarnings("hiding")
        ExecutableInfo(final String pythonVersion, final List<CondaPackageInfo> packages) {
            this.pythonVersion = pythonVersion;
            this.packages = packages;
        }
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

    /** Information about a table input port */

    enum CodeSuggestionStatus {
            SUCCESS, ERROR
    }

    /** Information about a code suggestion */
    static record CodeSuggestion(CodeSuggestionStatus status, String code, String error) {
    }

    private static final class PythonCodeAliasProvider {
        private PythonCodeAliasProvider() {
        }

        private static String getOutputViewCodeAlias() {
            return "knio.output_view"; // NOSONAR: we use a method for consistency
        }

        private static String appendStringSuffix(final String prefix, final String variableName) {
            return String.format("%s[\"%s\"]", prefix, variableName);
        }

        private static String appendIndexSuffix(final String prefix, final int index) {
            return String.format("%s[%d]", prefix, index);
        }

        public static String getFlowVariableCodeAlias(final String flowVariableName) {
            if (flowVariableName == null) {
                return "knio.flow_variables";
            }
            return appendStringSuffix("knio.flow_variables", flowVariableName);
        }

        public static String getInputObjectCodeAlias(final int index, final String type, final String subItemName) {
            switch (type) {
                case INPUT_OUTPUT_TYPE_TABLE: {
                    var tableAlias = appendIndexSuffix("knio.input_tables", index);
                    if (subItemName == null) {
                        return tableAlias;
                    }
                    return appendStringSuffix(tableAlias, subItemName);
                }
                case INPUT_OUTPUT_TYPE_OBJECT: {
                    return appendIndexSuffix("knio.input_objects", index);
                }
                default:
                    throw new IllegalArgumentException("Unexpected input object type: " + type);
            }
        }

        public static String getOutputObjectCodeAlias(final int index, final String type, final String subItemName) {
            switch (type) {
                case INPUT_OUTPUT_TYPE_TABLE: {
                    var tableAlias = appendIndexSuffix("knio.output_tables", index);
                    if (subItemName == null) {
                        return tableAlias;
                    }
                    return appendStringSuffix(tableAlias, subItemName);
                }
                case INPUT_OUTPUT_TYPE_OBJECT: {
                    return appendIndexSuffix("knio.output_objects", index);
                }
                case INPUT_OUTPUT_TYPE_IMAGE: {
                    return appendIndexSuffix("knio.output_images", index);
                }
                default:
                    throw new IllegalArgumentException("Unexpected input object type: " + type);
            }
        }
    }

    private static final class DialogFileStoreHandlerSupplier implements FileStoreHandlerSupplier {

        private IWriteFileStoreHandler m_temporaryWriteFileStoreHandler;

        @Override
        public IWriteFileStoreHandler getWriteFileStoreHandler() {
            if (m_temporaryWriteFileStoreHandler == null) {
                m_temporaryWriteFileStoreHandler = NotInWorkflowWriteFileStoreHandler.create();
            }
            return m_temporaryWriteFileStoreHandler;
        }

        @Override
        public IFileStoreHandler getFileStoreHandler(final FileStoreKey key) {
            // The file store can be coming from a previously written table -- then it is part of
            // the WorkflowDataRepositoryor it could have been created during execution of the Python
            // script, then it was using the NotInWorkflowWriteFileStoreHandler

            var dataRepository = NodeContext.getContext().getWorkflowManager().getWorkflowDataRepository();
            var handler = dataRepository.getHandler(key.getStoreUUID());

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
