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
package org.knime.python3.scripting.nodes2.script;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.knime.conda.CondaEnvironmentDirectory;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.VariableType;
import org.knime.core.webui.data.DataServiceContext;
import org.knime.python2.port.PickledObjectPortObjectSpec;
import org.knime.python3.scripting.nodes2.script.PythonScriptingService.ExecutableOption.ExecutableOptionType;
import org.knime.python3.scripting.nodes2.script.PythonScriptingSession.ExecutionInfo;
import org.knime.python3.scripting.nodes2.script.PythonScriptingSession.ExecutionStatus;
import org.knime.scripting.editor.ScriptingService;

/**
 * A special {@link ScriptingService} for the Python scripting node.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
final class PythonScriptingService extends ScriptingService {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonScriptingService.class);

    private static final HashSet<VariableType<?>> KNOWN_FLOW_VARIABLE_SET =
        new HashSet<>(Arrays.asList(PythonScriptNodeModel.KNOWN_FLOW_VARIABLE_TYPES));

    private static final Predicate<FlowVariable> FLOW_VARIABLE_FILTER =
        x -> KNOWN_FLOW_VARIABLE_SET.contains(x.getVariableType());

    private final PythonScriptPortsConfiguration m_ports;

    private Map<String, ExecutableOption> m_executableOptions = Collections.emptyMap();

    // indicates that killSession has been called
    private AtomicBoolean m_expectCancel;

    // TODO(AP-19357) close the session when the dialog is closed
    private PythonScriptingSession m_interactiveSession;

    /** Create a new {@link PythonScriptingService}. */
    PythonScriptingService() {
        super(PythonLanguageServer::startPythonLSP, FLOW_VARIABLE_FILTER);
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

    /**
     * An extension of the {@link org.knime.scripting.editor.ScriptingService.RpcService} that provides additional
     * methods to the frontend of the Python scripting node.
     *
     * NB: Must be public for the JSON-RPC server
     */
    public final class PythonRpcService extends RpcService {

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

        /**
         * Start the interactive Python session.
         *
         * @param executableSelection the id of the selected executable option
         * @return Information about the execution status
         * @throws Exception
         */
        public StartSessionInfo startInteractive(final String executableSelection) throws Exception {
            if (m_interactiveSession != null) {
                m_interactiveSession.close();
                m_interactiveSession = null;
            }

            // Start the interactive Python session and setup the IO
            final var workflowControl = getWorkflowControl();
            final var pythonCommand =
                ExecutableSelectionUtils.getPythonCommand(getExecutableOption(executableSelection));

            // TODO do we need to do more with the NotInWorkflowWriteFileStoreHandler?
            // TODO report the progress of converting the tables using the ExecutionMonitor?
            final var fileStoreHandler = NotInWorkflowWriteFileStoreHandler.create();
            try {
                m_interactiveSession = new PythonScriptingSession(pythonCommand,
                    PythonScriptingService.this::addConsoleOutputEvent, fileStoreHandler);
                m_interactiveSession.setupIO(workflowControl.getInputData(), getFlowVariables(),
                    m_ports.getNumOutTables(), m_ports.getNumOutImages(), m_ports.getNumOutObjects(),
                    new ExecutionMonitor());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new StartSessionInfo(StartSessionStatus.ERROR, e.getMessage());
            } catch (IOException | CanceledExecutionException | IllegalArgumentException e) {
                return new StartSessionInfo(StartSessionStatus.ERROR, e.getMessage());
            }
            return new StartSessionInfo(StartSessionStatus.SUCCESS, "Started new python session");
        }

        /**
         * Called from frontend during execution. Stops execution and eventually throws py4j error in runInteractive.
         *
         * @return whether the session has been killed successfully or not
         *
         */
        public KillSessionInfo killSession() {
            if (m_interactiveSession == null) {
                return new KillSessionInfo(KillSessionStatus.ERROR, "There is no active python session in progress");
            }
            try {
                m_expectCancel.set(true);
                m_interactiveSession.close();
                return new KillSessionInfo(KillSessionStatus.SUCCESS, "Stopped execution of python session.");
            } catch (final IOException e) {
                LOGGER.error(e);
                return new KillSessionInfo(KillSessionStatus.ERROR, String
                    .format("An error occurred when stopping the execution of the python session: %s", e.getMessage()));
            } finally {
                m_interactiveSession = null;
            }
        }

        /**
         * Run the given script in the running interactive session.
         *
         * @param script the Python script
         * @param checkOutputs
         * @return the workspace serialized as JSON
         */
        public ExecutionInfo runInteractive(final String script, final boolean checkOutputs) {

            if (m_interactiveSession == null) {
                return new ExecutionInfo(ExecutionStatus.FATAL_ERROR, "Session not available");
            }

            try {
                return m_interactiveSession.execute(script, checkOutputs);
            } catch (final Exception e) { // NOSONAR
                if (m_expectCancel.get()) {
                    return new ExecutionInfo(ExecutionStatus.CANCELLED, "Script execution was cancelled by user");
                } else {
                    LOGGER.error(e);
                    return new ExecutionInfo(ExecutionStatus.FATAL_ERROR, "Execution failed");
                }
            } finally {
                m_expectCancel.set(false);
            }
        }

        /**
         * @return information about all input ports that are available to the script
         */
        public InputPortInfo[] getInputObjects() {
            final var inputSpec = getWorkflowControl().getInputSpec();
            final var inputInfos = new InputPortInfo[inputSpec.length];

            int tableIdx = 0;
            int objectIdx = 0;
            for (int i = 0; i < inputSpec.length; i++) {
                final var spec = inputSpec[i];
                if (spec instanceof DataTableSpec) {
                    inputInfos[i] = InputTableInfo.createFromTableSpec(tableIdx, (DataTableSpec)spec);
                    tableIdx++;
                } else if (spec instanceof PickledObjectPortObjectSpec) {
                    inputInfos[i] = InputObjectInfo.createFromPortSpec(objectIdx, (PickledObjectPortObjectSpec)spec);
                    objectIdx++;
                } else {
                    throw new IllegalStateException("Unsupported input port. This is an implementation error.");
                }
            }
            return inputInfos;
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
        public String getLSPConfig(final String executableSelection) {
            var executablePath = ExecutableSelectionUtils.getPythonCommand(getExecutableOption(executableSelection))
                .getPythonExecutablePath().toAbsolutePath().toString();
            var extraPaths = PythonScriptingSession.getExtraPythonPaths().stream() //
                .map(Path::toAbsolutePath) //
                .map(Path::toString) //
                .toList();
            return PythonLanguageServer.getLSPConfig(executablePath, extraPaths);
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

    /** Information about an input port */
    public static class InputPortInfo { // NOSONAR: Only subclassed in this file
        public final String type; // NOSONAR

        public final String variableName; // NOSONAR

        @SuppressWarnings("hiding")
        private InputPortInfo(final String type, final String variableName) {
            this.type = type;
            this.variableName = variableName;
        }
    }

    /** Information about a table input port */
    public static final class InputTableInfo extends InputPortInfo {

        public final String[] columnNames; // NOSONAR

        public final String[] columnTypes; // NOSONAR

        @SuppressWarnings("hiding")
        private InputTableInfo(final int tableIdx, final String[] columnNames, final String[] columnTypes) {
            super("table", "knio.input_tables[" + tableIdx + "]");
            this.columnNames = columnNames;
            this.columnTypes = columnTypes;
        }

        private static InputTableInfo createFromTableSpec(final int tableIdx, final DataTableSpec spec) {
            final var columnNames = spec.getColumnNames();
            final var columnTypes = IntStream.range(0, spec.getNumColumns())
                .mapToObj(i -> spec.getColumnSpec(i).getType().getName()).toArray(String[]::new);
            return new InputTableInfo(tableIdx, columnNames, columnTypes);
        }
    }

    /** Information about a pickled object input port */
    public static final class InputObjectInfo extends InputPortInfo {

        public final String objectType; // NOSONAR

        public final String objectRepr; // NOSONAR

        @SuppressWarnings("hiding")
        public InputObjectInfo(final int objectIdx, final String objectType, final String objectRepr) {
            super("object", "knio.input_objects[" + objectIdx + "]");
            this.objectType = objectType;
            this.objectRepr = objectRepr;
        }

        private static InputObjectInfo createFromPortSpec(final int objectIdx, final PickledObjectPortObjectSpec spec) {
            return new InputObjectInfo(objectIdx, spec.getType(), spec.getRepresentation());
        }
    }
}
