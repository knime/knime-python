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
package org.knime.python3.js.scripting.nodes.script;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.knime.conda.CondaEnvironmentDirectory;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.VariableType;
import org.knime.core.webui.data.DataServiceContext;
import org.knime.python2.port.PickledObjectPortObjectSpec;
import org.knime.python3.js.scripting.nodes.script.PythonScriptingService.ExecutableOption.ExecutableOptionType;
import org.knime.scripting.editor.ScriptingService;
import org.knime.scripting.editor.lsp.LanguageServerProxy;

/**
 * A special {@link ScriptingService} for the Python scripting node.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
final class PythonScriptingService extends ScriptingService {

    private static final HashSet<VariableType<?>> KNOWN_FLOW_VARIABLE_SET =
        new HashSet<>(Arrays.asList(PythonScriptNodeModel.KNOWN_FLOW_VARIABLE_TYPES));

    private static final Predicate<FlowVariable> FLOW_VARIABLE_FILTER =
        x -> KNOWN_FLOW_VARIABLE_SET.contains(x.getVariableType());

    private final PythonScriptPortsConfiguration m_ports;

    private Map<String, ExecutableOption> m_executableOptions = Collections.emptyMap();

    // TODO(AP-19357) close the session when the dialog is closed
    // TODO(AP-19332) should the Python session be started immediately? (or whenever the frontend requests it?)
    private PythonScriptingSession m_interactiveSession;

    /** Create a new {@link PythonScriptingService}. */
    @SuppressWarnings("resource") // TODO(AP-19357) fix this
    PythonScriptingService() {
        super(connectToLanguageServer(), FLOW_VARIABLE_FILTER);
        m_ports = PythonScriptPortsConfiguration.fromCurrentNodeContext();
        // TODO(AP-19357) stop the language server when the dialog is closed
    }

    private static LanguageServerProxy connectToLanguageServer() {
        try {
            // TODO(AP-19338) make language server configurable
            return PythonLanguageServer.instance().connect();
        } catch (final Exception e) {
            // TODO(AP-19338) Handle better if the language server can't be used for any reason
            NodeLogger.getLogger(PythonScriptingService.class).error(e);
            return null;
        }
    }

    private ExecutableOption getExecutableOption(final String id) {
        if (!m_executableOptions.containsKey(id)) {
            final Map<String, FlowVariable> allFlowVars =
                getWorkflowControl().getFlowObjectStack().getAllAvailableFlowVariables();
            if (allFlowVars.containsKey(id)) {
                return getExecutableOptionFromVariable(id, allFlowVars.get(id).getStringValue());
            } else {
                // Missing variable selected
                return new ExecutableOption(ExecutableOptionType.MISSING_VAR, id, null, null, null);
            }
        }
        return m_executableOptions.get(id);
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

    /**
     * An extension of the {@link org.knime.scripting.editor.ScriptingService.RpcService} that provides additional
     * methods to the frontend of the Python scripting node.
     *
     * NB: Must be public for the JSON-RPC server
     */
    public final class PythonRpcService extends RpcService {

        /**
         * Notify that a new dialog has been opened. Must be called before calling any other method of the RPC server.
         */
        public void initExecutableOptions() {
            // Set the executable options with the currently available flow variables
            m_executableOptions =
                ExecutableSelectionUtils.getExecutableOptions(getWorkflowControl().getFlowObjectStack());
        }

        public void sendLastConsoleOutput() {
            // Send the console output of the last execution to the dialog
            try {
                ((PythonScriptNodeModel)getWorkflowControl().getNodeModel())
                    .sendLastConsoleOutputs(PythonScriptingService.this::addConsoleOutputEvent);
            } catch (final Exception e) {
                final var message = "Sending the console output of the last execution to the dialog failed.";
                NodeLogger.getLogger(PythonScriptingService.class).warn(message, e);
                DataServiceContext.get().addWarningMessage(message);
            }
        }

        /**
         * Start the interactive Python session.
         *
         * @param executableSelection the id of the selected executable option
         * @throws Exception
         */
        public void startInteractive(final String executableSelection) throws Exception {
            // TODO(AP-19332) Error handling
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
            m_interactiveSession = new PythonScriptingSession(pythonCommand,
                PythonScriptingService.this::addConsoleOutputEvent, fileStoreHandler);

            m_interactiveSession.setupIO(workflowControl.getInputData(), getFlowVariables(), m_ports.getNumOutTables(),
                m_ports.getNumOutImages(), m_ports.getNumOutObjects(), new ExecutionMonitor());
        }

        /**
         * Run the given script in the running interactive session.
         *
         * @param script the Python script
         * @return the workspace serialized as JSON
         */
        public String runInteractive(final String script) {
            if (m_interactiveSession != null) {
                // TODO(AP-19333) Error handling
                return m_interactiveSession.execute(script);
            } else {
                // TODO(AP-19332)
                throw new IllegalStateException("Please start the session before using it");
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
        public List<ExecutableOption> getExecutableOptions(final String executableSelection) {
            final var availableOptions = m_executableOptions.values().stream().sorted((o1, o2) -> {
                if (o1.type != o2.type) {
                    return o1.type.compareTo(o2.type);
                }
                return o1.id.compareTo(o2.id);
            }).collect(Collectors.toList());

            if (!m_executableOptions.containsKey(executableSelection)) {
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
