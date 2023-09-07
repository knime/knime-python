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
 *   Aug 17, 2022 (benjamin): created
 */
package org.knime.python3.scripting.nodes2.script;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.knime.conda.CondaEnvironmentDirectory;
import org.knime.conda.CondaEnvironmentPropagation.CondaEnvironmentType;
import org.knime.conda.prefs.CondaPreferences;
import org.knime.core.node.workflow.FlowObjectStack;
import org.knime.python3.CondaPythonCommand;
import org.knime.python3.PythonCommand;
import org.knime.python3.SimplePythonCommand;
import org.knime.python3.scripting.nodes.prefs.Python3ScriptingPreferences;
import org.knime.python3.scripting.nodes2.script.PythonScriptingService.CondaPackageInfo;
import org.knime.python3.scripting.nodes2.script.PythonScriptingService.ExecutableInfo;
import org.knime.python3.scripting.nodes2.script.PythonScriptingService.ExecutableOption;
import org.knime.python3.scripting.nodes2.script.PythonScriptingService.ExecutableOption.ExecutableOptionType;

final class ExecutableSelectionUtils {

    /** The id that is used to indicate that the preferences should be used */
    static final String EXEC_SELECTION_PREF_ID = "";

    private ExecutableSelectionUtils() {
        // Utility class
    }

    static Map<String, ExecutableOption> getExecutableOptions(final FlowObjectStack flowVarStack) {
        return Stream.concat( //
            Stream.of(getPreferenceOption()), //
            getCondaFlowVariableOptions(flowVarStack) //
        ).collect(Collectors.toMap(o -> o.id, Function.identity()));
    }

    static ExecutableInfo getExecutableInfo(final ExecutableOption option) {
        final ExecutableInfo specs;

        // TODO(AP-19432) Get the real data from the flow variable or by calling Conda
        if (EXEC_SELECTION_PREF_ID.equals(option.id)) {
            specs = new ExecutableInfo( //
                "3.18.0", //
                List.of( //
                    new CondaPackageInfo("conda", "4.13.0", "py39h06a4308_0", ""), //
                    new CondaPackageInfo("conda-build", "3.21.9", "py39hf3d152e_0", "conda-forge"), //
                    new CondaPackageInfo("conda-package-handling", "1.8.1", "py39h7f8727e_0", ""), //
                    new CondaPackageInfo("cryptography", "37.0.1", "py39h9ce1e76_0", ""), //
                    new CondaPackageInfo("filelock", "3.6.0", "pyhd3eb1b0_0", "") //
                ));
        } else {
            specs = new ExecutableInfo(//
                null, //
                List.of( //
                    new CondaPackageInfo("conda", "4.13.0", "py39h06a4308_0", "") //
                ));
        }

        // TODO(AP-19432) remove this
        try {
            // Wait for 1s to simulate that it takes time to get the specs
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return specs;
    }

    /** Get the PythonCommand from the selected option */
    static PythonCommand getPythonCommand(final ExecutableOption option) {
        switch (option.type) {
            case CONDA_ENV_VAR:
                return commandForConda(option.condaEnvDir);

            case STRING_VAR:
                if (option.condaEnvDir != null && !option.condaEnvDir.isEmpty()) {
                    return commandForConda(option.condaEnvDir);
                } else {
                    return commandForString(option.pythonExecutable);
                }

            case MISSING_VAR:
                throw new UnsupportedOperationException(
                    "Cannot get Python command because the selected variable '" + option.id + "' is missing.");

            default:
                // All other options are for preferences
                return commandForPreferences();
        }
    }

    /** Get the PythonCommand from the given settings String */
    static PythonCommand getPythonCommand(final String commandString) {
        if (commandString == null || EXEC_SELECTION_PREF_ID.equals(commandString)) {
            // Nothing configured -> Use preferences
            return commandForPreferences();
        } else if (isPathToCondaEnv(commandString)) {
            // Is a directory -> must be a Conda directory
            return commandForConda(commandString);
        } else {
            // Not a directory -> just use the string as the path to the Python executable
            return commandForString(commandString);
        }
    }

    /** @return true if the path points to a directory */
    static boolean isPathToCondaEnv(final String commandString) {
        return Files.isDirectory(Paths.get(commandString));
    }

    private static ExecutableOption getPreferenceOption() {
        // TODO(AP-19391) Get the information from the preferences
        return new ExecutableOption(ExecutableOptionType.PREF_BUNDLED, EXEC_SELECTION_PREF_ID, "/foo/bar/python", null,
            null);
    }

    private static Stream<ExecutableOption> getCondaFlowVariableOptions(final FlowObjectStack flowVarStack) {
        if (flowVarStack != null) {
            return flowVarStack //
                .getAvailableFlowVariables(CondaEnvironmentType.INSTANCE) //
                .entrySet() //
                .stream() //
                .map(e -> {
                    var flowVarName = e.getKey();
                    var env = e.getValue().getValue(CondaEnvironmentType.INSTANCE).getIdentifier();
                    return new ExecutableOption(ExecutableOptionType.CONDA_ENV_VAR, flowVarName,
                        CondaEnvironmentDirectory.getPythonExecutableString(env.getDirectoryPath()), env.getName(),
                        env.getDirectoryPath());
                }); //
        } else {
            return Stream.empty();
        }
    }

    private static PythonCommand commandForConda(final String condaEnvDir) {
        return new CondaPythonCommand(CondaPreferences.getCondaInstallationDirectory(), condaEnvDir);
    }

    private static PythonCommand commandForString(final String pythonExecutable) {
        return new SimplePythonCommand(pythonExecutable);
    }

    private static PythonCommand commandForPreferences() {
        return Python3ScriptingPreferences.getPythonCommandPreference();
    }
}
