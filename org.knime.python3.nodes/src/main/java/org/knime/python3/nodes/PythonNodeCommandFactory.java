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
 *   Apr 22, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.knime.conda.envbundling.environment.CondaEnvironmentRegistry;
import org.knime.conda.prefs.CondaPreferences;
import org.knime.core.node.NodeLogger;
import org.knime.python2.CondaPythonCommand;
import org.knime.python2.PythonVersion;
import org.knime.python3.PythonCommand;
import org.knime.python3.SimplePythonCommand;
import org.knime.python3.scripting.nodes.prefs.BundledPythonCommand;
import org.yaml.snakeyaml.Yaml;

/**
 * Creates {@link PythonCommand} objects for pure Python extensions.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class PythonNodeCommandFactory {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonNodeCommandFactory.class);

    private static final String YML_PROPERTY = "org.knime.python3.nodes.alternate_executable_yml";

    private PythonNodeCommandFactory() {

    }

    static PythonCommand createCommand(final String extensionId, final String environmentName) {
        var command = getCommandIfOverwrittenByUser(extensionId);
        if (command != null) {
            return command;
        } else {
            return getPythonCommandForEnvironment(environmentName);
        }
    }

    static PythonCommand getCommandIfOverwrittenByUser(final String extensionId) {
        // TODO implement a preference page that allows to specify this in a convenient way
        return getCommandSpecifiedViaYmlProperty(extensionId);
    }

    private static PythonCommand getPythonCommandForEnvironment(final String environmentName) {
        var environment = CondaEnvironmentRegistry.getEnvironment(environmentName);
        var legacyBundledCondaCommand = new BundledPythonCommand(environment.getPath().toAbsolutePath().toString());
        return new LegacyPythonCommandAdapter(legacyBundledCondaCommand);
    }

    private static PythonCommand getCommandSpecifiedViaYmlProperty(final String extensionId) {
        var pathToYml = System.getProperty(YML_PROPERTY);
        if (pathToYml == null) {
            return null;
        }
        var extensionsWithAlternateCommand = parseYml(pathToYml);
        if (extensionsWithAlternateCommand == null) {
            return null;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> envForExtension = (Map<String, Object>)extensionsWithAlternateCommand.get(extensionId);
        if (envForExtension == null) {
            return null;
        }
        return createAlternateCommand(envForExtension);
    }

    private static Map<String, Object> parseYml(final String pathToYml) {
        var yaml = new Yaml();
        try (var inputStream = Files.newInputStream(Path.of(pathToYml))) {
            return yaml.<Map<String, Object>>load(inputStream);
        } catch (IOException ex) {
            LOGGER.error(String.format("Failed to read the yml '%s' specified via the system property '%s'.", pathToYml,
                YML_PROPERTY), ex);
            return null;
        }
    }

    private static PythonCommand createAlternateCommand(final Map<String, Object> envForExtension) {
        var envType = envForExtension.get("type");
        if (envType == null) {
            LOGGER.error("No environment type specified. Falling back to the bundled environment.");
            return null;
        } else if ("conda".equals(envType)) {
            return createCondaCommand(envForExtension);
        } else if ("manual".equals(envType)) {
            return createManualCommand(envForExtension);
        } else {
            LOGGER.errorWithFormat(
                "Unknown environment type '%s' encountered. Falling back to the bundled environment.", envType);
            return null;
        }
    }

    private static PythonCommand createCondaCommand(final Map<String, Object> envForExtension) {
        String envPath = (String)envForExtension.get("environment_path");
        if (envPath == null) {
            LOGGER.errorWithFormat("No environment_path specified. Falling back to bundled environment.");
            return null;
        }
        final var resolvedPath = Path.of(envPath);
        if (!Files.exists(resolvedPath)) {
            LOGGER.errorWithFormat("The specified environment_path '%s' does not exist.", envPath);
        }
        LOGGER.debugWithFormat("Creating CondaPythonCommand with environment located at '%s'.", resolvedPath);
        // we use resolvedPath.toString() instead of envPath to avoid issues with the path separator on windows
        return new LegacyPythonCommandAdapter(new CondaPythonCommand(PythonVersion.PYTHON3,
            CondaPreferences.getCondaInstallationDirectory(), resolvedPath.toString()));
    }

    private static PythonCommand createManualCommand(final Map<String, Object> envForExtension) {
        String command = (String)envForExtension.get("command_path");
        if (command == null) {
            LOGGER.errorWithFormat("No command_path specified. Falling back to bundled environment.");
            return null;
        }
        if (!Files.exists(Path.of(command))) {
            LOGGER.errorWithFormat("The specified command_path '%s' does not exist.", command);
        }
        LOGGER.debugWithFormat("Creating manual PythonCommand with command located at '%s'", command);
        return new SimplePythonCommand(List.of(command));
    }

    /* TODO Discuss the future of PythonCommands
     * - Separate for python2 and python3 (code duplication)
     * - One PythonPath in a common PythonUtils plugin (might become difficult to maintain)
     * - Acceptance of python2 dependency (probably not)
     */
    private static final class LegacyPythonCommandAdapter implements PythonCommand {

        private final org.knime.python2.PythonCommand m_legacyCommand;

        LegacyPythonCommandAdapter(final org.knime.python2.PythonCommand legacyCommand) {
            m_legacyCommand = legacyCommand;
        }

        @Override
        public ProcessBuilder createProcessBuilder() {
            return m_legacyCommand.createProcessBuilder();
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            } else if (obj instanceof LegacyPythonCommandAdapter) {
                var other = (LegacyPythonCommandAdapter)obj;
                return m_legacyCommand.equals(other.m_legacyCommand);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return m_legacyCommand.hashCode();
        }

    }
}
