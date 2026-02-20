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
 *   May 6, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import org.knime.conda.prefs.CondaPreferences;
import org.knime.core.node.NodeLogger;
import org.knime.externalprocessprovider.ExternalProcessProvider;
import org.knime.python3.CondaPythonCommand;
import org.knime.python3.SimplePythonCommand;
import org.yaml.snakeyaml.Yaml;

/**
 * Contains configurations that users supplied via a custom yml file.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class PythonExtensionPreferences {

    private PythonExtensionPreferences() {

    }

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonExtensionPreferences.class);

    private static final String PY_EXTENSIONS_YML_PROPERTY = "knime.python.extension.config";

    private static final String PY_EXTENSIONS_DEBUG_KNIME_YAML_LIST_PROPERTY =
        "knime.python.extension.debug_knime_yaml_list";

    static Stream<Path> getPathsToCustomExtensions() {
        return loadConfigs()//
            .map(ExtensionConfig::getSrcPath)//
            .filter(Optional::isPresent)//
            .map(Optional::get);
    }

    static Optional<ExternalProcessProvider> getCustomPythonCommand(final String extensionId) {
        return loadConfigs()//
            .filter(e -> extensionId.equals(e.m_id))//
            .findFirst()//
            .flatMap(ExtensionConfig::getCommand);
    }

    static boolean debugMode(final String extensionId) {
        return loadConfigs()//
            .filter(e -> extensionId.equals(e.m_id))//
            .map(ExtensionConfig::debugMode)//
            .findFirst()//
            // cache the gateway if not told otherwise
            .orElse(false);
    }

    /**
     * Whether the extension has a custom source path set. Used to determine whether the node list can be cached.
     *
     * @param extensionId
     * @return True if a custom source path is set.
     */
    public static boolean hasCustomSrcPath(final String extensionId) {
        return loadConfigs()//
            .filter(e -> extensionId.equals(e.m_id))//
            .map(ExtensionConfig::getSrcPath)//
            .map(Optional::isPresent)//
            .findFirst()//
            .orElse(false);
    }

    private static Stream<ExtensionConfig> loadConfigs() {
        // First, try to load from the original config.yml approach
        Stream<ExtensionConfig> configYmlStream = loadConfigsFromYml();

        // Then, try to load from the new debug sources list approach
        Stream<ExtensionConfig> debugKnimeYamlStream = loadConfigsFromDebugKnimeYamlList();

        // Combine both streams
        return Stream.concat(configYmlStream, debugKnimeYamlStream);
    }

    private static Stream<ExtensionConfig> loadConfigsFromYml() {
        var pathToYml = System.getProperty(PY_EXTENSIONS_YML_PROPERTY);
        if (pathToYml == null) {
            return Stream.empty();
        } else {
            try {
                return parseYmlFile(Path.of(pathToYml));
            } catch (IOException ex) {
                LOGGER.error("Failed to read Python extension preference yml file at " + pathToYml, ex);
                return Stream.empty();
            }
        }
    }

    private static Stream<ExtensionConfig> loadConfigsFromDebugKnimeYamlList() {
        var debugKnimeYamlPaths = System.getProperty(PY_EXTENSIONS_DEBUG_KNIME_YAML_LIST_PROPERTY);
        if (debugKnimeYamlPaths == null) {
            return Stream.empty();
        } else {
            return Stream.of(debugKnimeYamlPaths.split(File.pathSeparator)) // NOSONAR - pathSeparator is not a regex
                .map(String::trim) //
                .filter(path -> !path.isEmpty()) //
                .flatMap(path -> {
                    try {
                        return Stream.of(loadConfigFromKnimeYaml(Path.of(path)));
                    } catch (IOException ex) {
                        LOGGER.error("Failed to read knime.yml file at " + path, ex);
                        return Stream.empty();
                    }
                });
        }
    }

    private static Stream<ExtensionConfig> parseYmlFile(final Path pathToYml) throws IOException {
        try (var inputStream = Files.newInputStream(pathToYml)) {
            var yml = new Yaml();
            Map<String, Object> configs = yml.load(inputStream);
            return configs.entrySet().stream()//
                .map(PythonExtensionPreferences::mapToConfig)//
                .filter(Objects::nonNull);
        }
    }

    private static ExtensionConfig loadConfigFromKnimeYaml(final Path pathToKnimeYaml) throws IOException {
        // Use the shared KnimeYaml parser instead of duplicating logic
        var knimeYaml = KnimeYaml.fromPath(pathToKnimeYaml);

        var pixiEnvPath = knimeYaml.extensionPath().resolve(".pixi/envs/default").toAbsolutePath();

        // Validate that the pixi environment exists and contains a valid Python installation
        validatePixiEnvironment(pixiEnvPath, knimeYaml.getId());

        return new ExtensionConfig( //
            knimeYaml.getId(), //
            knimeYaml.extensionPath().toString(), //
            pixiEnvPath.toString(), //
            null, // no python_executable for knime.yaml approach
            true // assume debug_mode=true for knime.yaml approach
        );
    }

    /**
     * Validates that the pixi environment path exists and contains a valid Python installation.
     *
     * @param pixiEnvPath the path to the pixi environment
     * @param extensionId the extension ID for error reporting
     * @throws IOException if the environment is invalid or doesn't exist
     */
    private static void validatePixiEnvironment(final Path pixiEnvPath, final String extensionId) throws IOException {
        LOGGER.debugWithFormat("Validating pixi environment for extension '%s' at path: %s", extensionId, pixiEnvPath);

        if (!Files.exists(pixiEnvPath)) {
            throw new IOException(String.format(
                "Pixi environment not found for extension '%s' at path: %s. "
                    + "Please ensure 'pixi install' has been run in the extension directory.",
                extensionId, pixiEnvPath));
        }

        // Find the Python executable in the pixi environment
        var pythonExecutable = findPixiPythonExecutable(pixiEnvPath);
        if (pythonExecutable == null) {
            throw new IOException(
                String.format(
                    "Python executable not found in pixi environment for extension '%s' at path: %s. "
                        + "Expected to find python or python.exe in the environment directory.",
                    extensionId, pixiEnvPath));
        }

        // Validate that the Python executable exists and is accessible
        if (!Files.exists(pythonExecutable) || !Files.isExecutable(pythonExecutable)) {
            throw new IOException(
                String.format("Python executable not accessible in pixi environment for extension '%s': %s",
                    extensionId, pythonExecutable));
        }

        LOGGER.debugWithFormat("Successfully validated pixi environment for extension '%s' at: %s with Python: %s",
            extensionId, pixiEnvPath, pythonExecutable);
    }

    /**
     * Finds the Python executable in a pixi environment. Pixi environments typically have the Python executable
     * directly in the environment directory.
     *
     * @param pixiEnvPath the path to the pixi environment
     * @return the path to the Python executable, or null if not found
     */
    private static Path findPixiPythonExecutable(final Path pixiEnvPath) {
        // Check common locations for Python executable in pixi environments
        var candidates = List.of(pixiEnvPath.resolve("python.exe"), // Windows
            pixiEnvPath.resolve("python"), // Linux/macOS
            pixiEnvPath.resolve("bin").resolve("python.exe"), // Windows with bin
            pixiEnvPath.resolve("bin").resolve("python") // Linux/macOS with bin
        );

        for (var candidate : candidates) {
            if (Files.exists(candidate)) {
                return candidate;
            }
        }

        return null;
    }

    private static ExtensionConfig mapToConfig(final Entry<String, Object> configEntry) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>)configEntry.getValue();
            // cache gateways if not told otherwise
            var debugMode = map.containsKey("debug_mode") && (boolean)map.get("debug_mode");
            return new ExtensionConfig(//
                configEntry.getKey(), //
                (String)map.get("src"), //
                (String)map.get("conda_env_path"), //
                (String)map.get("python_executable"), //
                debugMode//
            );
        } catch (RuntimeException ex) {
            LOGGER.errorWithFormat("Failed to parse Python extension config.", ex);
            return null;
        }
    }

    private static final class ExtensionConfig {
        private String m_id;

        private String m_src;

        private String m_condaEnvPath;

        private String m_pythonExecutable;

        private boolean m_debugMode;

        ExtensionConfig(final String id, final String src, final String condaEnvPath, final String pythonExecutable,
            final boolean debugMode) {
            m_id = id;
            m_src = src;
            m_condaEnvPath = condaEnvPath;
            m_pythonExecutable = pythonExecutable;
            m_debugMode = debugMode;
        }

        boolean debugMode() {
            return m_debugMode;
        }

        Optional<Path> getSrcPath() {
            if (m_src == null) {
                return Optional.empty();
            } else {
                return Optional.of(Path.of(m_src));
            }
        }

        Optional<ExternalProcessProvider> getCommand() {
            if (m_condaEnvPath != null) {
                if (m_pythonExecutable != null) {
                    LOGGER.warnWithFormat("Both conda_env_path and python_executable are provided for extension '%s'."
                        + " The conda_env_path takes precedence.", m_id);
                }
                return Optional
                    .of(new CondaPythonCommand(CondaPreferences.getCondaInstallationDirectory(), m_condaEnvPath));
            } else if (m_pythonExecutable != null) {
                return Optional.of(new SimplePythonCommand(List.of(m_pythonExecutable)));
            } else {
                return Optional.empty();
            }
        }

    }
}
