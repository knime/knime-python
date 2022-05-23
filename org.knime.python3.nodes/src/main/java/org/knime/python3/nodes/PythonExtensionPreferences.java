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
import org.knime.python2.CondaPythonCommand;
import org.knime.python2.PythonVersion;
import org.knime.python3.PythonCommand;
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

    static Stream<Path> getPathsToCustomExtensions() {
        return loadConfigs()//
            .map(ExtensionConfig::getSrcPath)//
            .filter(Optional::isPresent)//
            .map(Optional::get);
    }

    static Optional<PythonCommand> getCustomPythonCommand(final String extensionId) {
        return loadConfigs()//
            .filter(e -> extensionId.equals(e.m_id))//
            .findFirst()//
            .flatMap(ExtensionConfig::getCommand);
    }

    static boolean cacheGateway(final String extensionId) {
        return loadConfigs()//
                .filter(e -> extensionId.equals(e.m_id))//
                .map(ExtensionConfig::cacheGateway)//
                .findFirst()//
                // cache the gateway if not told otherwise
                .orElse(true);
    }

    private static Stream<ExtensionConfig> loadConfigs() {
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

    private static Stream<ExtensionConfig> parseYmlFile(final Path pathToYml) throws IOException {
        try (var inputStream = Files.newInputStream(pathToYml)) {
            var yml = new Yaml();
            Map<String, Object> configs = yml.load(inputStream);
            return configs.entrySet().stream()//
                    .map(PythonExtensionPreferences::mapToConfig)//
                    .filter(Objects::nonNull);
        }
    }

    private static ExtensionConfig mapToConfig(final Entry<String, Object> configEntry) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>)configEntry.getValue();
            // cache gateways if not told otherwise
            var cacheGateway = map.containsKey("cache_gateway") ? (boolean)map.get("cache_gateway") : true;
            return new ExtensionConfig(//
                configEntry.getKey(), //
                (String)map.get("src"), //
                (String)map.get("conda_env_path"), //
                (String)map.get("python_executable"),//
                cacheGateway//
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

        private boolean m_cacheGateway;

        ExtensionConfig(final String id, final String src, final String condaEnvPath, final String pythonExecutable,
            final boolean cacheNonExecutionConfig) {
            m_id = id;
            m_src = src;
            m_condaEnvPath = condaEnvPath;
            m_pythonExecutable = pythonExecutable;
            m_cacheGateway = cacheNonExecutionConfig;
        }

        boolean cacheGateway() {
            return m_cacheGateway;
        }

        Optional<Path> getSrcPath() {
            if (m_src == null) {
                return Optional.empty();
            } else {
                return Optional.of(Path.of(m_src));
            }
        }

        Optional<PythonCommand> getCommand() {
            if (m_condaEnvPath != null) {
                if (m_pythonExecutable != null) {
                    LOGGER.warnWithFormat("Both conda_env_path and python_executable are provided for extension '%s'."
                        + " The conda_env_path takes precedence.", m_id);
                }
                return Optional.of(new LegacyPythonCommandAdapter(new CondaPythonCommand(PythonVersion.PYTHON3,
                    CondaPreferences.getCondaInstallationDirectory(), m_condaEnvPath)));
            } else if (m_pythonExecutable != null) {
                return Optional.of(new SimplePythonCommand(List.of(m_pythonExecutable)));
            } else {
                return Optional.empty();
            }
        }

    }
}
