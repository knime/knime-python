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
 *   Sep 18, 2025 (Marc Lehner): created
 */
package org.knime.python3.nodes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.knime.core.node.NodeLogger;
import org.yaml.snakeyaml.Yaml;

/**
 * Represents the information present in a knime.yaml file. Provides useful methods to access inferred properties. Use
 * {@link #fromPath(Path)} or {@link #fromDirectory(Path)} to create an instance from an existing file.
 *
 *
 * @param extensionPath the path to the directory containing knime.yaml
 * @param name the extension name
 * @param groupId the extension group ID
 * @param bundledEnvName the bundled environment name
 * @param version the extension version (will be defaulted to "0.0.0" if null/blank)
 * @param extensionModule the extension module path
 * @author Marc Lehner
 */
record KnimeYaml( //
    Path extensionPath, //
    String name, //
    String groupId, //
    String bundledEnvName, //
    String version, //
    String extensionModule //
) {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(KnimeYaml.class);

    /**
     * Gets the full extension ID by combining group ID and name.
     *
     * @return the extension ID in the format "groupId.name"
     */
    public String getId() {
        return groupId + "." + name;
    }

    /**
     * Gets the resolved module path. Handles both top-level and nested module paths.
     *
     * @return the absolute path to the extension module
     */
    public Path getModulePath() {
        if (extensionModule == null) {
            return null;
        }

        var relativeModulePath = Path.of(extensionModule);
        if (relativeModulePath.getParent() == null) {
            // the extension module is top level in the extension folder next to the knime.yml
            return extensionPath.resolve(extensionModule);
        } else {
            // the extension module is in a potentially nested subfolder
            return extensionPath.resolve(relativeModulePath);
        }
    }

    /**
     * Parses a knime.yaml file from the given path.
     *
     * @param knimeYamlPath the path to the knime.yaml file
     * @return the parsed KnimeYaml record
     * @throws IOException if the file cannot be read or required fields are missing
     */
    public static KnimeYaml fromPath(final Path knimeYamlPath) throws IOException {
        var yaml = new Yaml();
        try (var inputStream = Files.newInputStream(knimeYamlPath)) {
            Map<String, Object> map = yaml.load(inputStream);

            final var name = (String)map.get("name");
            final var groupId = (String)map.get("group_id");

            // Validate required fields
            if (name == null || groupId == null) {
                throw new IOException("Missing 'name' or 'group_id' in knime.yaml file: " + knimeYamlPath);
            }

            final var bundledEnvName =
                (String)map.getOrDefault("bundled_env_name", groupId.replace('.', '_') + "_" + name);
            var version = (String)map.get("version");
            final var extensionModule = (String)map.get("extension_module");
            final var extensionPath = knimeYamlPath.getParent();

            // Normalize version and warn if missing
            if (version == null || version.isBlank()) {
                version = "0.0.0";
                LOGGER.warnWithFormat(
                    "Missing extension version in knime.yml for extension '%s'; setting version to '0.0.0'", name);
            }

            return new KnimeYaml(extensionPath, name, groupId, bundledEnvName, version, extensionModule);
        }
    }

    /**
     * Parses a knime.yaml file from the directory containing it.
     *
     * @param extensionPath the path to the directory containing knime.yaml
     * @return the parsed KnimeYaml record
     * @throws IOException if the file cannot be read or required fields are missing
     */
    public static KnimeYaml fromDirectory(final Path extensionPath) throws IOException {
        return fromPath(extensionPath.resolve("knime.yml"));
    }
}
