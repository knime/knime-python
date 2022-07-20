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
 *   Jan 20, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes.settings;

import org.knime.base.views.node.defaultdialog.JsonNodeSettingsMapperUtil;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModel;

/**
 * Represents node settings that are created as JSON and stored as NodeSettings.</br>
 * The settings consist of the actual parameters of the node as well as the version of AP the settings were created
 * with.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class JsonNodeSettings {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(JsonNodeSettings.class);

    private static final String CFG_VERSION = "version" + SettingsModel.CFGKEY_INTERNAL;

    private final String m_parameters;

    private final String m_version;

    private final String m_schema;

    /**
     * Constructor.
     *
     * @param parametersJson JSON containing the parameters
     * @param schema the JSON schema of the parameters
     * @param version the extension version
     */
    public JsonNodeSettings(final String parametersJson, final String schema, final String version) {
        m_schema = schema;
        m_parameters = parametersJson;
        m_version = version;
    }

    /**
     * Constructor that creates a new instance from a {@link NodeSettingsRO} object and a schema.
     *
     * @param settings containing the stored settings
     * @param schema of the settings
     * @throws InvalidSettingsException if the settings are invalid
     */
    private JsonNodeSettings(final NodeSettingsRO settings, final String schema) throws InvalidSettingsException {
        m_schema = schema;
        var preprocessed = preprocess(settings);
        m_parameters = JsonNodeSettingsMapperUtil.nodeSettingsToJsonString(preprocessed);
        m_version = settings.getString(CFG_VERSION);
    }

    /**
     * @return JSON string containing the parameters
     */
    public String getParameters() {
        return m_parameters;
    }

    /**
     * @return the version with which the settings were created
     */
    public String getCreationVersion() {
        return m_version;
    }

    /**
     * Saves the settings including their creation version.
     *
     * @param settings to save to
     */
    public void saveTo(final NodeSettingsWO settings) {
        var tempSettings = new NodeSettings("temp");
        JsonNodeSettingsMapperUtil.jsonStringToNodeSettings(m_parameters, m_schema, tempSettings);
        try {
            var modelSettings = tempSettings.getNodeSettings("model");
            modelSettings.copyTo(settings);
        } catch (InvalidSettingsException ex) {
            throw new IllegalStateException("Parameter conversion did not add model settings.", ex);
        }
        settings.addString(CFG_VERSION, m_version);
    }

    /**
     * Creates a new instance with the same schema but the settings stored in {@code settings}.
     *
     * @param settings to load into the newly created object
     * @return a new instance with the same schema as this instance but the values from settings
     * @throws InvalidSettingsException if the settings are invalid
     */
    public JsonNodeSettings createFromSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        return new JsonNodeSettings(settings, m_schema);
    }

    /**
     * Creates a new instance from the provided JSON string and the schema of this instance.
     *
     * @param json holding the settings
     * @return a new instance with the same schema as this instance but the values from the json
     */
    public JsonNodeSettings createFromJson(final String json) {
        return new JsonNodeSettings(json, m_schema, m_version);
    }

    private static NodeSettings preprocess(final NodeSettingsRO settings) {
        var settingsWithoutVersion = settingsWithoutVersion(toNodeSettings(settings));
        var tmpSettings = new NodeSettings("temp");
        var modelSettings = tmpSettings.addNodeSettings("model");
        settingsWithoutVersion.copyTo(modelSettings);
        return tmpSettings;
    }

    private static NodeSettings settingsWithoutVersion(final NodeSettings settingsWithVersion) {
        var settingsWithoutVersion = new NodeSettings(settingsWithVersion.getKey());
        for (var key : settingsWithVersion) {
            if (!CFG_VERSION.equals(key)) {
                var entry = settingsWithVersion.getEntry(key);
                settingsWithoutVersion.addEntry(entry);
            }
        }
        return settingsWithoutVersion;
    }

    private static NodeSettings toNodeSettings(final NodeSettingsRO settings) {
        if (settings instanceof NodeSettings) {
            return (NodeSettings)settings;
        } else {
            var newSettings = new NodeSettings(settings.getKey());
            settings.copyTo(newSettings);
            return newSettings;
        }
    }

}
