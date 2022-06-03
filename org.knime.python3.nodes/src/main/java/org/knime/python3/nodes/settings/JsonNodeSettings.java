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
import org.knime.core.node.KNIMEConstants;
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

    private static final String CFG_VERSION = "version" + SettingsModel.CFGKEY_INTERNAL;

    private String m_parameters;

    private String m_version;

    private final String m_schema;

    /**
     * Constructor.
     *
     * @param parametersJson JSON containing the parameters
     * @param schema the JSON schema of the parameters
     */
    public JsonNodeSettings(final String parametersJson, final String schema) {
        m_schema = schema;
        m_parameters = parametersJson;
        m_version = KNIMEConstants.VERSION;
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
     * Updates the parameters in this settings object. Also sets the version to the current AP version.
     *
     * @param parameters to update with
     */
    public void update(final String parameters) {
        m_version = KNIMEConstants.VERSION;
        m_parameters = parameters;
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
     * Loads the parameters from the provided settings. This method changes the state of this object. If you want to
     * load without changing the state use {@link #loadForValidation(NodeSettingsRO)}.
     *
     * @param settings to load from
     * @throws InvalidSettingsException if the settings are invalid i.e. can't be parsed
     */
    public void loadFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        var preprocessed = preprocess(settings);
        m_parameters = JsonNodeSettingsMapperUtil.nodeSettingsToJsonString(preprocessed);
        m_version = settings.getString(CFG_VERSION);
    }

    private static NodeSettings preprocess(final NodeSettingsRO settings) {
        var settingsWithoutVersion = settingsWithoutVersion(toNodeSettings(settings));
        var tmpSettings = new NodeSettings("temp");
        var modelSettings = tmpSettings.addNodeSettings("model");
        settingsWithoutVersion.copyTo(modelSettings);
        modelSettings.addNodeSettings(settingsWithoutVersion);
        return tmpSettings;
    }

    private JsonNodeSettings(final String schema, final String parameters, final String version) {
        m_schema = schema;
        m_parameters = parameters;
        m_version = version;
    }

    /**
     * Loads the parameters from the provided settings and produces a new instance of JsonNodeSettings. This method does
     * not change the state of this instance.
     *
     * @param settings to load from
     * @return JsonNodeSettings object containing the parameters in settings
     * @throws InvalidSettingsException if the settings are invalid i.e. can't be parsed
     */
    public JsonNodeSettings loadForValidation(final NodeSettingsRO settings) throws InvalidSettingsException {
        var preprocessed = preprocess(settings);
        var parameters = JsonNodeSettingsMapperUtil.nodeSettingsToJsonString(preprocessed);
        var version = settings.getString(CFG_VERSION);
        return new JsonNodeSettings(m_schema, parameters, version);
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
