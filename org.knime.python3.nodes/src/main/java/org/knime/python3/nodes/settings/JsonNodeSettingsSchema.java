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
 *   Jul 26, 2022 (Ivan Prigarin, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes.settings;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.defaultnodesettings.SettingsModel;

/**
 * Used to create {@link JsonNodeSettings} objects using the provided schema and NodeSettings or Json objects.
 *
 * @author Ivan Prigarin, KNIME GmbH, Konstanz, Germany
 */
public final class JsonNodeSettingsSchema {

    private static final String EXTENSION_VERSION = "extension_version" + SettingsModel.CFGKEY_INTERNAL;;

    private final String m_schema;

    private final String m_version;

    /**
     * Constructor.
     *
     * @param schema the JSON schema of the parameters
     * @param version of the extension
     */
    public JsonNodeSettingsSchema(final String schema, final String version) {
        m_schema = schema;
        m_version = version;
    }

    /**
     * @param settings the saved node settings
     * @return the version the settings were saved with
     * @throws InvalidSettingsException if settings are missing the version field
     */
    public static String readVersion(final NodeSettingsRO settings) throws InvalidSettingsException {
        try {
            // settings won't have the appropriate field if they were saved before versioning
            // was introduced. In this case the version defaults to 0.0.0.
            return settings.getString(EXTENSION_VERSION);
        } catch (Exception e) {
            return "0.0.0";
        }
    }

    /**
     * Creates a new instance with the same schema but the settings stored in {@code settings}.
     *
     * @param settings to load into the newly created object
     * @return a new instance with the same schema as this instance but the values from settings
     * @throws InvalidSettingsException if the settings are invalid
     */
    public JsonNodeSettings createFromSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        return new JsonNodeSettings(settings, m_schema, readVersion(settings));
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


}
