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
 *   Jun 30, 2022 (benjamin): created
 */
package org.knime.python3.js.scripting.nodes.script;

import java.util.Map;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.webui.node.dialog.JsonNodeSettingsService;
import org.knime.core.webui.node.dialog.SettingsType;

import com.google.gson.Gson;

/**
 * The settings of a Python scripting node.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
final class PythonScriptNodeSettings {

    private static final Gson GSON = new Gson();

    private static final String SCRIPT_CFG_KEY = "script";

    private Settings m_settings;

    PythonScriptNodeSettings() {
        m_settings = new Settings();
    }

    String getScript() {
        return m_settings.script;
    }

    void saveSettingsTo(final NodeSettingsWO settings) {
        saveSettings(m_settings, settings);
    }

    void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings = loadSettings(settings);
    }

    private static void saveSettings(final Settings s, final NodeSettingsWO settings) {
        settings.addString(SCRIPT_CFG_KEY, s.script);
    }

    private static Settings loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        final var script = settings.getString(SCRIPT_CFG_KEY);
        return new Settings(script);
    }

    /** For JSON serialization */
    private static final class Settings {

        private final String script;

        Settings() {
            // Defaults are given here
            // TODO(AP-19335) provide the template selection if the script is emtpy
            script = "";
        }

        @SuppressWarnings("hiding")
        Settings(final String script) {
            this.script = script;
        }
    }

    static JsonNodeSettingsService<Settings> createSettingsService() {
        return new SettingsService();
    }

    private static final class SettingsService implements JsonNodeSettingsService<Settings> {

        @Override
        public void getDefaultNodeSettings(final Map<SettingsType, NodeSettingsWO> settings,
            final PortObjectSpec[] specs) {
            toNodeSettingsFromObject(new Settings(), settings);
        }

        @Override
        public void toNodeSettingsFromObject(final Settings settingsObj,
            final Map<SettingsType, NodeSettingsWO> settings) {
            final var modelSettings = settings.get(SettingsType.MODEL);
            saveSettings(settingsObj, modelSettings);

        }

        @Override
        public Settings fromNodeSettingsToObject(final Map<SettingsType, NodeSettingsRO> settings,
            final PortObjectSpec[] specs) {
            try {
                return loadSettings(settings.get(SettingsType.MODEL));
            } catch (final InvalidSettingsException e) {
                // This should not happen because we do not save invalid settings. We just forward the exception
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        @Override
        public Settings fromJson(final String json) {
            return GSON.fromJson(json, Settings.class);
        }

        @Override
        public String toJson(final Settings obj) {
            return GSON.toJson(obj);
        }

    }
}
