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
package org.knime.python3.scripting.nodes2.script;

import static org.knime.python3.scripting.nodes2.script.ExecutableSelectionUtils.EXEC_SELECTION_PREF_ID;

import java.util.Map;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.core.webui.node.dialog.VariableSettingsWO;

import com.google.gson.Gson;

/**
 * The settings of a Python scripting node.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction") // the UIExtension node dialog API is still restricted
final class PythonScriptNodeSettings {

    private static final Gson GSON = new Gson();

    private static final String SCRIPT_CFG_KEY = "script";

    private static final String EXECUTABLE_SELECTION_CFG_KEY = "python3_command";

    private Settings m_settings;

    PythonScriptNodeSettings() {
        m_settings = new Settings();
    }

    String getScript() {
        return m_settings.script;
    }

    String getExecutableSelection() {
        return m_settings.executableSelection;
    }

    void saveSettingsTo(final NodeSettingsWO settings) {
        saveSettings(m_settings, settings);
    }

    void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings = loadSettings(settings);
    }

    private static void saveSettings(final Settings s, final NodeSettingsWO settings) {
        settings.addString(SCRIPT_CFG_KEY, s.script);
        // NB: only configured via flow variables
        settings.addString(EXECUTABLE_SELECTION_CFG_KEY, EXEC_SELECTION_PREF_ID);
    }

    private static Settings loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        final var script = settings.getString(SCRIPT_CFG_KEY);
        // NB: might be overwritten by a flow variable
        final var executableSelection = settings.getString(EXECUTABLE_SELECTION_CFG_KEY);
        return new Settings(script, executableSelection);
    }

    /** For JSON serialization */
    private static final class Settings {

        private final String script; // NOSONAR

        private final String executableSelection; // NOSONAR

        Settings() {
            // Defaults are given here
            // TODO(AP-19335) provide the template selection if the script is emtpy
            script = "";
            executableSelection = EXEC_SELECTION_PREF_ID;

        }

        @SuppressWarnings("hiding")
        Settings(final String script, final String executableSelection) {
            this.script = script;
            this.executableSelection = executableSelection;
        }
    }

    static NodeSettingsService createNodeSettingsService() {
        return new NodeSettingsService();
    }

    static VariableSettingsService createVariableSettingsService() {
        return new VariableSettingsService();
    }

    private static final class NodeSettingsService implements org.knime.core.webui.node.dialog.NodeSettingsService {
        @Override
        public void getDefaultNodeSettings(final Map<SettingsType, NodeSettingsWO> settings,
            final PortObjectSpec[] specs) {
            saveSettings(new Settings(), settings.get(SettingsType.MODEL));
        }

        @Override
        public void toNodeSettings(final String settingsString, final Map<SettingsType, NodeSettingsWO> settings) {
            var settingsObj = GSON.fromJson(settingsString, Settings.class);
            saveSettings(settingsObj, settings.get(SettingsType.MODEL));
        }

        @Override
        public String fromNodeSettings(final Map<SettingsType, NodeSettingsRO> settings, final PortObjectSpec[] specs) {
            try {
                return GSON.toJson(loadSettings(settings.get(SettingsType.MODEL)));
            } catch (final InvalidSettingsException e) {
                // This should not happen because we do not save invalid settings. We just forward the exception
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }

    private static final class VariableSettingsService
        implements org.knime.core.webui.node.dialog.VariableSettingsService {

        @Override
        public void toVariableSettings(final String settingsString,
            final Map<SettingsType, VariableSettingsWO> settings) {
            var settingsObj = GSON.fromJson(settingsString, Settings.class);
            if (!settingsObj.executableSelection.isEmpty()) {
                try {
                    settings.get(SettingsType.MODEL).addUsedVariable(EXECUTABLE_SELECTION_CFG_KEY,
                        settingsObj.executableSelection);
                } catch (final InvalidSettingsException e) {
                    // Cannot happen because we have a setting with the key EXECUTABLE_SELECTION_CFG_KEY
                    throw new IllegalStateException(e);
                }
            }
        }
    }

}
