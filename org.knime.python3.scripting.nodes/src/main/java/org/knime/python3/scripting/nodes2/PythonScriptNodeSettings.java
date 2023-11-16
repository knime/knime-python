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
package org.knime.python3.scripting.nodes2;

import static org.knime.python3.scripting.nodes2.ExecutableSelectionUtils.EXEC_SELECTION_PREF_ID;

import java.util.Map;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.webui.node.dialog.NodeAndVariableSettingsRO;
import org.knime.core.webui.node.dialog.NodeAndVariableSettingsWO;
import org.knime.core.webui.node.dialog.NodeSettingsService;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.scripting.editor.ScriptingNodeSettings;
import org.knime.scripting.editor.ScriptingNodeSettingsService;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The settings of a Python scripting node.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction") // the UIExtension node dialog API is still restricted
final class PythonScriptNodeSettings extends ScriptingNodeSettings {

    private static final SettingsType SCRIPT_SETTINGS_TYPE = SettingsType.MODEL;

    private static final String EXECUTABLE_SELECTION_CFG_KEY = "python3_command";

    private String m_executableSelection;

    PythonScriptNodeSettings(final PythonScriptPortsConfiguration portsConfiguration) {
        super(PythonScriptNodeDefaultScripts.getDefaultScript(portsConfiguration), SCRIPT_SETTINGS_TYPE);
        m_executableSelection = EXEC_SELECTION_PREF_ID;
    }

    String getExecutableSelection() {
        return m_executableSelection;
    }

    void saveSettingsTo(final NodeSettingsWO settings) {
        super.saveModelSettingsTo(settings);

        // NB: only configured via flow variables
        settings.addString(EXECUTABLE_SELECTION_CFG_KEY, EXEC_SELECTION_PREF_ID);
    }

    void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadModelSettings(settings);

        // NB: might be overwritten by a flow variable
        m_executableSelection = settings.getString(EXECUTABLE_SELECTION_CFG_KEY);
    }

    static NodeSettingsService createNodeSettingsService() {
        return new PythonScriptNodeSettingsService();
    }

    private static final class PythonScriptNodeSettingsService extends ScriptingNodeSettingsService {

        private static final String EXEC_SELECTION_JSON_KEY = "executableSelection";

        public PythonScriptNodeSettingsService() {
            super(SCRIPT_SETTINGS_TYPE);
        }

        @Override
        protected void putAdditionalSettingsToJson(final Map<SettingsType, NodeAndVariableSettingsRO> settings,
            final PortObjectSpec[] specs, final ObjectNode settingsJson) {
            var modelSettings = settings.get(SettingsType.MODEL);
            var executableSelection = modelSettings.getString(EXECUTABLE_SELECTION_CFG_KEY, EXEC_SELECTION_PREF_ID);
            if (modelSettings.isVariableSetting(EXECUTABLE_SELECTION_CFG_KEY)) {
                try {
                    executableSelection = modelSettings.getUsedVariable(EXECUTABLE_SELECTION_CFG_KEY);
                } catch (final InvalidSettingsException e) {
                    // This should not happen because we check that the executable selection is a variable setting first
                    throw new IllegalStateException(e);
                }
            }
            settingsJson.put(EXEC_SELECTION_JSON_KEY, executableSelection);
        }

        @Override
        protected void addAdditionalSettingsToNodeSettings(final ObjectNode settingsJson,
            final Map<SettingsType, NodeAndVariableSettingsWO> settings) {
            var modelSettings = settings.get(SettingsType.MODEL);
            modelSettings.addString(EXECUTABLE_SELECTION_CFG_KEY, EXEC_SELECTION_PREF_ID);
            try {
                modelSettings.addUsedVariable(EXECUTABLE_SELECTION_CFG_KEY,
                    settingsJson.get(EXEC_SELECTION_JSON_KEY).asText());
            } catch (final InvalidSettingsException e) {
                // Cannot happen because we have a setting with the key EXECUTABLE_SELECTION_CFG_KEY
                throw new IllegalStateException(e);
            }
        }
    }
}
