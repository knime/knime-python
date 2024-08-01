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

import java.util.HashMap;
import java.util.Map;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.webui.node.dialog.NodeAndVariableSettingsRO;
import org.knime.core.webui.node.dialog.NodeAndVariableSettingsWO;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.scripting.editor.GenericSettingsIOManager;
import org.knime.scripting.editor.ScriptingNodeSettings;

/**
 * The settings of a Python scripting node.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction") // the UIExtension node dialog API is still restricted
final class PythonScriptNodeSettings extends ScriptingNodeSettings implements GenericSettingsIOManager {

    private static final String CFG_KEY_EXECUTABLE_SELECTION = "python3_command";

    private static final String JSON_KEY_EXECUTABLE_SELECTION = "executableSelection";

    private static final String JSON_KEY_ARE_SETTINGS_OVERRIDDEN_BY_FLOW_VARIABLES = "settingsAreOverriddenByFlowVariable";

    private static final String JSON_KEY_OVERRIDING_FLOW_VARIABLE = "scriptUsedFlowVariable";

    private static final String CFG_KEY_SCRIPT = "script";

    private String m_executableSelection;

    private String m_script;

    PythonScriptNodeSettings(final PythonScriptPortsConfiguration portsConfiguration) {
        super(SettingsType.MODEL);

        m_executableSelection = EXEC_SELECTION_PREF_ID;
        m_script = PythonScriptNodeDefaultScripts.getDefaultScript(portsConfiguration);
    }

    String getExecutableSelection() {
        return m_executableSelection;
    }

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        // NB: only configured via flow variables
        settings.addString(CFG_KEY_EXECUTABLE_SELECTION, m_executableSelection);
        settings.addString(CFG_KEY_SCRIPT, m_script);
    }

    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        // NB: might be overwritten by a flow variable
        m_executableSelection = settings.getString(CFG_KEY_EXECUTABLE_SELECTION);
        m_script = settings.getString(CFG_KEY_SCRIPT);
    }

    @Override
    public Map<String, Object> convertNodeSettingsToMap(final Map<SettingsType, NodeAndVariableSettingsRO> settings)
        throws InvalidSettingsException {

        var nodeSettings = settings.get(m_scriptSettingsType);

        loadSettingsFrom(nodeSettings);

        // Check if the script is overridden by a flow variable, and if so, pass the var name to the frontend

        Map<String, Object> ret = new HashMap<>(Map.of( //
            CFG_KEY_SCRIPT, m_script, //
            JSON_KEY_EXECUTABLE_SELECTION, m_executableSelection //
        ));

        var scriptUsedFlowVariable = getOverridingFlowVariableName(nodeSettings, CFG_KEY_SCRIPT);
        if (scriptUsedFlowVariable.isPresent()) {
            ret.put(JSON_KEY_OVERRIDING_FLOW_VARIABLE, scriptUsedFlowVariable.get());
            ret.put(JSON_KEY_ARE_SETTINGS_OVERRIDDEN_BY_FLOW_VARIABLES, true);
        } else {
            ret.put(JSON_KEY_ARE_SETTINGS_OVERRIDDEN_BY_FLOW_VARIABLES, false);
        }

        return ret;
    }

    @Override
    public void writeMapToNodeSettings(final Map<String, Object> data,
        final Map<SettingsType, NodeAndVariableSettingsWO> settings) throws InvalidSettingsException {

        m_executableSelection = (String)data.get(JSON_KEY_EXECUTABLE_SELECTION);
        m_script = (String)data.get(CFG_KEY_SCRIPT);

        saveSettingsTo(settings);
    }

    public String getScript() {
        return m_script;
    }
}
