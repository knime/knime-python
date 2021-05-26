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
 *   Nov 13, 2020 (marcel): created
 */
package org.knime.python2.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.VariableType;
import org.knime.core.node.workflow.VariableType.StringType;
import org.knime.python2.CondaEnvironmentPropagation.CondaEnvironmentType;
import org.knime.python2.PythonCommand;

/**
 * Monitors the control of a given {@link PythonCommandConfig} by flow variable such that the value provided by the
 * variable is also reflected in the node dialog.
 * <P>
 * Additionally offers functionality to automatically control the given config by the first flow variable of type
 * {@link CondaEnvironmentType} in the stack of flow variables available to the surrounding node.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class PythonCommandFlowVariableModel {

    private final NodeDialogPane m_dialog;

    private final PythonCommandFlowVariableConfig m_config;

    private final FlowVariableModel m_model;

    private final CopyOnWriteArrayList<Consumer<Optional<PythonCommand>>> m_listeners = new CopyOnWriteArrayList<>();

    private List<String> m_availableFlowVarNames;

    /**
     * @param dialog The parent node dialog.
     * @param config The config to control by flow variable.
     */
    public PythonCommandFlowVariableModel(final NodeDialogPane dialog, final PythonCommandFlowVariableConfig config) {
        m_dialog = dialog;
        m_config = config;
        m_model = dialog.createFlowVariableModel(config.getConfigKey(), VariableType.StringType.INSTANCE);
        m_model.addChangeListener(e -> {
            final Optional<FlowVariable> variable = ((FlowVariableModel)(e.getSource())).getVariableValue();
            final String commandString = variable.map(v -> v.getValue(StringType.INSTANCE)).orElse(null);
            config.setCommandString(commandString);
            for (final Consumer<Optional<PythonCommand>> listener : m_listeners) {
                listener.accept(config.getCommand());
            }
        });
    }

    public PythonCommandConfig getConfig() {
        return m_config;
    }

    /**
     * @param listener The listener to add. The listener is notified whenever a new command value is set via flow
     *            variable and/or the command value is cleared (indicated by an empty {@link Optional}).
     */
    public void addCommandChangeListener(final Consumer<Optional<PythonCommand>> listener) {
        if (!m_listeners.contains(listener)) {
            m_listeners.add(listener);
        }
    }

    /**
     * @param listener The listener to remove.
     */
    public void removeCommandChangeListener(final Consumer<Optional<PythonCommand>> listener) {
        m_listeners.remove(listener);
    }

    public void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_config.saveSettingsTo(settings);
    }

    public void loadSettingsFrom(final NodeSettingsRO settings) throws NotConfigurableException {
        try {
            m_config.loadSettingsFrom(settings);
        } catch (final InvalidSettingsException ex) {
            throw new NotConfigurableException(ex.getMessage(), ex);
        }
        m_availableFlowVarNames =
            new ArrayList<>(m_dialog.getAvailableFlowVariables(CondaEnvironmentType.INSTANCE).keySet());
    }

    /**
     * HACK: overwriting the settings by flow variable is only respected after the settings were loaded.
     */
    public void onDialogOpen() {
        if (!m_config.getDialogWasOpened()) {
            tryOverwriteSettingsByVariable();
            m_config.setDialogWasOpened(true);
        } else {
            refreshSettingsOverwrittenByVariable();
        }
    }

    /**
     * Tries to overwrite the controlled config by the flow variable output by
     * {@link CondaEnvironmentPropagationNodeFactory}, if that variable is present.
     */
    private void tryOverwriteSettingsByVariable() {
        if (!m_availableFlowVarNames.isEmpty()) {
            m_model.setInputVariableName(m_availableFlowVarNames.get(0));
        }
        m_availableFlowVarNames = null;
    }

    /**
     * HACK: for some reason, a KNIME node dialog does not pick up changed input flow variable values once it has been
     * constructed. Only closing and reopening the entire workflow makes it reflect the new values. But we can trigger
     * the update ourselves by simply renewing the model's input variable name.
     */
    private void refreshSettingsOverwrittenByVariable() {
        final String variableName = m_model.getInputVariableName();
        if (variableName != null) {
            m_model.setInputVariableName(null);
            m_model.setInputVariableName(variableName);
        }
    }
}
