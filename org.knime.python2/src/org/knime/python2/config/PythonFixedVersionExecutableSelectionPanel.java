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
 *   Jun 7, 2021 (marcel): created
 */
package org.knime.python2.config;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ItemEvent;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.JComboBox;
import javax.swing.JRadioButton;
import javax.swing.event.ChangeListener;

import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.util.FlowVariableListCellRenderer;
import org.knime.core.node.util.FlowVariableListCellRenderer.FlowVariableCell;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.VariableType.StringType;
import org.knime.python2.CondaEnvironmentPropagation.CondaEnvironmentType;
import org.knime.python2.PythonCommand;
import org.knime.python2.PythonVersion;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("serial") // Not intended for serialization.
public final class PythonFixedVersionExecutableSelectionPanel extends PythonExecutableSelectionPanel {

    private static final String UNKNOWN_FLOW_VARIABLE_COMMAND_STRING = "unknown_flow_variable_selected";

    private final NodeDialogPane m_dialog; // NOSONAR Not intended for serialization.

    private final PythonCommandConfig m_config; // NOSONAR Not intended for serialization.

    private final FlowVariableModel m_flowVariableModel; // NOSONAR Not intended for serialization.

    private final JRadioButton m_usePreferencesButton;

    private final JRadioButton m_useVariableButton;

    // Sonar: Not intended for serialization.
    private final PythonEnvironmentVariableSelectionBox m_variableSelectionBox; // NOSONAR

    /**
     * @param dialog The hosting node dialog. Needed to create flow variable models for the Python-command config
     *            exposed to the user by this instance.
     * @param config The configuration exposed to the user, and accordingly manipulated, by this instance.
     */
    public PythonFixedVersionExecutableSelectionPanel(final NodeDialogPane dialog, final PythonCommandConfig config) {
        setLayout(new GridBagLayout());
        setBorder(BorderFactory
            .createTitledBorder("Conda environment propagation (" + config.getPythonVersion().getName() + ")"));

        m_dialog = dialog;
        m_config = config;
        m_flowVariableModel = dialog.createFlowVariableModel(m_config.getConfigKey(), StringType.INSTANCE);

        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.BOTH;
        final ButtonGroup flowVarButtonGroup = new ButtonGroup();
        m_usePreferencesButton = new JRadioButton("Use KNIME Preferences (ignore Conda flow variables)");
        flowVarButtonGroup.add(m_usePreferencesButton);
        gbc.gridwidth = 2;
        add(m_usePreferencesButton, gbc);
        gbc.gridy++;
        m_useVariableButton = new JRadioButton("Use Conda flow variable");
        flowVarButtonGroup.add(m_useVariableButton);
        gbc.gridwidth = 1;
        add(m_useVariableButton, gbc);
        gbc.gridx++;
        m_variableSelectionBox = new PythonEnvironmentVariableSelectionBox(m_flowVariableModel);
        add(m_variableSelectionBox.getSelectionBox(), gbc);

        m_usePreferencesButton.addActionListener(e -> updateModel(null));
        m_useVariableButton.addActionListener(e -> updateModel(m_variableSelectionBox.getSelectedVariableName()));
        m_flowVariableModel.addChangeListener(e -> updateConfigAndView());
    }

    @Override
    public PythonVersion getPythonVersion() {
        return m_config.getPythonVersion();
    }

    @Override
    public PythonCommand getPythonCommand() {
        return m_config.getCommand();
    }

    boolean isControlledByFlowVariable() {
        return m_flowVariableModel.isVariableReplacementEnabled();
    }

    /**
     * @return The underlying configuration that is exposed to the user, and accordingly manipulated, by this instance.
     */
    public PythonCommandConfig getConfig() {
        return m_config;
    }

    @Override
    public void addChangeListener(final ChangeListener listener) {
        m_flowVariableModel.addChangeListener(listener);
    }

    @Override
    public void removeChangeListener(final ChangeListener listener) {
        m_flowVariableModel.removeChangeListener(listener);
    }

    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings) throws NotConfigurableException {
        try {
            m_config.loadSettingsFrom(settings);
        } catch (InvalidSettingsException ex) {
            throw new NotConfigurableException(ex.getMessage(), ex);
        }
        updateView();
    }

    private void updateModel(final String flowVariableName) {
        m_flowVariableModel.setInputVariableName(flowVariableName);
    }

    /**
     * See {@link PythonFixedVersionExecutableSelectionPanel#updateView()} for the documentation of the different
     * states.
     */
    private void updateConfigAndView() {
        final String commandString;
        if (!m_flowVariableModel.isVariableReplacementEnabled()) {
            // States 1 & 5
            commandString = null;
        } else {
            final Optional<FlowVariable> variableValue = m_flowVariableModel.getVariableValue();
            if (variableValue.isPresent()) {
                // States 2 & 3
                commandString = variableValue.map(v -> v.getValue(StringType.INSTANCE)).orElse(null);
            } else {
                // State 4
                NodeLogger.getLogger(PythonFixedVersionExecutableSelectionPanel.class).warn("The variable \"" +
                    m_flowVariableModel.getInputVariableName() +
                    "\" that controls the Python executable of this node does not exist (anymore).\n" +
                    "Please select a different Python executable via the respective tab of the configuration dialog " +
                    "of this node.");
                commandString = UNKNOWN_FLOW_VARIABLE_COMMAND_STRING;
            }
        }
        m_config.setCommandString(commandString);
        updateView();
    }

    /**
     * As a function of the current state of the {@link #m_flowVariableModel flow variable model} and the currently
     * available flow variables, this panel and its underlying config can be in one of five different states at a time:
     * <ul>
     * <li><b>State 1:</b> no flow variable set -&gt; use preferences</i>
     * <li><b>State 2:</b> conda flow variable set -&gt; use conda variable</i>
     * <li><b>State 3:</b> string flow variable set -&gt; use string variable; disable the entire panel but leave it
     * otherwise unchanged (Python nodes support string flow variables via the Flow Variables tab, but we do not want to
     * display them in this panel. Therefore selecting one effectively overrides the panel.)</i>
     * <li><b>State 4:</b> flow variable set but does not exist (anymore) -&gt; use variable but indicate that the
     * selection is invalid</i>
     * <li><b>State 5:</b> no flow variable set &amp; no conda flow variables available at all -&gt; like State 1 but
     * also disable the option to choose a conda variable; add a placeholder to the selection box
     * </ul>
     */
    private void updateView() {
        if (!m_flowVariableModel.isVariableReplacementEnabled()) {
            if (!m_dialog.getAvailableFlowVariables(CondaEnvironmentType.INSTANCE).isEmpty()) {
                // State 1
                m_usePreferencesButton.setEnabled(true);
                m_usePreferencesButton.setSelected(true);
                m_useVariableButton.setEnabled(true);
                m_useVariableButton.setSelected(false);
            } else {
                // State 5
                m_usePreferencesButton.setEnabled(true);
                m_usePreferencesButton.setSelected(true);
                m_useVariableButton.setEnabled(false);
                m_useVariableButton.setSelected(false);
            }
        } else {
            final Optional<FlowVariable> variableValue = m_flowVariableModel.getVariableValue();
            if (variableValue.isPresent()) {
                if (variableValue.get().getVariableType().equals(CondaEnvironmentType.INSTANCE)) {
                    // State 2
                    m_usePreferencesButton.setEnabled(true);
                    m_usePreferencesButton.setSelected(false);
                    m_useVariableButton.setEnabled(true);
                    m_useVariableButton.setSelected(true);
                } else {
                    // State 3
                    m_usePreferencesButton.setEnabled(false);
                    m_useVariableButton.setEnabled(false);
                }
            } else {
                // State 4
                m_usePreferencesButton.setEnabled(true);
                m_usePreferencesButton.setSelected(false);
                m_useVariableButton.setEnabled(true);
                m_useVariableButton.setSelected(true);
            }
        }

        m_variableSelectionBox.updateSelection(
            m_dialog.getAvailableFlowVariables(CondaEnvironmentType.INSTANCE, StringType.INSTANCE),
            m_flowVariableModel);
    }

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_config.saveSettingsTo(settings);
    }

    private static final class PythonEnvironmentVariableSelectionBox {

        private final JComboBox<FlowVariableCell> m_selectionBox = new JComboBox<>();

        private boolean m_isSelectionBoxUpdating = false;

        public PythonEnvironmentVariableSelectionBox(final FlowVariableModel model) {
            m_selectionBox.setRenderer(new FlowVariableListCellRenderer());
            m_selectionBox.addItemListener(e -> {
                if (m_selectionBox.isEnabled() && !m_isSelectionBoxUpdating &&
                    e.getStateChange() == ItemEvent.SELECTED) {
                    model.setInputVariableName(getSelectedVariableName());
                }
            });
        }

        public JComboBox<FlowVariableCell> getSelectionBox() {
            return m_selectionBox;
        }

        public String getSelectedVariableName() {
            final FlowVariableCell flowVariable = ((FlowVariableCell)m_selectionBox.getSelectedItem());
            return flowVariable != null ? flowVariable.getName() : null;
        }

        /**
         * See {@link PythonFixedVersionExecutableSelectionPanel#updateView()} for the documentation of the different
         * states.
         *
         * @param availableVariables All flow variables of type {@link StringType} and {@link CondaEnvironmentType} that
         *            are available to the enclosing node dialog.
         */
        public void updateSelection(final Map<String, FlowVariable> availableVariables,
            @SuppressWarnings("javadoc") final FlowVariableModel flowVariableModel) {
            final List<FlowVariableCell> environmentVariables =
                collectAvailableEnvironmentVariables(availableVariables, flowVariableModel);

            final boolean selectionBoxEnabledState;
            final FlowVariableCell variableToSelect;
            if (!flowVariableModel.isVariableReplacementEnabled()) {
                // States 1 & 5
                selectionBoxEnabledState = false;
                variableToSelect = environmentVariables.get(0);
            } else {
                final Optional<FlowVariable> variableValue = flowVariableModel.getVariableValue();
                if (variableValue.isPresent()) {
                    if (variableValue.get().getVariableType().equals(CondaEnvironmentType.INSTANCE)) {
                        // State 2
                        selectionBoxEnabledState = true;
                        final String variableNameToSelect = flowVariableModel.getInputVariableName();
                        variableToSelect = findVariableByName(environmentVariables, variableNameToSelect).orElseThrow();
                    } else {
                        // State 3
                        selectionBoxEnabledState = false;
                        final FlowVariableCell oldSelectedVariable = (FlowVariableCell)m_selectionBox.getSelectedItem();
                        final String oldSelectedVariableName = oldSelectedVariable != null //
                            ? oldSelectedVariable.getName() //
                            : null;
                        variableToSelect = findVariableByName(environmentVariables, oldSelectedVariableName)
                            .orElse(environmentVariables.get(0));
                    }
                } else {
                    // State 4
                    selectionBoxEnabledState = true;
                    final String variableNameToSelect = flowVariableModel.getInputVariableName();
                    variableToSelect = findVariableByName(environmentVariables, variableNameToSelect).orElseThrow();
                }
            }

            updateSelectionBox(selectionBoxEnabledState, environmentVariables, variableToSelect);
        }

        private static List<FlowVariableCell> collectAvailableEnvironmentVariables(
            final Map<String, FlowVariable> availableVariables, final FlowVariableModel flowVariableModel) {
            List<FlowVariableCell> environmentVariables = availableVariables.values().stream() //
                .filter(v -> v.getVariableType().equals(CondaEnvironmentType.INSTANCE)) //
                .map(FlowVariableCell::new) //
                .collect(Collectors.toList());
            if (flowVariableModel.isVariableReplacementEnabled() && flowVariableModel.getVariableValue().isEmpty()) {
                // State 4
                environmentVariables.add(new FlowVariableCell(flowVariableModel.getInputVariableName()));
            } else if (environmentVariables.isEmpty()) {
                // State 5
                environmentVariables = Arrays.asList((FlowVariableCell)null);
            }
            environmentVariables.sort(Comparator.comparing(FlowVariableCell::getName));
            return environmentVariables;
        }

        private static Optional<FlowVariableCell> findVariableByName(final List<FlowVariableCell> environmentVariables,
            final String variableName) {
            return environmentVariables.stream() //
                .filter(v -> v != null && v.getName().equals(variableName)) //
                .findFirst();
        }

        private void updateSelectionBox(final boolean selectionBoxEnabledState,
            final List<FlowVariableCell> environmentVariables, final FlowVariableCell variableToSelect) {
            m_selectionBox.setEnabled(selectionBoxEnabledState);

            m_isSelectionBoxUpdating = true;
            m_selectionBox.removeAllItems();
            for (final FlowVariableCell variable : environmentVariables) {
                m_selectionBox.addItem(variable);
            }
            m_selectionBox.setSelectedItem(variableToSelect);
            m_isSelectionBoxUpdating = false;

            m_selectionBox.setSize(m_selectionBox.getPreferredSize());
            m_selectionBox.getParent().validate();
        }
    }
}
