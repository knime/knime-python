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

import java.awt.Component;
import java.awt.Container;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.util.Collection;
import java.util.Optional;

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.Icon;
import javax.swing.JComboBox;
import javax.swing.JRadioButton;
import javax.swing.event.ChangeListener;

import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.DefaultStringIconOption;
import org.knime.core.node.util.StringIconOption;
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

        updateView();

        final SettingsModelString variableSelectionModel = m_variableSelectionBox.getModel();
        m_usePreferencesButton.addActionListener(e -> {
            variableSelectionModel.setEnabled(false);
            updateModel(null);
        });
        // Triggers the change listener one below which in turn updates the model.
        m_useVariableButton.addActionListener(e -> variableSelectionModel.setEnabled(true));
        variableSelectionModel.addChangeListener(e -> {
            if (variableSelectionModel.isEnabled()) {
                updateModel(m_variableSelectionBox.isPlaceholderSelected() //
                    ? null //
                    : variableSelectionModel.getStringValue());
            }
        });
        m_flowVariableModel.addChangeListener(e -> {
            final Optional<FlowVariable> variable = m_flowVariableModel.getVariableValue();
            final String commandString = variable.map(v -> v.getValue(StringType.INSTANCE)).orElse(null);
            m_config.setCommandString(commandString);
            updateView();
        });
    }

    @Override
    public PythonVersion getPythonVersion() {
        return m_config.getPythonVersion();
    }

    @Override
    public PythonCommand getPythonCommand() {
        return m_config.getCommand();
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

        final Collection<FlowVariable> availableVariables =
            m_dialog.getAvailableFlowVariables(CondaEnvironmentType.INSTANCE).values();
        final String variableName = m_flowVariableModel.getInputVariableName();
        m_variableSelectionBox.updateSelection(availableVariables, variableName);
        updateModel(variableName);
        updateView();
    }

    private void updateModel(final String flowVariableName) {
        m_flowVariableModel.setInputVariableName(flowVariableName);
    }

    // TODO: consolidate box-specific parts of this with logic in box constructor (--> box.updateView())
    private void updateView() {
        final boolean useVariable = m_flowVariableModel.isVariableReplacementEnabled();
        if (useVariable) {
            final String variableName = m_flowVariableModel.getInputVariableName();
            if (!m_dialog.getAvailableFlowVariables(CondaEnvironmentType.INSTANCE).containsKey(variableName) &&
                m_dialog.getAvailableFlowVariables(StringType.INSTANCE).containsKey(variableName)) {
                // User selected a simple string variable on the Flow Variables tab. This is legitimate but not
                // supported by this selection panel, so disable/override the panel.
                setAllEnabled(this, false);
                return;
            }
        }

        setAllEnabled(this, true);

        m_usePreferencesButton.setSelected(!useVariable);
        m_useVariableButton.setSelected(useVariable);

        if (useVariable) {
            m_variableSelectionBox.getModel().setStringValue(m_flowVariableModel.getInputVariableName());
        }

        m_useVariableButton.setEnabled(!m_variableSelectionBox.isPlaceholderSelected());
        m_variableSelectionBox.getModel().setEnabled(m_useVariableButton.isSelected());
        m_variableSelectionBox.m_selectionBox.setEnabled(m_variableSelectionBox.getModel().isEnabled());
    }

    private static void setAllEnabled(final Component component, final boolean enabled) {
        component.setEnabled(enabled);
        if (component instanceof Container) {
            for (Component c : ((Container)component).getComponents()) {
                setAllEnabled(c, enabled);
            }
        }
    }

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_config.saveSettingsTo(settings);
    }

    private static final class PythonEnvironmentVariableSelectionBox {

        private static final String PLACEHOLDER_ENV_VARIABLE_NAME = CondaEnvironmentsConfig.PLACEHOLDER_CONDA_ENV_NAME;

        private final SettingsModelString m_model;

        private final DialogComponentStringSelection m_selection;

        private final JComboBox<?> m_selectionBox;

        public PythonEnvironmentVariableSelectionBox(final FlowVariableModel model) {
            final String initialValue = model.isVariableReplacementEnabled() //
                ? model.getInputVariableName() //
                : PLACEHOLDER_ENV_VARIABLE_NAME;
            m_model = new SettingsModelString("dummy", initialValue);
            m_model.setEnabled(model.isVariableReplacementEnabled());
            m_selection = new DialogComponentStringSelection(m_model, "", initialValue);
            m_selectionBox = getFirstComponent(m_selection, JComboBox.class);
        }

        public SettingsModelString getModel() {
            return m_model;
        }

        public boolean isPlaceholderSelected() {
            return m_model.getStringValue().equals(PLACEHOLDER_ENV_VARIABLE_NAME);
        }

        public JComboBox<?> getSelectionBox() {
            return m_selectionBox;
        }

        public void updateSelection(final Collection<FlowVariable> availableVariables, final String variableToSelect) {
            final Icon icon = CondaEnvironmentType.INSTANCE.getIcon();
            StringIconOption[] environmentVariableNames = availableVariables.stream() //
                .filter(v -> v.getVariableType().equals(CondaEnvironmentType.INSTANCE)) //
                .map(FlowVariable::getName) //
                .sorted() //
                .map(name -> new DefaultStringIconOption(name, icon)) //
                .toArray(StringIconOption[]::new);
            if (environmentVariableNames.length == 0) {
                environmentVariableNames =
                    new StringIconOption[]{new DefaultStringIconOption(PLACEHOLDER_ENV_VARIABLE_NAME)};
            }
            m_selection.replaceListItems(environmentVariableNames, variableToSelect);
        }

        private static <T extends Component> T getFirstComponent(final DialogComponent dialogComponent,
            final Class<T> componentClass) {
            for (final Component c : dialogComponent.getComponentPanel().getComponents()) {
                if (componentClass.isInstance(c)) {
                    @SuppressWarnings("unchecked")
                    final T safe = (T)c;
                    return safe;
                }
            }
            return null;
        }
    }
}
