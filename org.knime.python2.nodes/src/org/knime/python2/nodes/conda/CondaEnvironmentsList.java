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
 *   Nov 9, 2020 (marcel): created
 */
package org.knime.python2.nodes.conda;

import static org.knime.python2.nodes.conda.CondaEnvironmentPropagationNodeDialog.getFirstComponent;
import static org.knime.python2.nodes.conda.CondaEnvironmentPropagationNodeDialog.invokeOnEDT;

import java.awt.CardLayout;
import java.awt.Color;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.SwingConstants;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.python2.Conda;
import org.knime.python2.Conda.CondaEnvironmentIdentifier;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
final class CondaEnvironmentsList {

    private static final String INITIALIZING = "initializing";

    private static final String POPULATED = "populated";

    private static final String ERROR = "error";

    private final SettingsModelString m_environmentNameModel;

    private final Supplier<Conda> m_conda;

    private final JPanel m_panel = new JPanel(new CardLayout());

    private final JLabel m_initializingLabel = new JLabel("Collecting environments...", SwingConstants.CENTER);

    private final DialogComponentStringSelection m_environmentNameSelection;

    private final JLabel m_errorLabel = new JLabel("", SwingConstants.CENTER);

    public CondaEnvironmentsList(final SettingsModelString evironmentNameModel, final Supplier<Conda> conda) {
        m_environmentNameModel = evironmentNameModel;
        m_conda = conda;

        m_panel.add(m_initializingLabel, INITIALIZING);

        m_environmentNameSelection = new DialogComponentStringSelection(m_environmentNameModel, "Conda environment",
            m_environmentNameModel.getStringValue());
        final JPanel listPanel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.ipadx = 5;
        gbc.gridx = 0;
        gbc.gridy = 0;
        listPanel.add(getFirstComponent(m_environmentNameSelection, JLabel.class), gbc);
        gbc.gridx++;
        gbc.weightx = 1;
        listPanel.add(getFirstComponent(m_environmentNameSelection, JComboBox.class), gbc);
        m_panel.add(listPanel, POPULATED);

        m_errorLabel.setForeground(Color.RED);
        m_panel.add(m_errorLabel, ERROR);
    }

    public JComponent getComponent() {
        return m_panel;
    }

    public void setToInitializingView() {
        setState(INITIALIZING);
    }

    private void setState(final String state) {
        ((CardLayout)m_panel.getLayout()).show(m_panel, state);
    }

    public void loadSettingsFrom(final NodeSettingsRO settings) throws NotConfigurableException {
        try {
            // Make sure that the loaded value is not rejected by the dialog component.
            final SettingsModelString tempEnvironmentNameModel =
                new SettingsModelString(m_environmentNameModel.getKey(), m_environmentNameModel.getStringValue());
            tempEnvironmentNameModel.loadSettingsFrom(settings);
            final String environmentName = tempEnvironmentNameModel.getStringValue();
            m_environmentNameSelection.replaceListItems(Arrays.asList(environmentName), environmentName);
        } catch (final InvalidSettingsException ex) {
            throw new NotConfigurableException(ex.getMessage(), ex);
        }
    }

    public void initializeEnvironments() {
        try {
            final String environmentName = m_environmentNameModel.getStringValue();
            final List<CondaEnvironmentIdentifier> environments = m_conda.get().getEnvironments();
            final List<String> environmentNames = environments.stream() //
                .map(CondaEnvironmentIdentifier::getName) //
                .collect(Collectors.toList());
            if (environmentNames.isEmpty()) {
                throw new IOException("No Conda environments available.\nPlease review the Conda "
                    + "installation specified in the Preferences of the KNIME Python Integration.");
            }
            invokeOnEDT(() -> m_environmentNameSelection.replaceListItems(environmentNames, environmentName));
            invokeOnEDT(() -> setState(POPULATED));
        } catch (final IOException ex) {
            invokeOnEDT(() -> {
                final String defaultEnvironmentName =
                    CondaEnvironmentPropagationNodeModel.createCondaEnvironmentNameModel().getStringValue();
                m_environmentNameModel.setStringValue(defaultEnvironmentName);
                m_errorLabel.setText(ex.getMessage());
                setState(ERROR);
            });
            NodeLogger.getLogger(CondaEnvironmentPropagationNodeDialog.class).error(ex.getMessage(), ex);
        }
    }

    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_environmentNameModel.saveSettingsTo(settings);
    }
}
