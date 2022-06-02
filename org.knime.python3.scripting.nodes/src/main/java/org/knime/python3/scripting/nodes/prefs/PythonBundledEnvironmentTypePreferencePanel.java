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
 *   Feb 15, 2019 (marcel): created
 */
package org.knime.python3.scripting.nodes.prefs;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.python2.config.AbstractPythonConfigPanel;
import org.knime.python2.config.PythonEnvironmentType;
import org.knime.python2.config.PythonEnvironmentTypeConfig;

/**
 * This is an adaptation of the {@link org.knime.python2.prefs.PythonEnvironmentTypePreferencePanel} that also adds the
 * option to select a bundled conda environment - if it is available.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class PythonBundledEnvironmentTypePreferencePanel
    extends AbstractPythonConfigPanel<PythonEnvironmentTypeConfig, Composite> {

    /**
     * Create a {@link PythonBundledEnvironmentTypePreferencePanel}
     *
     * @param config The {@link PythonEnvironmentTypeConfig} that is updated when the user selects a different option
     * @param parent The parent {@link Composite} in which the environment selection will be added
     * @param isBundledEnvAvailable A boolean flag whether the bundled environment is available. This will
     *            enable/disable the "Bundled" option.
     */
    public PythonBundledEnvironmentTypePreferencePanel(final PythonEnvironmentTypeConfig config, final Composite parent,
        final boolean isBundledEnvAvailable) {
        super(config, parent);
        createEnvironmentTypeWidget(config.getEnvironmentType(), getPanel(), isBundledEnvAvailable);
    }

    @Override
    protected Composite createPanel(final Composite parent) {
        final var panel = new Composite(parent, SWT.NONE);
        panel.setLayout(new GridLayout());
        return panel;
    }

    private static void createEnvironmentTypeWidget(final SettingsModelString environmentTypeConfig,
        final Composite panel, final boolean isBundledEnvAvailable) {
        final var environmentTypeSelection =
            new EnvironmentTypeSelectionRadioGroup(environmentTypeConfig, panel, isBundledEnvAvailable);
        final var gridData = new GridData();
        gridData.grabExcessHorizontalSpace = true;
        gridData.horizontalAlignment = SWT.FILL;
        environmentTypeSelection.setLayoutData(gridData);
    }

    private static final class EnvironmentTypeSelectionRadioGroup extends Composite {

        private final Button m_bundledCondaEnvironmentRadioButton;

        private final Button m_condaEnvironmentRadioButton;

        private final Button m_manualEnvironmentRadioButton;

        public EnvironmentTypeSelectionRadioGroup(final SettingsModelString environmentTypeConfig,
            final Composite parent, final boolean isBundledEnvAvailable) {
            super(parent, SWT.NONE);
            final var rowLayout = new RowLayout(SWT.HORIZONTAL);
            rowLayout.pack = false;
            rowLayout.justify = true;
            setLayout(rowLayout);

            m_bundledCondaEnvironmentRadioButton = new Button(this, SWT.RADIO);
            m_bundledCondaEnvironmentRadioButton.setText(PythonEnvironmentType.BUNDLED.getName());
            m_bundledCondaEnvironmentRadioButton.setEnabled(isBundledEnvAvailable);

            m_condaEnvironmentRadioButton = new Button(this, SWT.RADIO);
            m_condaEnvironmentRadioButton.setText(PythonEnvironmentType.CONDA.getName());
            m_manualEnvironmentRadioButton = new Button(this, SWT.RADIO);
            m_manualEnvironmentRadioButton.setText(PythonEnvironmentType.MANUAL.getName());
            pack();
            setSelectedEnvironmentType(environmentTypeConfig.getStringValue());
            environmentTypeConfig
                .addChangeListener(e -> setSelectedEnvironmentType(environmentTypeConfig.getStringValue()));
            final var radioButtonSelectionListener = createRadioButtonSelectionListener(environmentTypeConfig);
            m_bundledCondaEnvironmentRadioButton.addSelectionListener(radioButtonSelectionListener);
            m_condaEnvironmentRadioButton.addSelectionListener(radioButtonSelectionListener);
            m_manualEnvironmentRadioButton.addSelectionListener(radioButtonSelectionListener);
        }

        private static void setButtonSelection(final Button button, final boolean enabled) {
            if (button.getSelection() != enabled) {
                button.setSelection(enabled);
            }
        }

        private void setSelectedEnvironmentType(final String environmentTypeId) {
            final var environmentType = PythonEnvironmentType.fromId(environmentTypeId);

            if (PythonEnvironmentType.BUNDLED == environmentType) {
                setButtonSelection(m_bundledCondaEnvironmentRadioButton, true);
                setButtonSelection(m_condaEnvironmentRadioButton, false);
                setButtonSelection(m_manualEnvironmentRadioButton, false);
            } else if (PythonEnvironmentType.CONDA == environmentType) {
                setButtonSelection(m_bundledCondaEnvironmentRadioButton, false);
                setButtonSelection(m_condaEnvironmentRadioButton, true);
                setButtonSelection(m_manualEnvironmentRadioButton, false);
            } else if (PythonEnvironmentType.MANUAL == environmentType) {
                setButtonSelection(m_bundledCondaEnvironmentRadioButton, false);
                setButtonSelection(m_condaEnvironmentRadioButton, false);
                setButtonSelection(m_manualEnvironmentRadioButton, true);
            }
        }

        private static SelectionListener
            createRadioButtonSelectionListener(final SettingsModelString environmentTypeConfig) {
            return new SelectionListener() {

                @Override
                public void widgetSelected(final SelectionEvent e) {
                    final var button = (Button)e.widget;
                    if (button.getSelection()) {
                        final var selectedEnvironmentType = PythonEnvironmentType.fromName(button.getText());
                        environmentTypeConfig.setStringValue(selectedEnvironmentType.getId());
                    }
                }

                @Override
                public void widgetDefaultSelected(final SelectionEvent e) {
                    widgetSelected(e);
                }
            };
        }
    }
}
