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
package org.knime.python2.prefs;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.python2.config.AbstractEnvironmentTypePanel;
import org.knime.python2.config.PythonEnvironmentTypeConfig;
import org.knime.python2.config.PythonEnvironmentType;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
final class PythonEnvironmentTypePreferencePanel extends AbstractEnvironmentTypePanel<Composite> {

    public PythonEnvironmentTypePreferencePanel(final PythonEnvironmentTypeConfig config, final Composite parent) {
        super(config, parent);
    }

    @Override
    protected Composite createPanel(final Composite parent) {
        final Composite panel = new Composite(parent, SWT.NONE);
        panel.setLayout(new GridLayout());
        return panel;
    }

    @Override
    protected void createEnvironmentTypeWidget(final SettingsModelString environmentTypeConfig, final Composite panel) {
        final EnvironmentTypeSelectionRadioGroup environmentTypeSelection =
            new EnvironmentTypeSelectionRadioGroup(environmentTypeConfig, panel);
        final GridData gridData = new GridData();
        gridData.grabExcessHorizontalSpace = true;
        gridData.horizontalAlignment = SWT.FILL;
        environmentTypeSelection.setLayoutData(gridData);
    }

    private static final class EnvironmentTypeSelectionRadioGroup extends Composite {

        private final Button m_condaEnvironmentRadioButton;

        private final Button m_manualEnvironmentRadioButton;

        public EnvironmentTypeSelectionRadioGroup(final SettingsModelString environmentTypeConfig,
            final Composite parent) {
            super(parent, SWT.NONE);
            final RowLayout rowLayout = new RowLayout(SWT.HORIZONTAL);
            rowLayout.pack = false;
            rowLayout.justify = true;
            setLayout(rowLayout);
            m_condaEnvironmentRadioButton = new Button(this, SWT.RADIO);
            m_condaEnvironmentRadioButton.setText(PythonEnvironmentType.CONDA.getName());
            m_manualEnvironmentRadioButton = new Button(this, SWT.RADIO);
            m_manualEnvironmentRadioButton.setText(PythonEnvironmentType.MANUAL.getName());
            pack();
            setSelectedEnvironmentType(environmentTypeConfig.getStringValue());
            environmentTypeConfig
                .addChangeListener(e -> setSelectedEnvironmentType(environmentTypeConfig.getStringValue()));
            final SelectionListener radioButtonSelectionListener =
                createRadioButtonSelectionListener(environmentTypeConfig);
            m_condaEnvironmentRadioButton.addSelectionListener(radioButtonSelectionListener);
            m_manualEnvironmentRadioButton.addSelectionListener(radioButtonSelectionListener);
        }

        private void setSelectedEnvironmentType(final String environmentTypeId) {
            final PythonEnvironmentType environmentType = PythonEnvironmentType.fromId(environmentTypeId);
            final Button radioButtonToSelect;
            final Button radioButtonToUnselect;
            if (PythonEnvironmentType.CONDA.equals(environmentType)) {
                radioButtonToSelect = m_condaEnvironmentRadioButton;
                radioButtonToUnselect = m_manualEnvironmentRadioButton;
            } else if (PythonEnvironmentType.MANUAL.equals(environmentType)) {
                radioButtonToSelect = m_manualEnvironmentRadioButton;
                radioButtonToUnselect = m_condaEnvironmentRadioButton;
            } else {
                throw new IllegalStateException("Selected Python environment type is neither Conda nor manual. This is "
                    + "an implementation error.");
            }
            if (!radioButtonToSelect.getSelection()) {
                radioButtonToSelect.setSelection(true);
            }
            if (radioButtonToUnselect.getSelection()) {
                radioButtonToUnselect.setSelection(false);
            }
        }

        private static SelectionListener
            createRadioButtonSelectionListener(final SettingsModelString environmentTypeConfig) {
            return new SelectionListener() {

                @Override
                public void widgetSelected(final SelectionEvent e) {
                    final Button button = (Button)e.widget;
                    if (button.getSelection()) {
                        final PythonEnvironmentType selectedEnvironmentType =
                            PythonEnvironmentType.fromName(button.getText());
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
