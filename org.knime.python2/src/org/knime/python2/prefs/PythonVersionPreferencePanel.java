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
 *   Jan 25, 2019 (marcel): created
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
import org.eclipse.swt.widgets.Group;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.python2.PythonVersion;
import org.knime.python2.config.AbstractPythonVersionPanel;
import org.knime.python2.config.PythonVersionConfig;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
final class PythonVersionPreferencePanel extends AbstractPythonVersionPanel<Composite> {

    public PythonVersionPreferencePanel(final PythonVersionConfig config, final Composite parent) {
        super(config, parent);
    }

    @Override
    protected Composite createPanel(final Composite parent) {
        final Composite panel = new Composite(parent, SWT.NONE);
        final GridLayout gridLayout = new GridLayout();
        gridLayout.marginWidth = 0;
        panel.setLayout(gridLayout);
        return panel;
    }

    @Override
    protected void createPythonVersionWidget(final SettingsModelString versionConfig, final Composite panel) {
        final PythonVersionSelectionRadioGroup versionSelection =
            new PythonVersionSelectionRadioGroup(versionConfig, panel);
        final GridData gridData = new GridData();
        gridData.grabExcessHorizontalSpace = true;
        gridData.horizontalAlignment = SWT.FILL;
        versionSelection.setLayoutData(gridData);
    }

    private static final class PythonVersionSelectionRadioGroup extends Composite {

        private final Button m_python2RadioButton;

        private final Button m_python3RadioButton;

        public PythonVersionSelectionRadioGroup(final SettingsModelString versionConfig, final Composite parent) {
            super(parent, SWT.NONE);
            final Group radioButtonGroup = new Group(this, SWT.NONE);
            radioButtonGroup.setText("Python version to use by default");
            m_python2RadioButton = new Button(radioButtonGroup, SWT.RADIO);
            m_python2RadioButton.setText(PythonVersion.PYTHON2.getName());
            m_python3RadioButton = new Button(radioButtonGroup, SWT.RADIO);
            m_python3RadioButton.setText(PythonVersion.PYTHON3.getName());
            final RowLayout rowLayout = new RowLayout(SWT.HORIZONTAL);
            rowLayout.pack = false;
            rowLayout.justify = true;
            rowLayout.marginLeft = 0;
            radioButtonGroup.setLayout(rowLayout);
            radioButtonGroup.pack();
            setSelectedPythonVersion(versionConfig.getStringValue());
            versionConfig.addChangeListener(e -> setSelectedPythonVersion(versionConfig.getStringValue()));
            final SelectionListener radioButtonSelectionListener = createRadioButtonSelectionListener(versionConfig);
            m_python2RadioButton.addSelectionListener(radioButtonSelectionListener);
            m_python3RadioButton.addSelectionListener(radioButtonSelectionListener);

        }

        private void setSelectedPythonVersion(final String pythonVersionId) {
            final PythonVersion pythonVersion = PythonVersion.fromId(pythonVersionId);
            final Button pythonRadioButtonToSelect;
            final Button pythonRadioButtonToUnselect;
            if (PythonVersion.PYTHON2.equals(pythonVersion)) {
                pythonRadioButtonToSelect = m_python2RadioButton;
                pythonRadioButtonToUnselect = m_python3RadioButton;
            } else if (PythonVersion.PYTHON3.equals(pythonVersion)) {
                pythonRadioButtonToSelect = m_python3RadioButton;
                pythonRadioButtonToUnselect = m_python2RadioButton;
            } else {
                throw new IllegalStateException("Selected default Python version is neither Python 2 nor Python 3. "
                    + "This is an implementation error.");
            }
            if (!pythonRadioButtonToSelect.getSelection()) {
                pythonRadioButtonToSelect.setSelection(true);
            }
            if (pythonRadioButtonToUnselect.getSelection()) {
                pythonRadioButtonToUnselect.setSelection(false);
            }
        }

        private static SelectionListener createRadioButtonSelectionListener(final SettingsModelString versionConfig) {
            return new SelectionListener() {

                @Override
                public void widgetSelected(final SelectionEvent e) {
                    final Button button = (Button)e.widget;
                    if (button.getSelection()) {
                        final PythonVersion selectedPythonVersion = PythonVersion.fromName(button.getText());
                        versionConfig.setStringValue(selectedPythonVersion.getId());
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
