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
package org.knime.python2.prefs.advanced;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Spinner;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.python2.config.AbstractPythonConfigPanel;
import org.knime.python2.config.advanced.PythonKernelQueueConfig;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class PythonKernelQueuePreferencePanel
    extends AbstractPythonConfigPanel<PythonKernelQueueConfig, Composite> {

    /**
     * @param config The config for this panel.
     * @param parent The container of this panel.
     */
    public PythonKernelQueuePreferencePanel(final PythonKernelQueueConfig config, final Composite parent) {
        super(config, parent);
        final Composite panel = getPanel();
        createInfoTextWidget(panel);
        createLabeledSpinnerWidget("Maximum number of provisioned processes", config.getMaxNumberOfIdlingProcesses(),
            panel);
        createLabeledSpinnerWidget("Expiration duration of each process (in minutes)",
            config.getExpirationDurationInMinutes(), panel);
    }

    @Override
    protected Composite createPanel(final Composite parent) {
        final Group panel = new Group(parent, SWT.NONE);
        panel.setText("Prelaunched Python processes");
        final GridLayout gridLayout = new GridLayout();
        gridLayout.numColumns = 2;
        panel.setLayout(gridLayout);
        final GridData gridData = new GridData();
        gridData.horizontalAlignment = SWT.FILL;
        gridData.grabExcessHorizontalSpace = true;
        panel.setLayoutData(gridData);
        return panel;
    }

    private static void createInfoTextWidget(final Composite parent) {
        final Label infoText = new Label(parent, SWT.WRAP);
        final GridData gridData = new GridData();
        gridData.horizontalSpan = 2;
        gridData.horizontalAlignment = SWT.FILL;
        gridData.widthHint = 500;
        gridData.grabExcessHorizontalSpace = true;
        infoText.setLayoutData(gridData);
        infoText.setText("In the background, KNIME initializes and maintains a pool of Python processes for use by "
            + "individual Python nodes. This reduces the startup cost when executing any Python nodes. The pool size "
            + "and the duration (in minutes) before recycling idle processes in the pool can be modified from their "
            + "recommended defaults below.");
    }

    private static void createLabeledSpinnerWidget(final String labelText,
        final SettingsModelIntegerBounded integerConfig, final Composite parent) {
        final Label spinnerLabel = new Label(parent, SWT.NONE);
        spinnerLabel.setLayoutData(new GridData());
        spinnerLabel.setText(labelText);

        final Spinner spinner = new Spinner(parent, SWT.NONE);
        final GridData spinnerGridData = new GridData();
        spinnerGridData.horizontalAlignment = SWT.FILL;
        spinnerGridData.grabExcessHorizontalSpace = true;
        spinner.setLayoutData(spinnerGridData);
        spinner.setMinimum(integerConfig.getLowerBound());
        spinner.setMaximum(integerConfig.getUpperBound());
        spinner.setSelection(integerConfig.getIntValue());
        integerConfig.addChangeListener(e -> spinner.setSelection(integerConfig.getIntValue()));
        spinner.addSelectionListener(new SelectionListener() {

            @Override
            public void widgetSelected(final SelectionEvent e) {
                integerConfig.setIntValue(spinner.getSelection());
            }

            @Override
            public void widgetDefaultSelected(final SelectionEvent e) {
                widgetSelected(e);
            }
        });
    }
}
