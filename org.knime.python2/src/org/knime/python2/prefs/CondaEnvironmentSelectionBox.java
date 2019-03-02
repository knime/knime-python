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
 *   Feb 10, 2019 (marcel): created
 */
package org.knime.python2.prefs;

import static org.knime.python2.prefs.PythonPreferenceUtils.performActionOnWidgetInUiThread;

import org.eclipse.jface.resource.FontDescriptor;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.defaultnodesettings.SettingsModelStringArray;
import org.knime.python2.config.CondaEnvironmentCreationObserver;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
final class CondaEnvironmentSelectionBox extends Composite {

    private final Label m_header;

    private final Combo m_environmentSelection;

    private final SettingsModelString m_environmentModel;

    /**
     * @param environmentModel The settings model for the conda environment name. May be updated asynchronously, that
     *            is, in a non-UI thread.
     * @param availableEnvironmentsModel The list of available conda environments. May be updated asynchronously, that
     *            is, in a non-UI thread.
     * @param selectionBoxLabel The description text for the environment selection box.
     * @param headerLabel The text of the header for the path editor's enclosing group box.
     * @param infoMessageModel The settings model for the info label. May be updated asynchronously, that is, in a
     *            non-UI thread.
     * @param errorMessageModel The settings model for the error label. May be updated asynchronously, that is, in a
     *            non-UI thread.
     * @param environmentCreator Handles the creation of new conda environments when the user clicks the widget's
     *            "New..." button.
     * @param parent The parent widget.
     */
    public CondaEnvironmentSelectionBox(final SettingsModelString environmentModel,
        final SettingsModelStringArray availableEnvironmentsModel, final String headerLabel,
        final String selectionBoxLabel, final SettingsModelString infoMessageModel,
        final SettingsModelString errorMessageModel, final CondaEnvironmentCreationObserver environmentCreator,
        final Composite parent) {
        super(parent, SWT.NONE);
        m_environmentModel = environmentModel;

        final GridLayout gridLayout = new GridLayout();
        gridLayout.numColumns = 3;
        setLayout(gridLayout);

        // Header:
        m_header = new Label(this, SWT.NONE);
        FontDescriptor descriptor = FontDescriptor.createFrom(m_header.getFont());
        descriptor = descriptor.setStyle(SWT.BOLD);
        m_header.setFont(descriptor.createFont(m_header.getDisplay()));
        m_header.setText(headerLabel);
        GridData gridData = new GridData();
        gridData.horizontalSpan = 3;
        m_header.setLayoutData(gridData);

        // Environment selection:
        final Label environmentSelectionLabel = new Label(this, SWT.NONE);
        gridData = new GridData();
        environmentSelectionLabel.setLayoutData(gridData);
        environmentSelectionLabel.setText(selectionBoxLabel);
        m_environmentSelection = new Combo(this, SWT.DROP_DOWN | SWT.READ_ONLY);
        gridData = new GridData();
        m_environmentSelection.setLayoutData(gridData);

        // Environment generation:
        final Button environmentCreationButton = new Button(this, SWT.NONE);
        environmentCreationButton.setText("New...");
        environmentCreationButton.setEnabled(environmentCreator.getIsEnvironmentCreationEnabled().getBooleanValue());
        gridData = new GridData();
        environmentCreationButton.setLayoutData(gridData);

        // Info and error labels:
        final InstallationStatusDisplayPanel statusDisplay =
            new InstallationStatusDisplayPanel(infoMessageModel, errorMessageModel, this);
        gridData = new GridData();
        gridData.verticalIndent = 10;
        gridData.horizontalSpan = 3;
        statusDisplay.setLayoutData(gridData);

        // Populate environment selection, hooks:
        setAvailableEnvironments(availableEnvironmentsModel.getStringArrayValue());
        setSelectedEnvironment(environmentModel.getStringValue());
        availableEnvironmentsModel
            .addChangeListener(e -> setAvailableEnvironments(availableEnvironmentsModel.getStringArrayValue()));
        environmentModel.addChangeListener(e -> setSelectedEnvironment(environmentModel.getStringValue()));
        m_environmentSelection.addSelectionListener(new SelectionListener() {

            @Override
            public void widgetSelected(final SelectionEvent e) {
                environmentModel.setStringValue(getSelectedEnvironment());
            }

            @Override
            public void widgetDefaultSelected(final SelectionEvent e) {
                widgetSelected(e);
            }
        });

        environmentCreator.getIsEnvironmentCreationEnabled()
            .addChangeListener(e -> performActionOnWidgetInUiThread(environmentCreationButton, () -> {
                environmentCreationButton
                    .setEnabled(environmentCreator.getIsEnvironmentCreationEnabled().getBooleanValue());
                return null;
            }, true));

        environmentCreationButton.addSelectionListener(new SelectionListener() {

            @Override
            public void widgetSelected(final SelectionEvent e) {
                new CondaEnvironmentCreationPreferenceDialog(environmentCreator, getShell()).open();
            }

            @Override
            public void widgetDefaultSelected(final SelectionEvent e) {
                widgetSelected(e);
            }
        });
    }

    private void setAvailableEnvironments(final String[] availableEnvironments) {
        String selectedEnvironment = m_environmentModel.getStringValue();
        performActionOnWidgetInUiThread(m_environmentSelection, () -> {
            m_environmentSelection.setItems(availableEnvironments);
            layout();
            return null;
        }, false);
        if (selectedEnvironment != null) {
            setSelectedEnvironment(selectedEnvironment);
        }
    }

    private String getSelectedEnvironment() {
        return performActionOnWidgetInUiThread(m_environmentSelection,
            () -> m_environmentSelection.getItem(m_environmentSelection.getSelectionIndex()), false);
    }

    private void setSelectedEnvironment(final String environmentName) {
        performActionOnWidgetInUiThread(m_environmentSelection, () -> {
            final int numEnvironments = m_environmentSelection.getItemCount();
            for (int i = 0; i < numEnvironments; i++) {
                if (m_environmentSelection.getItem(i).equals(environmentName)) {
                    m_environmentSelection.select(i);
                    break;
                }
            }
            return null;
        }, false);
    }

    public void setDisplayAsDefault(final boolean setAsDefault) {
        final String defaultSuffix = " (Default)";
        final String oldHeaderText = m_header.getText();
        if (setAsDefault) {
            if (!oldHeaderText.endsWith(defaultSuffix)) {
                m_header.setText(oldHeaderText + defaultSuffix);
                layout();
            }
        } else {
            final int suffixStart = oldHeaderText.indexOf(defaultSuffix);
            if (suffixStart != -1) {
                m_header.setText(oldHeaderText.substring(0, suffixStart));
                layout();
            }
        }
    }
}
