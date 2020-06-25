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

import java.util.Objects;
import java.util.function.Consumer;

import org.eclipse.jface.resource.FontDescriptor;
import org.eclipse.jface.viewers.ArrayContentProvider;
import org.eclipse.jface.viewers.ComboViewer;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.LabelProvider;
import org.eclipse.jface.viewers.StructuredSelection;
import org.eclipse.jface.viewers.ViewerComparator;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.python2.Conda.CondaEnvironmentSpec;
import org.knime.python2.PythonVersion;
import org.knime.python2.config.AbstractCondaEnvironmentCreationObserver;
import org.knime.python2.config.AbstractCondaEnvironmentsPanel;
import org.knime.python2.config.CondaEnvironmentCreationObserver;
import org.knime.python2.config.ObservableValue;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class CondaEnvironmentSelectionBox extends Composite {

    private final Label m_header;

    private final ComboViewer m_environmentSelection;

    private final SettingsModelString m_environmentModel;

    /**
     * Creates a new environment selection box without a warning label and with the default creation dialog.
     *
     * @param pythonVersion The Python version of the Conda environment intended for selection/creation.
     * @param environmentModel The settings model for the Conda environment directory. May be updated asynchronously,
     *            that is, in a non-UI thread.
     * @param availableEnvironmentsModel The list of the available Conda environments. May be updated asynchronously,
     *            that is, in a non-UI thread.
     * @param selectionBoxLabel The description text for the environment selection box.
     * @param headerLabel The text of the header for the path editor's enclosing group box.
     * @param infoMessageModel The settings model for the info label. May be updated asynchronously, that is, in a
     *            non-UI thread.
     * @param errorMessageModel The settings model for the error label. May be updated asynchronously, that is, in a
     *            non-UI thread.
     * @param environmentCreator Handles the creation of new Conda environments when the user clicks the widget's "New
     *            environment..." button.
     * @param parent The parent widget.
     */
    public CondaEnvironmentSelectionBox(final PythonVersion pythonVersion, final SettingsModelString environmentModel,
        final ObservableValue<CondaEnvironmentSpec[]> availableEnvironmentsModel, final String headerLabel,
        final String selectionBoxLabel, final SettingsModelString infoMessageModel,
        final SettingsModelString errorMessageModel, final CondaEnvironmentCreationObserver environmentCreator,
        final Composite parent) {
        this(pythonVersion, environmentModel, availableEnvironmentsModel, headerLabel, selectionBoxLabel,
            infoMessageModel, null, errorMessageModel, environmentCreator, parent);
    }

    /**
     * Creates a new environment selection box with a warning label and with the default creation dialog.
     *
     * @param pythonVersion The Python version of the Conda environment intended for selection/creation.
     * @param environmentModel The settings model for the Conda environment directory. May be updated asynchronously,
     *            that is, in a non-UI thread.
     * @param availableEnvironmentsModel The list of the available Conda environments. May be updated asynchronously,
     *            that is, in a non-UI thread.
     * @param selectionBoxLabel The description text for the environment selection box.
     * @param headerLabel The text of the header for the path editor's enclosing group box.
     * @param infoMessageModel The settings model for the info label. May be updated asynchronously, that is, in a
     *            non-UI thread.
     * @param warningMessageModel The settings model for the warning label. May be updated asynchronously, that is, in a
     *            non-UI thread. May be <code>null</code> if no warning should be displayed.
     * @param errorMessageModel The settings model for the error label. May be updated asynchronously, that is, in a
     *            non-UI thread.
     * @param environmentCreator Handles the creation of new Conda environments when the user clicks the widget's "New
     *            environment..." button.
     * @param parent The parent widget.
     */
    public CondaEnvironmentSelectionBox(final PythonVersion pythonVersion, final SettingsModelString environmentModel,
        final ObservableValue<CondaEnvironmentSpec[]> availableEnvironmentsModel, final String headerLabel,
        final String selectionBoxLabel, final SettingsModelString infoMessageModel,
        final SettingsModelString warningMessageModel, final SettingsModelString errorMessageModel,
        final CondaEnvironmentCreationObserver environmentCreator, final Composite parent) {
        this(pythonVersion, environmentModel, availableEnvironmentsModel, headerLabel, selectionBoxLabel,
            infoMessageModel, warningMessageModel, errorMessageModel, environmentCreator, parent,
            shell -> new CondaEnvironmentCreationPreferenceDialog(environmentCreator, shell).open());
    }

    /**
     * Creates a new environment selection box with warning label and with a custom creation dialog.
     *
     * @param pythonVersion The Python version of the Conda environment intended for selection/creation.
     * @param environmentModel The settings model for the Conda environment directory. May be updated asynchronously,
     *            that is, in a non-UI thread.
     * @param availableEnvironmentsModel The list of the available Conda environments. May be updated asynchronously,
     *            that is, in a non-UI thread.
     * @param selectionBoxLabel The description text for the environment selection box.
     * @param headerLabel The text of the header for the path editor's enclosing group box.
     * @param infoMessageModel The settings model for the info label. May be updated asynchronously, that is, in a
     *            non-UI thread.
     * @param warningMessageModel The settings model for the warning label. May be updated asynchronously, that is, in a
     *            non-UI thread. May be <code>null</code> if no warning should be displayed.
     * @param errorMessageModel The settings model for the error label. May be updated asynchronously, that is, in a
     *            non-UI thread.
     * @param environmentCreator Handles the creation of new Conda environments when the user clicks the widget's "New
     *            environment..." button.
     * @param parent The parent widget.
     * @param openCreationDialog A consumer which opens a creation dialog with the given parent.
     */
    public CondaEnvironmentSelectionBox(final PythonVersion pythonVersion, final SettingsModelString environmentModel,
        final ObservableValue<CondaEnvironmentSpec[]> availableEnvironmentsModel, final String headerLabel,
        final String selectionBoxLabel, final SettingsModelString infoMessageModel,
        final SettingsModelString warningMessageModel, final SettingsModelString errorMessageModel,
        final AbstractCondaEnvironmentCreationObserver environmentCreator, final Composite parent,
        final Consumer<Shell> openCreationDialog) {
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
        final Combo combo = new Combo(this, SWT.DROP_DOWN | SWT.READ_ONLY);
        gridData = new GridData();
        combo.setLayoutData(gridData);
        m_environmentSelection = new ComboViewer(combo);
        m_environmentSelection.setContentProvider(ArrayContentProvider.getInstance());
        m_environmentSelection.setLabelProvider(new LabelProvider() {

            @Override
            public String getText(final Object element) {
                if (element instanceof CondaEnvironmentSpec) {
                    return ((CondaEnvironmentSpec)element).getName();
                }
                return super.getText(element);
            }
        });
        m_environmentSelection.setComparator(new ViewerComparator());

        // Environment generation:
        final Button environmentCreationButton = new Button(this, SWT.NONE);
        environmentCreationButton.setText(AbstractCondaEnvironmentsPanel.CREATE_NEW_ENVIRONMENT_BUTTON_TEXT);
        environmentCreationButton.setToolTipText("Create a new preconfigured Conda environment for "
            + pythonVersion.getName() + " that contains all packages required by the KNIME Python integration.");
        environmentCreationButton.setEnabled(environmentCreator.getIsEnvironmentCreationEnabled().getBooleanValue());
        gridData = new GridData();
        environmentCreationButton.setLayoutData(gridData);

        // Info, warning and error labels:
        gridData = new GridData();
        gridData.verticalIndent = 10;
        gridData.horizontalSpan = 3;
        if (warningMessageModel == null) {
            final InstallationStatusDisplayPanel statusDisplay =
                new InstallationStatusDisplayPanel(infoMessageModel, errorMessageModel, this);
            statusDisplay.setLayoutData(gridData);
        } else {
            final InstallationStatusDisplayPanelWithWarning statusDisplay =
                new InstallationStatusDisplayPanelWithWarning(infoMessageModel, warningMessageModel, errorMessageModel,
                    this);
            statusDisplay.setLayoutData(gridData);
        }

        // Populate environment selection, hooks:
        setAvailableEnvironments(availableEnvironmentsModel.getValue());
        setSelectedEnvironment(environmentModel.getStringValue());
        availableEnvironmentsModel.addObserver((newValue, oldValue) -> setAvailableEnvironments(newValue));
        environmentModel.addChangeListener(e -> setSelectedEnvironment(environmentModel.getStringValue()));
        m_environmentSelection
            .addSelectionChangedListener(event -> environmentModel.setStringValue(getSelectedEnvironment()));

        environmentCreator.getIsEnvironmentCreationEnabled()
            .addChangeListener(e -> performActionOnWidgetInUiThread(environmentCreationButton, () -> {
                environmentCreationButton
                    .setEnabled(environmentCreator.getIsEnvironmentCreationEnabled().getBooleanValue());
                return null;
            }, true));

        environmentCreationButton.addSelectionListener(new SelectionListener() {

            @Override
            public void widgetSelected(final SelectionEvent e) {
                openCreationDialog.accept(getShell());
            }

            @Override
            public void widgetDefaultSelected(final SelectionEvent e) {
                widgetSelected(e);
            }
        });
    }

    private void setAvailableEnvironments(final CondaEnvironmentSpec[] availableEnvironments) {
        String selectedEnvironment = m_environmentModel.getStringValue();
        performActionOnWidgetInUiThread(m_environmentSelection.getCombo(), () -> {
            m_environmentSelection.setInput(availableEnvironments);
            layout();
            return null;
        }, false);
        if (selectedEnvironment != null) {
            setSelectedEnvironment(selectedEnvironment);
        }
    }

    private String getSelectedEnvironment() {
        return performActionOnWidgetInUiThread(m_environmentSelection.getCombo(),
            () -> ((CondaEnvironmentSpec)((IStructuredSelection)m_environmentSelection.getSelection())
                .getFirstElement()).getDirectoryPath(),
            false);
    }

    private void setSelectedEnvironment(final String environmentToSelect) {
        performActionOnWidgetInUiThread(m_environmentSelection.getCombo(), () -> {
            final int numEnvironments = m_environmentSelection.getCombo().getItemCount();
            for (int i = 0; i < numEnvironments; i++) {
                final Object element = m_environmentSelection.getElementAt(i);
                if (element instanceof CondaEnvironmentSpec
                    && Objects.equals(environmentToSelect, ((CondaEnvironmentSpec)element).getDirectoryPath())) {
                    m_environmentSelection.setSelection(new StructuredSelection(element), true);
                    break;
                }
            }
            return null;
        }, false);

    }

    /**
     * @param setAsDefault If {@code true}, indicates that the Conda environment selected via this widget is the one
     *            that is used by default. If {@code false}, this indicator is cleared.
     */
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
