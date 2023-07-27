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
package org.knime.python3.scripting.nodes.prefs;

import static org.knime.python3.scripting.nodes.prefs.PythonPreferenceUtils.performActionOnWidgetInUiThread;
import static org.knime.python3.scripting.nodes.prefs.PythonPreferenceUtils.setLabelTextAndResize;

import java.util.Objects;

import org.eclipse.jface.resource.FontDescriptor;
import org.eclipse.jface.viewers.ArrayContentProvider;
import org.eclipse.jface.viewers.ComboViewer;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.LabelProvider;
import org.eclipse.jface.viewers.StructuredSelection;
import org.eclipse.jface.viewers.ViewerComparator;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.knime.conda.CondaEnvironmentIdentifier;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Copied and modified from org.knime.python2.prefs.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
final class CondaEnvironmentSelectionBox extends Composite {

    private final Label m_header;

    private final ComboViewer m_environmentSelection;

    private final SettingsModelString m_environmentModel;

    /**
     * Creates a new environment selection box with warning label and with a custom creation dialog.
     *
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
     * @param parent The parent widget.
     */
    public CondaEnvironmentSelectionBox(final SettingsModelString environmentModel,
        final ObservableValue<CondaEnvironmentIdentifier[]> availableEnvironmentsModel, final String headerLabel,
        final String selectionBoxLabel, final SettingsModelString infoMessageModel,
        final SettingsModelString errorMessageModel, final Composite parent) {
        super(parent, SWT.NONE);
        m_environmentModel = environmentModel;

        final var gridLayout = new GridLayout();
        gridLayout.numColumns = 2;
        setLayout(gridLayout);

        // Header:
        m_header = new Label(this, SWT.NONE);
        var descriptor = FontDescriptor.createFrom(m_header.getFont());
        descriptor = descriptor.setStyle(SWT.BOLD);
        m_header.setFont(descriptor.createFont(m_header.getDisplay()));
        m_header.setText(headerLabel);
        var gridData = new GridData();
        gridData.horizontalSpan = 2;
        m_header.setLayoutData(gridData);

        // Environment selection:
        var environmentSelectionLabel = new Label(this, SWT.NONE);
        gridData = new GridData();
        environmentSelectionLabel.setLayoutData(gridData);
        environmentSelectionLabel.setText(selectionBoxLabel);
        var combo = new Combo(this, SWT.DROP_DOWN | SWT.READ_ONLY);
        gridData = new GridData();
        combo.setLayoutData(gridData);
        m_environmentSelection = new ComboViewer(combo);
        m_environmentSelection.setContentProvider(ArrayContentProvider.getInstance());
        m_environmentSelection.setLabelProvider(new LabelProvider() {

            @Override
            public String getText(final Object element) {
                if (element instanceof CondaEnvironmentIdentifier id) {
                    return id.getName();
                }
                return super.getText(element);
            }
        });
        m_environmentSelection.setComparator(new ViewerComparator());

        // Info, warning and error labels:
        gridData = new GridData();
        gridData.verticalIndent = 10;
        gridData.horizontalSpan = 2;
        var statusDisplay = new InstallationStatusDisplayPanel(infoMessageModel, errorMessageModel, this);
        statusDisplay.setLayoutData(gridData);

        // Populate environment selection, hooks:
        setAvailableEnvironments(availableEnvironmentsModel.getValue());
        setSelectedEnvironment(environmentModel.getStringValue());
        availableEnvironmentsModel.addObserver((newValue, oldValue) -> setAvailableEnvironments(newValue));
        environmentModel.addChangeListener(e -> setSelectedEnvironment(environmentModel.getStringValue()));
        m_environmentSelection
            .addSelectionChangedListener(event -> environmentModel.setStringValue(getSelectedEnvironment()));
    }

    private void setAvailableEnvironments(final CondaEnvironmentIdentifier[] availableEnvironments) {
        var selectedEnvironment = m_environmentModel.getStringValue(); // NOSONAR: has to happen before we change the options
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
            ((CondaEnvironmentIdentifier)((IStructuredSelection)m_environmentSelection.getSelection())
                .getFirstElement())::getDirectoryPath,
            false);
    }

    private void setSelectedEnvironment(final String environmentToSelect) {
        performActionOnWidgetInUiThread(m_environmentSelection.getCombo(), () -> {
            final int numEnvironments = m_environmentSelection.getCombo().getItemCount();
            for (int i = 0; i < numEnvironments; i++) {
                final Object element = m_environmentSelection.getElementAt(i);
                if (element instanceof CondaEnvironmentIdentifier id
                    && Objects.equals(environmentToSelect, id.getDirectoryPath())) {
                    m_environmentSelection.setSelection(new StructuredSelection(element), true);
                    break;
                }
            }
            return null;
        }, false);

    }

    static final class InstallationStatusDisplayPanel extends Composite {

        /**
         * @param infoMessageModel May be updated asynchronously, that is, in a non-UI thread. May contain or be set to
         *            a {@code null} {@link SettingsModelString#getStringValue() value}.
         * @param errorMessageModel May be updated asynchronously, that is, in a non-UI thread. May contain or be set to
         *            a {@code null} {@link SettingsModelString#getStringValue() value}.
         * @param parent The parent control.
         */
        public InstallationStatusDisplayPanel(final SettingsModelString infoMessageModel,
            final SettingsModelString errorMessageModel, final Composite parent) {
            super(parent, SWT.NONE);
            final var gridLayout = new GridLayout();
            gridLayout.marginWidth = 0;
            gridLayout.marginHeight = 0;
            gridLayout.verticalSpacing = 0;
            setLayout(gridLayout);

            // Info label:
            final var info = new Label(this, SWT.NONE);
            setLabelText(info, infoMessageModel.getStringValue());
            info.setLayoutData(new GridData(GridData.FILL, GridData.CENTER, true, false));

            // Error label:
            final var error = new Label(this, SWT.NONE);
            final var red = new Color(parent.getDisplay(), 255, 0, 0);
            error.setForeground(red);
            error.addDisposeListener(e -> red.dispose());
            setLabelText(error, errorMessageModel.getStringValue());
            error.setLayoutData(new GridData(GridData.FILL, GridData.CENTER, true, false));

            // Hooks:
            infoMessageModel.addChangeListener(e -> setLabelText(info, infoMessageModel.getStringValue()));
            errorMessageModel.addChangeListener(e -> setLabelText(error, errorMessageModel.getStringValue()));
        }

        private static void setLabelText(final Label label, final String text) {
            performActionOnWidgetInUiThread(label, () -> {
                setLabelTextAndResize(label, text);
                return null;
            }, false);
        }
    }

}
