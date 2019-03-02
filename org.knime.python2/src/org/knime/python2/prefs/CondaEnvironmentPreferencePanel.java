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
 *   Jan 24, 2019 (marcel): created
 */
package org.knime.python2.prefs;

import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.python2.PythonVersion;
import org.knime.python2.config.AbstractCondaEnvironmentPanel;
import org.knime.python2.config.CondaEnvironmentConfig;
import org.knime.python2.config.CondaEnvironmentCreationObserver;
import org.knime.python2.config.CondaEnvironmentsConfig;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
final class CondaEnvironmentPreferencePanel
    extends AbstractCondaEnvironmentPanel<CondaEnvironmentCreationPreferenceDialog, Composite> {

    public CondaEnvironmentPreferencePanel(final CondaEnvironmentsConfig config,
        final CondaEnvironmentCreationObserver python2EnvironmentCreator,
        final CondaEnvironmentCreationObserver python3EnvironmentCreator, final Composite parent) {
        super(config, python2EnvironmentCreator, python3EnvironmentCreator, parent);
    }

    @Override
    protected Composite createPanel(final Composite parent) {
        final Composite panel = new Composite(parent, SWT.NONE);
        panel.setLayout(new GridLayout());
        return panel;
    }

    @Override
    protected void createCondaDirectoryPathWidget(final SettingsModelString condaDirectoryPath,
        final SettingsModelString installationInfoMessage, final SettingsModelString installationErrorMessage,
        final Composite panel) {
        final StatusDisplayingFilePathEditor directoryPathEditor =
            new StatusDisplayingFilePathEditor(condaDirectoryPath, false, "Conda",
                "Path to the Conda installation directory", installationInfoMessage, installationErrorMessage, panel);
        final GridData gridData = new GridData();
        gridData.grabExcessHorizontalSpace = true;
        gridData.horizontalAlignment = SWT.FILL;
        directoryPathEditor.setLayoutData(gridData);
    }

    @Override
    protected void createPython2EnvironmentWidget(final CondaEnvironmentConfig python2Config,
        final CondaEnvironmentCreationObserver python2EnvironmentCreator, final Composite panel) {
        createPythonEnvironmentWidget(PythonVersion.PYTHON2, python2Config, python2EnvironmentCreator, panel);
    }

    @Override
    protected void createPython3EnvironmentWidget(final CondaEnvironmentConfig python3Config,
        final CondaEnvironmentCreationObserver python3EnvironmentCreator, final Composite panel) {
        createPythonEnvironmentWidget(PythonVersion.PYTHON3, python3Config, python3EnvironmentCreator, panel);
    }

    private static void createPythonEnvironmentWidget(final PythonVersion pythonVersion,
        final CondaEnvironmentConfig pythonConfig, final CondaEnvironmentCreationObserver environmentCreator,
        final Composite panel) {
        final String pythonName = pythonVersion.getName();
        final CondaEnvironmentSelectionBox environmentSelection = new CondaEnvironmentSelectionBox(
            pythonConfig.getEnvironmentName(), pythonConfig.getAvailableEnvironmentNames(), pythonName,
            "Name of the " + pythonName + " Conda environment", pythonConfig.getPythonInstallationInfo(),
            pythonConfig.getPythonInstallationError(), environmentCreator, panel);
        final GridData gridData = new GridData();
        gridData.grabExcessHorizontalSpace = true;
        gridData.horizontalAlignment = SWT.FILL;
        gridData.horizontalIndent = 20;
        environmentSelection.setLayoutData(gridData);
        final SettingsModelBoolean isDefaultEnvironment = pythonConfig.getIsDefaultPythonEnvironment();
        environmentSelection.setDisplayAsDefault(isDefaultEnvironment.getBooleanValue());
        isDefaultEnvironment
            .addChangeListener(e -> environmentSelection.setDisplayAsDefault(isDefaultEnvironment.getBooleanValue()));
    }
}
