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
import org.knime.python2.PythonVersion;
import org.knime.python2.config.AbstractManualEnvironmentsPanel;
import org.knime.python2.config.ManualEnvironmentConfig;
import org.knime.python2.config.ManualEnvironmentsConfig;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
final class ManualEnvironmentsPreferencePanel extends AbstractManualEnvironmentsPanel<Composite> {

    public ManualEnvironmentsPreferencePanel(final ManualEnvironmentsConfig config, final Composite parent) {
        super(config, parent);
    }

    @Override
    protected Composite createPanel(final Composite parent) {
        final Composite panel = new Composite(parent, SWT.NONE);
        panel.setLayout(new GridLayout());
        return panel;
    }

    @Override
    protected void createPython2PathWidget(final ManualEnvironmentConfig python2Config, final Composite panel) {
        createPythonPathWidget(PythonVersion.PYTHON2, python2Config, panel);
    }

    @Override
    protected void createPython3PathWidget(final ManualEnvironmentConfig python3Config, final Composite panel) {
        final StatusDisplayingFilePathEditor python3PathWidget =
            createPythonPathWidget(PythonVersion.PYTHON3, python3Config, panel);
        ((GridData)python3PathWidget.getLayoutData()).verticalIndent = 30;
    }

    private static StatusDisplayingFilePathEditor createPythonPathWidget(final PythonVersion pythonVersion,
        final ManualEnvironmentConfig pythonConfig, final Composite panel) {
        final String pythonName = pythonVersion.getName();
        final StatusDisplayingFilePathEditor pythonPathEditor = new StatusDisplayingFilePathEditor(
            pythonConfig.getExecutablePath(), true, pythonName, "Path to the " + pythonName + " start script",
            pythonConfig.getPythonInstallationInfo(), pythonConfig.getPythonInstallationError(), panel);
        final GridData gridData = new GridData();
        gridData.horizontalAlignment = SWT.FILL;
        gridData.grabExcessHorizontalSpace = true;
        pythonPathEditor.setLayoutData(gridData);
        final SettingsModelBoolean isDefaultEnvironment = pythonConfig.getIsDefaultPythonEnvironment();
        pythonPathEditor.setDisplayAsDefault(isDefaultEnvironment.getBooleanValue());
        isDefaultEnvironment
            .addChangeListener(e -> pythonPathEditor.setDisplayAsDefault(isDefaultEnvironment.getBooleanValue()));
        return pythonPathEditor;
    }
}
