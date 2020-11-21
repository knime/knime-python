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
 *   Nov 19, 2020 (marcel): created
 */
package org.knime.python2.config;

import java.util.function.Supplier;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.python2.CondaPythonCommand;
import org.knime.python2.PythonVersion;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class PythonCommandFlowVariableConfig extends PythonCommandConfig {

    private static final String CFG_KEY_DIALOG_WAS_OPENED = "dialog_was_opened";

    private boolean m_dialogWasOpened = false;

    /**
     * @param pythonVersion The Python version of the command.
     * @param condaInstallationDirectoryPath The directory of the Conda installation. Needed to create
     *            {@link CondaPythonCommand Conda-based commands}.
     */
    public PythonCommandFlowVariableConfig(final PythonVersion pythonVersion,
        final Supplier<String> condaInstallationDirectoryPath) {
        super(pythonVersion, condaInstallationDirectoryPath);
    }

    /**
     * @param commandConfigKey The key for the command entry of this config.
     * @param pythonVersion The Python version of the command.
     * @param condaInstallationDirectoryPath The directory of the Conda installation. Needed to create
     *            {@link CondaPythonCommand Conda-based commands}.
     */
    public PythonCommandFlowVariableConfig(final String commandConfigKey, final PythonVersion pythonVersion,
        final Supplier<String> condaInstallationDirectoryPath) {
        super(commandConfigKey, pythonVersion, condaInstallationDirectoryPath);
    }

    public boolean getDialogWasOpened() {
        return m_dialogWasOpened;
    }

    public void setDialogWasOpened(final boolean dialogWasOpened) {
        m_dialogWasOpened = dialogWasOpened;
    }

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        super.saveSettingsTo(settings);
        settings.addBoolean(CFG_KEY_DIALOG_WAS_OPENED, m_dialogWasOpened);
    }

    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadSettingsFrom(settings);
        // Backward compatibility: let "old" nodes also benefit from the auto-controlled command by defaulting to
        // "false" here.
        m_dialogWasOpened = settings.getBoolean(CFG_KEY_DIALOG_WAS_OPENED, false);
    }
}
