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
 *   Nov 13, 2020 (marcel): created
 */
package org.knime.python2.config;

import java.io.File;
import java.util.Optional;
import java.util.function.Supplier;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.context.NodeSettingsPersistable;
import org.knime.python2.CondaPythonCommand;
import org.knime.python2.ManualPythonCommand;
import org.knime.python2.PythonCommand;
import org.knime.python2.PythonVersion;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz Germany
 */
public class PythonCommandConfig implements NodeSettingsPersistable {

    private static final String DEFAULT_CFG_KEY_PYTHON2_COMMAND = "python2_command";

    private static final String DEFAULT_CFG_KEY_PYTHON3_COMMAND = "python3_command";

    private static final String INDICATE_FALLBACK_TO_PREFERENCES_COMMAND_VALUE = "";

    private final PythonVersion m_pythonVersion;

    private final Supplier<String> m_condaInstallationDirectoryPath;

    private final String m_configKey;

    private PythonCommand m_command;

    /**
     * @param pythonVersion The Python version of the command.
     * @param condaInstallationDirectoryPath The directory of the Conda installation. Needed to create
     *            {@link CondaPythonCommand Conda-based commands}.
     */
    public PythonCommandConfig(final PythonVersion pythonVersion,
        final Supplier<String> condaInstallationDirectoryPath) {
        this(pythonVersion == PythonVersion.PYTHON2 //
            ? DEFAULT_CFG_KEY_PYTHON2_COMMAND //
            : DEFAULT_CFG_KEY_PYTHON3_COMMAND, //
            pythonVersion, condaInstallationDirectoryPath);
    }

    /**
     * @param configKey The key for this config.
     * @param pythonVersion The Python version of the command.
     * @param condaInstallationDirectoryPath The directory of the Conda installation. Needed to create
     *            {@link CondaPythonCommand Conda-based commands}.
     */
    public PythonCommandConfig(final String configKey, final PythonVersion pythonVersion,
        final Supplier<String> condaInstallationDirectoryPath) {
        m_pythonVersion = pythonVersion;
        m_configKey = configKey;
        m_condaInstallationDirectoryPath = condaInstallationDirectoryPath;
    }

    /**
     * @return The key of this configuration entry.
     */
    public String getConfigKey() {
        return m_configKey;
    }

    /**
     * @return The Python command persisted by this config, if any.
     */
    public Optional<PythonCommand> getCommand() {
        return Optional.ofNullable(m_command);
    }

    /**
     * @param command The command to set. May be {@code null} in which case no command is configured and
     *            {@link #getCommand()} returns an empty optional.
     */
    public void setCommand(final PythonCommand command) {
        m_command = command;
    }

    /**
     * @param commandString The string representation of the command to set. May be {@code null} or empty in which case
     *            no command is configured and {@link #getCommand()} returns an empty optional.
     */
    public void setCommandString(final String commandString) {
        setCommand(commandFromString(commandString));
    }

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addString(m_configKey, commandToString(m_command));
    }

    /**
     * Validates the given settings object.
     *
     * @param settings The settings object.
     * @throws InvalidSettingsException If the settings object is invalid.
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        loadSettingsFrom(settings);
    }

    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        final String commandString = settings.getString(m_configKey, commandToString(m_command));
        m_command = commandFromString(commandString);
    }

    private static String commandToString(final PythonCommand command) {
        final String commandString;
        if (command != null) {
            if (command instanceof CondaPythonCommand) {
                commandString = ((CondaPythonCommand)command).getEnvironmentDirectoryPath();
            } else if (command instanceof ManualPythonCommand) {
                commandString = command.toString();
            } else {
                throw new UnsupportedOperationException("Unsupported Python command type: " + command.getClass());
            }
        } else {
            commandString = INDICATE_FALLBACK_TO_PREFERENCES_COMMAND_VALUE;
        }
        return commandString;
    }

    private PythonCommand commandFromString(final String commandString) {
        final PythonCommand command;
        if (commandString != null && !INDICATE_FALLBACK_TO_PREFERENCES_COMMAND_VALUE.equals(commandString)) {
            if (new File(commandString).isDirectory()) { // NOSONAR Command is supposed to be user configurable.
                final String condaInstallationDirectoryPath = m_condaInstallationDirectoryPath.get();
                command = new CondaPythonCommand(m_pythonVersion, condaInstallationDirectoryPath, commandString);
            } else {
                command = new ManualPythonCommand(m_pythonVersion, commandString);
            }
        } else {
            command = null;
        }
        return command;
    }
}
