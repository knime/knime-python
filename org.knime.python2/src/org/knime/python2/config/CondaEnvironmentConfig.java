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
 *   Feb 3, 2019 (marcel): created
 */
package org.knime.python2.config;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.knime.core.node.NodeLogger;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.python2.Conda;
import org.knime.python2.Conda.CondaEnvironmentSpec;
import org.knime.python2.PythonCommand;
import org.knime.python2.PythonVersion;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class CondaEnvironmentConfig extends AbstractPythonEnvironmentConfig {

    public static final CondaEnvironmentSpec PLACEHOLDER_ENV =
        new CondaEnvironmentSpec("no environment available", "no_conda_environment_selected");

    private final PythonVersion m_pythonVersion;

    private final SettingsModelString m_environmentDirectory;

    private final String m_legacyConfigKey;

    private String m_notConfiguredMessage;

    /** Only used for legacy support. See {@link #loadConfigFrom(PythonConfigStorage)} below. */
    private static final String LEGACY_PLACEHOLDER_CONDA_ENV_NAME = "<no environment>";

    /** Not managed by this config. Only needed to create command object. */
    private final SettingsModelString m_condaDirectory;

    /** Not meant for saving/loading. We just want observable values here to communicate with the view. */
    private final ObservableValue<CondaEnvironmentSpec[]> m_availableEnvironments;

    /**
     * @param pythonVersion The Python version of Conda environments described by this instance.
     */
    public CondaEnvironmentConfig(final PythonVersion pythonVersion, final String configKey,
        final String legacyConfigKey, final CondaDirectoryConfig condaDirectoryConfig) {
        m_pythonVersion = pythonVersion;
        m_environmentDirectory = new SettingsModelString(configKey, PLACEHOLDER_ENV.getDirectoryPath());
        m_legacyConfigKey = legacyConfigKey;
        m_availableEnvironments = new ObservableValue<>(new CondaEnvironmentSpec[]{PLACEHOLDER_ENV});
        m_condaDirectory = condaDirectoryConfig.getCondaDirectoryPath();
    }

    /**
     * @return The path to the directory of the Python Conda environment.
     */
    public SettingsModelString getEnvironmentDirectory() {
        return m_environmentDirectory;
    }

    /**
     * @return The list of the currently available Python Conda environments. Not meant for saving/loading.
     */
    public ObservableValue<CondaEnvironmentSpec[]> getAvailableEnvironments() {
        return m_availableEnvironments;
    }

    @Override
    public PythonCommand getPythonCommand() {
        if (PLACEHOLDER_ENV.getDirectoryPath().equals(m_environmentDirectory.getStringValue())) {
            if (m_notConfiguredMessage == null) {
                m_notConfiguredMessage = "No Python environment configured.\n"
                    + "Please configure an environment in the Preferences of the KNIME Python Integration.\n"
                    + "Instructions on how to do this can be found in the installation guide on "
                    + "https://docs.knime.com/?category=integrations.";
            }
            return new UnconfiguredCondaCommand(m_pythonVersion, m_notConfiguredMessage);
        } else {
            return Conda.createPythonCommand(m_pythonVersion, m_condaDirectory.getStringValue(),
                m_environmentDirectory.getStringValue());
        }
    }

    @Override
    public void saveDefaultsTo(final PythonConfigStorage storage) {
        // Do nothing.
    }

    @Override
    public void saveConfigTo(final PythonConfigStorage storage) {
        storage.saveStringModel(m_environmentDirectory);
    }

    @Override
    public void loadConfigFrom(final PythonConfigStorage storage) {
        m_notConfiguredMessage = null;
        if (environmentDirectoryEntryExists(storage)) {
            storage.loadStringModel(m_environmentDirectory);
        } else if (!tryUseEnvironmentNameEntry(storage)) {
            m_environmentDirectory.setStringValue(PLACEHOLDER_ENV.getDirectoryPath());
        }
    }

    private boolean environmentDirectoryEntryExists(final PythonConfigStorage storage) {
        final SettingsModelString environmentDirectory =
            new SettingsModelString(m_environmentDirectory.getKey(), LEGACY_PLACEHOLDER_CONDA_ENV_NAME);
        storage.loadStringModel(environmentDirectory);
        return !LEGACY_PLACEHOLDER_CONDA_ENV_NAME.equals(environmentDirectory.getStringValue());
    }

    /**
     * Legacy support: we used to only save the environment's name, not the path to its directory. If the name is
     * available, we need to convert it into the correct path.
     */
    private boolean tryUseEnvironmentNameEntry(final PythonConfigStorage storage) {
        final SettingsModelString environmentName =
            new SettingsModelString(m_legacyConfigKey, LEGACY_PLACEHOLDER_CONDA_ENV_NAME);
        storage.loadStringModel(environmentName);
        final String environmentNameValue = environmentName.getStringValue();
        final boolean environmentNameEntryExists = !LEGACY_PLACEHOLDER_CONDA_ENV_NAME.equals(environmentNameValue);
        return environmentNameEntryExists && tryConvertEnvironmentNameIntoDirectory(environmentNameValue, storage);
    }

    private boolean tryConvertEnvironmentNameIntoDirectory(final String environmentName,
        final PythonConfigStorage storage) {
        try {
            final List<CondaEnvironmentSpec> environments =
                new Conda(m_condaDirectory.getStringValue()).getEnvironments();
            for (final CondaEnvironmentSpec environment : environments) {
                if (environmentName.equals(environment.getName())) {
                    m_environmentDirectory.setStringValue(environment.getDirectoryPath());
                    storage.saveStringModel(m_environmentDirectory);
                    return true;
                }
            }
        } catch (final IOException ex) {
            // Name conversion failed. Fall through and use directory path's default value instead.
            NodeLogger.getLogger(CondaEnvironmentConfig.class).debug(ex);
        }
        m_notConfiguredMessage = "Could not locate Conda environment '" + environmentName
            + "'.\nPlease review the Preferences of the KNIME Python Integration and make sure the environment exists.\n"
            + "Note that Preferences entry '" + m_legacyConfigKey + "' has been deprecated in favor of '"
            + m_environmentDirectory.getKey() + "'.";
        return false;
    }

    private static final class UnconfiguredCondaCommand implements PythonCommand {

        private final PythonVersion m_pythonVersion;

        private final String m_errorMessage;

        public UnconfiguredCondaCommand(final PythonVersion pythonVersion, final String errorMessage) {
            m_pythonVersion = pythonVersion;
            m_errorMessage = errorMessage;
        }

        @Override
        public PythonVersion getPythonVersion() {
            return m_pythonVersion;
        }

        @Override
        public ProcessBuilder createProcessBuilder() throws UnconfiguredEnvironmentException {
            throw new UnconfiguredEnvironmentException(m_errorMessage);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m_pythonVersion, m_errorMessage);
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof UnconfiguredCondaCommand)) {
                return false;
            }
            final UnconfiguredCondaCommand other = (UnconfiguredCondaCommand)obj;
            return other.m_pythonVersion == m_pythonVersion //
                && other.m_errorMessage.equals(m_errorMessage);
        }

        /**
         * The PySpark node expects a legal path here. Since the error message may contain spaces or other illegal
         * characters, we return the placeholder path instead.
         */
        @Override
        public String toString() {
            return PLACEHOLDER_ENV.getDirectoryPath();
        }
    }
}
