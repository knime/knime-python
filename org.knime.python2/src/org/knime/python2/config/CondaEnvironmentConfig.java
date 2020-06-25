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

import org.eclipse.core.runtime.Platform;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.python2.Conda;
import org.knime.python2.Conda.CondaEnvironmentSpec;
import org.knime.python2.PythonCommand;
import org.knime.python2.PythonVersion;
import org.knime.python2.prefs.PreferenceWrappingConfigStorage;
import org.osgi.service.prefs.Preferences;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class CondaEnvironmentConfig extends AbstractPythonEnvironmentConfig {

    private PythonVersion m_pythonVersion;

    private final SettingsModelString m_environmentDirectory;

    /** Only used for legacy support. See {@link #loadConfigFrom(PythonConfigStorage)} below. */
    private static final String LEGACY_CFG_KEY_PYTHON2_CONDA_ENV_NAME = "python2CondaEnvironmentName";

    /** Only used for legacy support. See {@link #loadConfigFrom(PythonConfigStorage)} below. */
    private static final String LEGACY_CFG_KEY_PYTHON3_CONDA_ENV_NAME = "python3CondaEnvironmentName";

    /** Only used for legacy support. See {@link #loadConfigFrom(PythonConfigStorage)} below. */
    private static final String LEGACY_PLACEHOLDER_CONDA_ENV_NAME = "<no environment>";

    /** Not managed by this config. Only needed to create command object. */
    private final SettingsModelString m_condaDirectory;

    /** Not meant for saving/loading. We just want observable values here to communicate with the view. */
    private final ObservableValue<CondaEnvironmentSpec[]> m_availableEnvironments;

    /**
     * @param pythonVersion The Python version of Conda environments described by this instance.
     * @param environmentDirectoryConfigKey The identifier of the Conda environment directory config. Used for
     *            saving/loading the path to the environment's directory.
     * @param defaultEnvironment The initial Conda environment.
     * @param condaDirectory The settings model that specifies the Conda installation directory. Not saved/loaded or
     *            otherwise managed by this config.
     */
    public CondaEnvironmentConfig(final PythonVersion pythonVersion, //
        final String environmentDirectoryConfigKey, //
        final CondaEnvironmentSpec defaultEnvironment, //
        final SettingsModelString condaDirectory) {
        m_pythonVersion = pythonVersion;
        m_environmentDirectory =
            new SettingsModelString(environmentDirectoryConfigKey, defaultEnvironment.getDirectoryPath());
        m_availableEnvironments = new ObservableValue<>(new CondaEnvironmentSpec[]{defaultEnvironment});
        m_condaDirectory = condaDirectory;
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
        return Conda.createPythonCommand(m_pythonVersion, m_condaDirectory.getStringValue(),
            m_environmentDirectory.getStringValue());
    }

    @Override
    public void saveConfigTo(final PythonConfigStorage storage) {
        storage.saveStringModel(m_environmentDirectory);
    }

    @Override
    public void loadConfigFrom(final PythonConfigStorage storage) {
        // Legacy support: we used to only save the environment's name, not the path to its directory. If only the name
        // is available, we need to convert it into the correct path.
        if (storage instanceof PreferenceWrappingConfigStorage) {
            final Preferences preferences =
                ((PreferenceWrappingConfigStorage)storage).getWrappedPreferences().getWritePreferences();
            final boolean isLegacy = Platform.getPreferencesService().get(m_environmentDirectory.getKey(), null,
                new Preferences[]{preferences}) == null;
            if (isLegacy) {
                final SettingsModelString environmentName = new SettingsModelString(
                    m_pythonVersion == PythonVersion.PYTHON2 //
                        ? LEGACY_CFG_KEY_PYTHON2_CONDA_ENV_NAME //
                        : LEGACY_CFG_KEY_PYTHON3_CONDA_ENV_NAME, //
                    LEGACY_PLACEHOLDER_CONDA_ENV_NAME);
                storage.loadStringModel(environmentName);
                try {
                    final String environmentNameValue = environmentName.getStringValue();
                    final List<CondaEnvironmentSpec> environments =
                        new Conda(m_condaDirectory.getStringValue()).getEnvironments();
                    for (final CondaEnvironmentSpec environment : environments) {
                        if (environmentNameValue.equals(environment.getName())) {
                            m_environmentDirectory.setStringValue(environment.getDirectoryPath());
                            break;
                        }
                    }
                } catch (final IOException ex) {
                    // Keep directory path's default value.
                }
                return;
            }
        }
        storage.loadStringModel(m_environmentDirectory);
    }
}
