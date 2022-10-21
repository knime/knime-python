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
package org.knime.python3.scripting.nodes.prefs;

import org.eclipse.core.runtime.preferences.DefaultScope;
import org.eclipse.core.runtime.preferences.InstanceScope;
import org.knime.conda.prefs.CondaPreferences;
import org.knime.python2.PythonCommand;
import org.knime.python2.PythonVersion;
import org.knime.python2.config.PythonConfigStorage;
import org.knime.python2.config.PythonEnvironmentConfig;
import org.knime.python2.config.PythonEnvironmentType;
import org.knime.python2.config.PythonEnvironmentTypeConfig;
import org.knime.python2.config.PythonEnvironmentsConfig;
import org.knime.python2.prefs.PreferenceStorage;
import org.knime.python2.prefs.PreferenceWrappingConfigStorage;
import org.knime.python2.prefs.PythonPreferences;

/**
 * Convenience front-end of the preference-based configuration of the Python (Labs) integration.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class Python3ScriptingPreferences {
    private static final String PLUGIN_ID = "org.knime.python3.scripting.nodes";

    /**
     * The identifier under which we can look up the bundled Python environment for scripting nodes at the extension
     * point
     */
    public static final String BUNDLED_PYTHON_ENV_ID = "org_knime_pythonscripting";

    private static final PreferenceStorage DEFAULT_SCOPE_PREFERENCES =
        new PreferenceStorage(PLUGIN_ID, DefaultScope.INSTANCE);

    private static final PreferenceStorage CURRENT_SCOPE_PREFERENCES =
        new PreferenceStorage(PLUGIN_ID, InstanceScope.INSTANCE, DefaultScope.INSTANCE);

    /**
     * Accessed by preference page.
     */
    static final PythonConfigStorage CURRENT = new PreferenceWrappingConfigStorage(CURRENT_SCOPE_PREFERENCES);

    /**
     * Accessed by preference page and preferences initializer.
     */
    static final PythonConfigStorage DEFAULT = new PreferenceWrappingConfigStorage(DEFAULT_SCOPE_PREFERENCES);

    static {
        // The Python3ScriptingPreferencesInitializer is only called when we access the node in the default preferences
        // We do that once before any other method can be called to be sure that the initializer has finished and
        // other methods can be called in parallel
        DefaultScope.INSTANCE.getNode(PLUGIN_ID);
    }

    private Python3ScriptingPreferences() {
    }

    /**
     * @return The currently selected default Python version.
     */
    public static PythonVersion getPythonVersionPreference() {
        return PythonVersion.PYTHON3;
    }

    /**
     * @return The currently selected Python environment type (Bundled vs. Conda vs. Manual).
     */
    public static PythonEnvironmentType getEnvironmentTypePreference() {
        PythonEnvironmentType pythonPreferenceSetting = PythonPreferences.getEnvironmentTypePreference();
        final var environmentTypeConfig = new PythonEnvironmentTypeConfig(pythonPreferenceSetting);
        environmentTypeConfig.loadConfigFrom(CURRENT);
        return PythonEnvironmentType.fromId(environmentTypeConfig.getEnvironmentType().getStringValue());
    }

    /**
     * @return The currently selected default Python 3 command.
     */
    public static PythonCommand getPython3CommandPreference() {
        return getPythonCommandPreference(PythonVersion.PYTHON3);
    }

    private static BundledCondaEnvironmentConfig getBundledCondaEnvironmentConfig() {
        final var bundledEnvConfig = new BundledCondaEnvironmentConfig(BUNDLED_PYTHON_ENV_ID);
        bundledEnvConfig.loadConfigFrom(CURRENT);
        return bundledEnvConfig;
    }

    private static PythonCommand getPythonCommandPreference(final PythonVersion pythonVersion) {
        final var envType = getEnvironmentTypePreference();
        PythonEnvironmentsConfig environmentsConfig;

        if (envType == PythonEnvironmentType.BUNDLED) {
            environmentsConfig = getBundledCondaEnvironmentConfig();
        } else {
            environmentsConfig = PythonPreferences.getPythonEnvironmentsConfig(envType);
            environmentsConfig.loadConfigFrom(CURRENT);
        }

        PythonEnvironmentConfig environmentConfig;
        if (PythonVersion.PYTHON3 == pythonVersion) {
            environmentConfig = environmentsConfig.getPython3Config();
        } else {
            throw new IllegalStateException(
                "Selected default Python version is not Python 3. This is an implementation error.");
        }
        return environmentConfig.getPythonCommand();
    }

    /**
     * @return the configured path to the conda installation
     */
    public static String getCondaInstallationPath() {
        return CondaPreferences.getCondaInstallationDirectory();
    }
}
