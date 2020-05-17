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
package org.knime.python2.prefs;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.knime.python2.Activator;
import org.knime.python2.PythonCommand;
import org.knime.python2.PythonModuleSpec;
import org.knime.python2.PythonVersion;
import org.knime.python2.config.CondaEnvironmentsConfig;
import org.knime.python2.config.ManualEnvironmentsConfig;
import org.knime.python2.config.PythonConfigStorage;
import org.knime.python2.config.PythonEnvironmentConfig;
import org.knime.python2.config.PythonEnvironmentType;
import org.knime.python2.config.PythonEnvironmentTypeConfig;
import org.knime.python2.config.PythonEnvironmentsConfig;
import org.knime.python2.config.PythonVersionConfig;
import org.knime.python2.config.SerializerConfig;
import org.knime.python2.extensions.serializationlibrary.SerializationLibraryExtensions;

/**
 * Convenience front-end of the preference-based configuration of the Python integration.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class PythonPreferences {

    private static final PreferenceStorage DEFAULT_SCOPE_PREFERENCES =
        new DefaultScopePreferenceStorage(Activator.PLUGIN_ID);

    private static final InstanceScopePreferenceStorage CURRENT_SCOPE_PREFERENCES =
        new InstanceScopePreferenceStorage(Activator.PLUGIN_ID, DEFAULT_SCOPE_PREFERENCES);

    /**
     * Accessed by preference page.
     */
    static final PythonConfigStorage CURRENT = new PreferenceWrappingConfigStorage(CURRENT_SCOPE_PREFERENCES);

    /**
     * Accessed by preference page and preferences initializer.
     */
    static final PythonConfigStorage DEFAULT = new PreferenceWrappingConfigStorage(DEFAULT_SCOPE_PREFERENCES);

    private PythonPreferences() {}

    /**
     * Initializes sensible default Python preferences on the instance scope-level if necessary.
     *
     * @see PythonPreferencesInitializer
     */
    public static void init() {
        // Backward compatibility: Old configured preferences should still have the "Manual" environment configuration
        // selected by default while we want "Conda" environment configuration as the default for new installations.
        try {
            final List<String> currentPreferencesKeys =
                Arrays.asList(CURRENT_SCOPE_PREFERENCES.getPreferences().keys());
            if (!currentPreferencesKeys.contains(PythonEnvironmentTypeConfig.CFG_KEY_ENVIRONMENT_TYPE)
                && (currentPreferencesKeys.contains(ManualEnvironmentsConfig.CFG_KEY_PYTHON2_PATH)
                    || currentPreferencesKeys.contains(ManualEnvironmentsConfig.CFG_KEY_PYTHON3_PATH))) {
                final PythonEnvironmentTypeConfig environmentTypeConfig = new PythonEnvironmentTypeConfig();
                environmentTypeConfig.getEnvironmentType().setStringValue(PythonEnvironmentType.MANUAL.getId());
                environmentTypeConfig.saveConfigTo(CURRENT);
            }
        } catch (final Exception ex) {
            // Stick with default value.
        }
    }

    /**
     * @return The currently selected default Python version.
     */
    public static PythonVersion getPythonVersionPreference() {
        final PythonVersionConfig pythonVersionConfig = new PythonVersionConfig();
        pythonVersionConfig.loadConfigFrom(CURRENT);
        return PythonVersion.fromId(pythonVersionConfig.getPythonVersion().getStringValue());
    }

    /**
     * @return The currently selected Python environment type (Conda v. manual).
     */
    public static PythonEnvironmentType getEnvironmentTypePreference() {
        final PythonEnvironmentTypeConfig environmentTypeConfig = new PythonEnvironmentTypeConfig();
        environmentTypeConfig.loadConfigFrom(CURRENT);
        return PythonEnvironmentType.fromId(environmentTypeConfig.getEnvironmentType().getStringValue());
    }

    /**
     * @return The currently selected default Python 2 command.
     */
    public static PythonCommand getPython2CommandPreference() {
        return getPythonCommandPreference(PythonVersion.PYTHON2);
    }

    /**
     * @return The currently selected default Python 3 command.
     */
    public static PythonCommand getPython3CommandPreference() {
        return getPythonCommandPreference(PythonVersion.PYTHON3);
    }

    private static PythonCommand getPythonCommandPreference(final PythonVersion pythonVersion) {
        final PythonEnvironmentType currentEnvironmentType = getEnvironmentTypePreference();
        PythonEnvironmentsConfig environmentsConfig;
        if (PythonEnvironmentType.CONDA.equals(currentEnvironmentType)) {
            environmentsConfig = new CondaEnvironmentsConfig();
        } else if (PythonEnvironmentType.MANUAL.equals(currentEnvironmentType)) {
            environmentsConfig = new ManualEnvironmentsConfig();
        } else {
            throw new IllegalStateException(
                "Selected Python environment type is neither Conda nor manual. This is an implementation error.");
        }
        environmentsConfig.loadConfigFrom(CURRENT);
        PythonEnvironmentConfig environmentConfig;
        if (PythonVersion.PYTHON2.equals(pythonVersion)) {
            environmentConfig = environmentsConfig.getPython2Config();
        } else if (PythonVersion.PYTHON3.equals(pythonVersion)) {
            environmentConfig = environmentsConfig.getPython3Config();
        } else {
            throw new IllegalStateException("Selected default Python version is neither Python 2 nor Python 3. "
                + "This is an implementation error.");
        }
        return environmentConfig.getPythonCommand();
    }

    /**
     * @return The currently selected serialization library.
     */
    public static String getSerializerPreference() {
        final SerializerConfig serializerConfig = new SerializerConfig();
        serializerConfig.loadConfigFrom(CURRENT);
        return serializerConfig.getSerializer().getStringValue();
    }

    /**
     * @return The required modules of the currently selected serialization library.
     */
    public static Collection<PythonModuleSpec> getCurrentlyRequiredSerializerModules() {
        return SerializationLibraryExtensions.getSerializationLibraryFactory(getSerializerPreference())
            .getRequiredExternalModules();
    }

    /**
     * @return the configured path to the conda installation
     */
    public static String getCondaInstallationPath() {
        final CondaEnvironmentsConfig condaEnvironmentsConfig = new CondaEnvironmentsConfig();
        condaEnvironmentsConfig.loadConfigFrom(CURRENT);
        return condaEnvironmentsConfig.getCondaDirectoryPath().getStringValue();
    }
}
