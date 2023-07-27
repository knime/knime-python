/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 */

package org.knime.python3.scripting.nodes.prefs;

import org.eclipse.core.runtime.preferences.AbstractPreferenceInitializer;

/**
 * Preference initializer for the org.knime.python3.scripting.nodes plugin.
 *
 * @since 4.6
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class Python3ScriptingPreferencesInitializer extends AbstractPreferenceInitializer {

    /**
     * We first introduced the Python Script nodes in 4.5 such that they (re)used the Python (legacy) preference page,
     * and are now adding a dedicated preference page in 4.6. Thus we initialize the Python Scripting preference page
     * with the current settings of the legacy Python preferences because we do not want to alter the settings that were
     * already configured in the Python preference page.
     */
    @Override
    public void initializeDefaultPreferences() {
        final PythonConfigStorage defaultPreferences = Python3ScriptingPreferences.DEFAULT;

        var bundledEnvConfig = new BundledCondaEnvironmentConfig(Python3ScriptingPreferences.BUNDLED_PYTHON_ENV_ID);
        // To store the settings from the PythonPreferences we need to use "saveConfigTo" instead of "saveDefaultsTo"
        bundledEnvConfig.saveConfigTo(defaultPreferences);
        var defaultPythonEnvTypeConfig = getDefaultPythonEnvironmentTypeConfig();

        if (defaultPythonEnvTypeConfig.getEnvironmentType().getStringValue().equals(PythonEnvironmentType.CONDA.getId())
            && isPlaceholderCondaEnvSelected()) {
            // We use bundled as default if conda was currently active but no Python environment has been selected yet.
            // This should make bundled the default option for users who have not configured a Python environment to use.
            // The Python preference page does this as well, but for that the page needs to be opened first.
            new PythonEnvironmentTypeConfig(PythonEnvironmentType.BUNDLED).saveConfigTo(defaultPreferences);
        } else {
            defaultPythonEnvTypeConfig.saveConfigTo(defaultPreferences);
        }
        getDefaultCondaEnvironmentsConfig().saveConfigTo(defaultPreferences);
        getDefaultManualEnvironmentsConfig().saveConfigTo(defaultPreferences);
    }

    static PythonEnvironmentTypeConfig getDefaultPythonEnvironmentTypeConfig() {
        return new PythonEnvironmentTypeConfig(LegacyPreferncesUtil.getEnvironmentTypePreference());
    }

    static CondaEnvironmentsConfig getDefaultCondaEnvironmentsConfig() {
        return (CondaEnvironmentsConfig)LegacyPreferncesUtil.getPythonEnvironmentsConfig(PythonEnvironmentType.CONDA);
    }

    static ManualEnvironmentsConfig getDefaultManualEnvironmentsConfig() {
        return (ManualEnvironmentsConfig)LegacyPreferncesUtil.getPythonEnvironmentsConfig(PythonEnvironmentType.MANUAL);
    }

    private static boolean isPlaceholderCondaEnvSelected() {
        return Python3ScriptingPreferences.isPlaceholderCondaEnvSelected(getDefaultCondaEnvironmentsConfig());
    }
}
