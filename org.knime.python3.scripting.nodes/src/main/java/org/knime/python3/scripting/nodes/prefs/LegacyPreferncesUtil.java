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
 *   Jul 28, 2023 (benjamin): created
 */
package org.knime.python3.scripting.nodes.prefs;

import org.eclipse.core.runtime.preferences.DefaultScope;
import org.eclipse.core.runtime.preferences.InstanceScope;

/**
 * Utility for accessing preferences from the Python (legacy) preferences page.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
final class LegacyPreferncesUtil {

    private static final PreferenceStorage CURRENT_SCOPE_PREFERENCES =
        new PreferenceStorage("org.knime.python2", InstanceScope.INSTANCE, DefaultScope.INSTANCE);

    private static final PythonConfigStorage CURRENT = new PreferenceWrappingConfigStorage(CURRENT_SCOPE_PREFERENCES);

    private LegacyPreferncesUtil() {
        // Utility class
    }

    /**
     * Copy of org.knime.python2.prefs.PythonPreferences#getEnvironmentTypePreference() but using the copied config
     * classes
     */
    static PythonEnvironmentType getEnvironmentTypePreference() {
        // The config classes are copied over from python2. Therefore they are compatible with the python2 preferences.
        final var environmentTypeConfig = new PythonEnvironmentTypeConfig();
        environmentTypeConfig.loadConfigFrom(CURRENT);
        return PythonEnvironmentType.fromId(environmentTypeConfig.getEnvironmentType().getStringValue());
    }

    /**
     * Copy of org.knime.python2.prefs.PythonPreferences#getPythonEnvironmentsConfig(PythonEnvironmentType) but using
     * the copied config classes
     */
    static PythonEnvironmentsConfig getPythonEnvironmentsConfig(final PythonEnvironmentType environmentType) {
        PythonEnvironmentsConfig environmentsConfig;
        if (environmentType == PythonEnvironmentType.CONDA) {
            environmentsConfig = new CondaEnvironmentsConfig();
        } else if (environmentType == PythonEnvironmentType.MANUAL) {
            environmentsConfig = new ManualEnvironmentsConfig();
        } else {
            throw new IllegalStateException(
                "Selected Python environment type is neither Conda nor manual. This is an implementation error.");
        }
        environmentsConfig.loadConfigFrom(CURRENT);
        return environmentsConfig;
    }
}
