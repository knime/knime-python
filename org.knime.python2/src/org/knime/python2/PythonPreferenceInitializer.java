/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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

package org.knime.python2;

import org.eclipse.core.runtime.preferences.AbstractPreferenceInitializer;
import org.eclipse.core.runtime.preferences.DefaultScope;
import org.eclipse.core.runtime.preferences.IEclipsePreferences;
import org.knime.core.node.NodeLogger;
import org.osgi.service.prefs.BackingStoreException;

/**
 * Preference Initializer for the org.knime.python2 plugin.
 *
 * @author Patrick Winter, Universität Konstanz, Konstanz, Germany
 */

public class PythonPreferenceInitializer extends AbstractPreferenceInitializer {

    /**
     * Use the command 'python' without a specified location as default
     */
    public static final String DEFAULT_PYTHON_2_PATH = "python";

    /**
     * Use the command 'python3' without a specified location as default
     */
    public static final String DEFAULT_PYTHON_3_PATH = "python3";

    /**
     * Use flatbuffers serialization as a default
     */
    public static final String DEFAULT_SERIALIZER_ID = "org.knime.serialization.flatbuffers.column";

    /**
     * The initial state of the default python version
     */
    public static final String DEFAULT_DEFAULT_PYTHON_OPTION_CFG = "python3";

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonPreferenceInitializer.class);

    @Override
    public void initializeDefaultPreferences() {
        final IEclipsePreferences prefs = DefaultScope.INSTANCE.getNode(Activator.PLUGIN_ID);
        prefs.put(PythonPreferencePage.PYTHON_2_PATH_CFG, DEFAULT_PYTHON_2_PATH);
        prefs.put(PythonPreferencePage.PYTHON_3_PATH_CFG, DEFAULT_PYTHON_3_PATH);
        prefs.put(PythonPreferencePage.SERIALIZER_ID_CFG, DEFAULT_SERIALIZER_ID);
        prefs.put(PythonPreferencePage.DEFAULT_PYTHON_OPTION_CFG, DEFAULT_DEFAULT_PYTHON_OPTION_CFG);
        try {
            prefs.flush();
        } catch (final BackingStoreException e) {
            LOGGER.error("Could not save preferences: " + e.getMessage(), e);
        }
    }

}
