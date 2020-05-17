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
 *   Apr 18, 2020 (marcel): created
 */
package org.knime.python2.prefs;

import org.eclipse.core.runtime.preferences.IEclipsePreferences;
import org.knime.core.node.NodeLogger;
import org.osgi.service.prefs.BackingStoreException;

/**
 * Abstract base class for the different preference storages (instance scope, default scope) of the KNIME Python
 * integration.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public abstract class AbstractPreferenceStorage implements PreferenceStorage {

    /**
     * The qualified name for this preference storage, serves as identifier in the {@link #getPreferences() underlying
     * preferences}.
     */
    protected final String m_qualifier;

    /**
     * @param qualifier The qualified name (identifier) for this preference storage.
     */
    public AbstractPreferenceStorage(final String qualifier) {
        m_qualifier = qualifier;
    }

    /**
     * @return The underlying {@link IEclipsePreferences} which this instance wraps.
     */
    protected abstract IEclipsePreferences getPreferences();

    @Override
    public void writeBoolean(final String key, final boolean value) {
        getPreferences().putBoolean(key, value);
        flush();
    }

    @Override
    public void writeInt(final String key, final int value) {
        getPreferences().putInt(key, value);
        flush();
    }

    @Override
    public void writeString(final String key, final String value) {
        getPreferences().put(key, value);
        flush();
    }

    private void flush() {
        try {
            getPreferences().flush();
        } catch (final BackingStoreException ex) {
            NodeLogger.getLogger(PythonPreferencesInitializer.class)
                .error("Could not save Python preferences entry: " + ex.getMessage(), ex);
        }
    }
}
