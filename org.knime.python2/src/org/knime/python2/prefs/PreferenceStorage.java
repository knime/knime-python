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
 *   Jan 26, 2019 (marcel): created
 */
package org.knime.python2.prefs;

import java.util.function.Function;

import org.eclipse.core.runtime.Platform;
import org.eclipse.core.runtime.preferences.IEclipsePreferences;
import org.eclipse.core.runtime.preferences.IEclipsePreferences.IPreferenceChangeListener;
import org.eclipse.core.runtime.preferences.IScopeContext;
import org.knime.core.node.NodeLogger;
import org.osgi.service.prefs.BackingStoreException;
import org.osgi.service.prefs.Preferences;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class PreferenceStorage {

    private final String m_qualifier;

    private final IScopeContext m_writeContext;

    private final IScopeContext[] m_readContexts;

    /**
     * Creates a new instance that writes to and reads from the preferences identified by the given qualifier and
     * context.
     *
     * @param qualifier The qualified name (identifier) for this preference storage.
     * @param context The context to and from which this instance writes and reads preference entries.
     */
    public PreferenceStorage(final String qualifier, final IScopeContext context) {
        this(qualifier, context, new IScopeContext[]{context});
    }

    /**
     * Creates a new instance that writes to and reads from the preferences identified by the given qualifier and
     * context. Allows to specify an additional context that is consulted upon reading if the primary context does not
     * contain the requested preference entry.
     *
     * @param qualifier The qualified name (identifier) for this preference storage.
     * @param context The context to which this instance writes preference entries and from which it first tries to read
     *            preference entries.
     * @param readFallbackContext The context from which this instance reads preference entries if they are not present
     *            in {@code context}.
     */
    public PreferenceStorage(final String qualifier, final IScopeContext context,
        final IScopeContext readFallbackContext) {
        this(qualifier, context, new IScopeContext[]{context, readFallbackContext});
    }

    private PreferenceStorage(final String qualifier, final IScopeContext writeContext,
        final IScopeContext[] readContexts) {
        m_qualifier = qualifier;
        m_writeContext = writeContext;
        m_readContexts = readContexts;
    }

    IEclipsePreferences getWritePreferences() {
        return m_writeContext.getNode(m_qualifier);
    }

    private Preferences[] getReadPreferences() {
        final Preferences[] readPreferences = new Preferences[m_readContexts.length];
        for (int i = 0; i < m_readContexts.length; i++) {
            readPreferences[i] = m_readContexts[i].getNode(m_qualifier);
        }
        return readPreferences;
    }

    /**
     * Equivalent to calling the linked method on the primary context of this storage.
     *
     * @see IEclipsePreferences#addPreferenceChangeListener(IPreferenceChangeListener)
     */
    @SuppressWarnings("javadoc")
    public void addPrimaryPreferenceChangeListener(final IPreferenceChangeListener listener) {
        getWritePreferences().addPreferenceChangeListener(listener);
    }

    /**
     * Equivalent to calling the linked method on the primary context of this storage.
     *
     * @see IEclipsePreferences#removePreferenceChangeListener(IPreferenceChangeListener)
     */
    @SuppressWarnings("javadoc")
    public void removePrimaryPreferenceChangeListener(final IPreferenceChangeListener listener) {
        getWritePreferences().removePreferenceChangeListener(listener);
    }

    /**
     * @param key The key of the preference entry.
     * @param value The value of the preference entry.
     */
    public void writeBoolean(final String key, final boolean value) {
        writeValue(key, value);
    }

    /**
     * @param key The key of the preference entry.
     * @param defaultValue The default value to use if the entry identified by the given key is not present in this
     *            storage.
     * @return The read value or the provided default value.
     */
    public boolean readBoolean(final String key, final boolean defaultValue) {
        return readValue(key, defaultValue, Boolean::parseBoolean);
    }

    /**
     * @param key The key of the preference entry.
     * @param value The value of the preference entry.
     */
    public void writeInt(final String key, final int value) {
        writeValue(key, value);
    }

    /**
     * @param key The key of the preference entry.
     * @param defaultValue The default value to use if the entry identified by the given key is not present in this
     *            storage.
     * @return The read value or the provided default value.
     */
    public int readInt(final String key, final int defaultValue) {
        return readValue(key, defaultValue, Integer::parseInt);
    }

    /**
     * @param key The key of the preference entry.
     * @param value The value of the preference entry.
     */
    public void writeString(final String key, final String value) {
        writeValue(key, value);
    }

    /**
     * @param key The key of the preference entry.
     * @param defaultValue The default value to use if the entry identified by the given key is not present in this
     *            storage.
     * @return The read value or the provided default value.
     */
    public String readString(final String key, final String defaultValue) {
        return readValue(key, defaultValue, Function.identity());
    }

    private <T> void writeValue(final String key, final T value) {
        final Preferences writePreferences = getWritePreferences();
        writePreferences.put(key, value.toString());
        try {
            writePreferences.flush();
        } catch (final BackingStoreException ex) {
            NodeLogger.getLogger(PythonPreferencesInitializer.class)
                .error("Could not save Python preferences entry: " + ex.getMessage(), ex);
        }
    }

    private <T> T readValue(final String key, final T defaultValue, final Function<String, T> fromString) {
        final Preferences[] readPreferences = getReadPreferences();
        final String stringValue = Platform.getPreferencesService().get(key, defaultValue.toString(), readPreferences);
        return fromString.apply(stringValue);
    }
}
