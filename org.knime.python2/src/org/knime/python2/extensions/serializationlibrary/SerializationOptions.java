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

package org.knime.python2.extensions.serializationlibrary;

import java.util.Objects;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;
import org.knime.python2.prefs.PythonPreferences;

/**
 * Options for configuring the data transfer between Java and Python. Missing values for integer and long columns may be
 * replaced by a sentinel value in order to avoid automatic conversion to double on Python side. The sentinel may be
 * replaced with missing values for data coming from Python.
 * <P>
 * Note that the serialization options will be ignored if using the Python kernel's new back end.
 * <P>
 * Implementation note: This class is intended to be immutable.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
// @Deprecated in the new kernel back end.
public class SerializationOptions {

    /**
     * The default number of rows to transfer per chunk.
     */
    public static final int DEFAULT_CHUNK_SIZE = 500000;

    /**
     * Do not convert missing values to sentinel value by default (to Python)
     */
    public static final boolean DEFAULT_CONVERT_MISSING_TO_PYTHON = false;

    /**
     * Do not convert sentinel value to missing values by default (from Python)
     */
    public static final boolean DEFAULT_CONVERT_MISSING_FROM_PYTHON = false;

    /**
     * Use minimum value of column's data type as sentinel value by default
     */
    public static final SentinelOption DEFAULT_SENTINEL_OPTION = SentinelOption.MIN_VAL;

    /**
     * Use 0 as default custom sentinel value (only if {@link SentinelOption#CUSTOM} is active).
     */
    public static final int DEFAULT_SENTINEL_VALUE = 0;

    private final String m_serializerId;

    private final int m_chunkSize;

    private final boolean m_convertMissingToPython;

    private final boolean m_convertMissingFromPython;

    private final SentinelOption m_sentinelOption;

    private final int m_sentinelValue;

    /**
     * Default constructor. Consults the {@link PythonPreferences preferences} for the
     * {@link PythonPreferences#getSerializerPreference() serializer} to use. Initializes the other values of these
     * options to the respective static fields of this class.
     */
    public SerializationOptions() {
        m_serializerId = PythonPreferences.getSerializerPreference();
        m_chunkSize = DEFAULT_CHUNK_SIZE;
        m_convertMissingToPython = DEFAULT_CONVERT_MISSING_TO_PYTHON;
        m_convertMissingFromPython = DEFAULT_CONVERT_MISSING_FROM_PYTHON;
        m_sentinelOption = DEFAULT_SENTINEL_OPTION;
        m_sentinelValue = DEFAULT_SENTINEL_VALUE;
    }

    /**
     * Consults the {@link PythonPreferences preferences} for the {@link PythonPreferences#getSerializerPreference()
     * serializer} to use.
     *
     * @param chunkSize The number of rows to transfer to/from Python per chunk of an input/output table .
     * @param convertMissingToPython {@true} if missing values shall be converted to sentinel values on the way to
     *            Python. {@code false} if they shall remain missing.
     * @param convertMissingFromPython {@true} if missing values shall be converted to sentinel values on the way back
     *            from Python. {@code false} if they shall remain missing.
     * @param sentinelOption The sentinel options to use (if applicable).
     * @param sentinelValue The sentinel value to use (only used if {@code sentinelOption} is
     *            {@link SentinelOption#CUSTOM}).
     */
    public SerializationOptions(final int chunkSize, final boolean convertMissingToPython,
        final boolean convertMissingFromPython, final SentinelOption sentinelOption, final int sentinelValue) {
        m_serializerId = PythonPreferences.getSerializerPreference();
        m_chunkSize = chunkSize;
        m_convertMissingToPython = convertMissingToPython;
        m_convertMissingFromPython = convertMissingFromPython;
        m_sentinelOption = sentinelOption;
        m_sentinelValue = sentinelValue;
    }

    /**
     * @param serializerId The {@link SerializationLibraryExtension#getId() id} of the serializer to use for data
     *            transfer between Java and Python. May be {@code null} in which case we resort to
     *            {@link PythonPreferences#getSerializerPreference()}.
     */
    private SerializationOptions(final String serializerId, final int chunkSize, final boolean convertMissingToPython,
        final boolean convertMissingFromPython, final SentinelOption sentinelOption, final int sentinelValue) {
        m_serializerId = serializerId != null ? serializerId : PythonPreferences.getSerializerPreference();
        m_chunkSize = chunkSize;
        m_convertMissingToPython = convertMissingToPython;
        m_convertMissingFromPython = convertMissingFromPython;
        m_sentinelOption = sentinelOption;
        m_sentinelValue = sentinelValue;
    }

    /**
     * @return The {@link SerializationLibraryExtension#getId() id} of the serializer to use for the data transfer
     *         between Java and Python.
     */
    public String getSerializerId() {
        return m_serializerId;
    }

    /**
     * Returns a copy of this instance for the given serializer id option. This instance remains unaffected.
     *
     * @param serializerId The {@link SerializationLibraryExtension#getId() id} of the serializer to use for the data
     *            transfer between Java and Python. May be {@code null} in which case we resort to
     *            {@link PythonPreferences#getSerializerPreference()}.
     *
     * @return A copy of this options instance with the given value set.
     */
    public SerializationOptions forSerializerId(final String serializerId) {
        return new SerializationOptions(serializerId, m_chunkSize, m_convertMissingToPython, m_convertMissingFromPython,
            m_sentinelOption, m_sentinelValue);
    }

    /**
     *
     * @return The configured number of rows to transfer to/from Python per chunk of an input/output table.
     */
    public int getChunkSize() {
        return m_chunkSize;
    }

    /**
     * Returns a copy of this instance for the given chunk size option. This instance remains unaffected.
     *
     * @param chunkSize The configured number of rows to transfer to/from Python per chunk of an input/output table.
     * @return A copy of this options instance with the given value set.
     */
    public SerializationOptions forChunkSize(final int chunkSize) {
        return new SerializationOptions(m_serializerId, chunkSize, m_convertMissingToPython, m_convertMissingFromPython,
            m_sentinelOption, m_sentinelValue);
    }

    /**
     * @return {@true} if missing values shall be converted to sentinel values on the way to Python. {@code false} if
     *         they shall remain missing.
     */
    public boolean getConvertMissingToPython() {
        return m_convertMissingToPython;
    }

    /**
     * Returns a copy of this instance for the given missing value conversion option. This instance remains unaffected.
     *
     * @param convertMissingToPython {@true} to configure that missing values shall be converted to sentinel values on
     *            the way to Python. {@code false} if they shall remain missing.
     * @return A copy of this options instance with the given value set.
     */
    public SerializationOptions forConvertMissingToPython(final boolean convertMissingToPython) {
        return new SerializationOptions(m_serializerId, m_chunkSize, convertMissingToPython, m_convertMissingFromPython,
            m_sentinelOption, m_sentinelValue);
    }

    /**
     * @return {@true} if missing values shall be converted to sentinel values on the way back from Python.
     *         {@code false} if they shall remain missing.
     */
    public boolean getConvertMissingFromPython() {
        return m_convertMissingFromPython;
    }

    /**
     * Returns a copy of this instance for the given missing value conversion option. This instance remains unaffected.
     *
     * @param convertMissingFromPython {@true} to configure that missing values shall be converted to sentinel values on
     *            the way back from Python. {@code false} if they shall remain missing.
     * @return A copy of this options instance with the given value set.
     */
    public SerializationOptions forConvertMissingFromPython(final boolean convertMissingFromPython) {
        return new SerializationOptions(m_serializerId, m_chunkSize, m_convertMissingToPython, convertMissingFromPython,
            m_sentinelOption, m_sentinelValue);
    }

    /**
     * @return The configured sentinel options to use (if applicable; see {@link #getConvertMissingToPython()} and
     *         {@link #getConvertMissingFromPython()}).
     */
    public SentinelOption getSentinelOption() {
        return m_sentinelOption;
    }

    /**
     * Returns a copy of this instance for the given sentinel option. This instance remains unaffected.
     *
     * @param sentinelOption The configured sentinel options to use (if applicable; see
     *            {@link #getConvertMissingToPython()} and {@link #getConvertMissingFromPython()}).
     * @return A copy of this options instance with the given value set.
     */
    public SerializationOptions forSentinelOption(final SentinelOption sentinelOption) {
        return new SerializationOptions(m_serializerId, m_chunkSize, m_convertMissingToPython,
            m_convertMissingFromPython, sentinelOption, m_sentinelValue);
    }

    /**
     * @return The configured sentinel value to use (only used if {@link #getSentinelOption()} is
     *         {@link SentinelOption#CUSTOM}).
     */
    public int getSentinelValue() {
        return m_sentinelValue;
    }

    /**
     * Returns a copy of this instance for the given sentinel value. This instance remains unaffected.
     *
     * @param sentinelValue The configured sentinel value to use (only used if {@link #getSentinelOption()} is
     *            {@link SentinelOption#CUSTOM}).
     * @return A copy of this options instance with the given value set.
     */
    public SerializationOptions forSentinelValue(final int sentinelValue) {
        return new SerializationOptions(m_serializerId, m_chunkSize, m_convertMissingToPython,
            m_convertMissingFromPython, m_sentinelOption, sentinelValue);
    }

    /**
     * Returns the configured sentinel value for the given type.
     *
     * @param type Either {@link Type#INTEGER} or {@link Type#LONG}.
     * @return The sentinel value based on the stored options.
     * @throws IllegalArgumentException If the given type is invalid.
     */
    public long getSentinelForType(final Type type) {
        if (m_sentinelOption == SentinelOption.CUSTOM) {
            return m_sentinelValue;
        } else if (m_sentinelOption == SentinelOption.MAX_VAL) {
            if (type == Type.INTEGER) {
                return Integer.MAX_VALUE;
            } else if (type == Type.LONG) {
                return Long.MAX_VALUE;
            }
        } else {
            if (type == Type.INTEGER) {
                return Integer.MIN_VALUE;
            } else if (type == Type.LONG) {
                return Long.MIN_VALUE;
            }
        }
        throw new IllegalArgumentException("Sentinel value does not exist for type '" + type.toString() + "'.");
    }

    /**
     * Indicates whether the given value equals the sentinel value for the given type.
     *
     * @param type Either {@link Type#INTEGER} or {@link Type#LONG}.
     * @param value The value to check.
     * @return {@code true} if the given value equals the configured sentinel value of the given type, {@code false}
     *         otherwise.
     */
    public boolean isSentinel(final Type type, final long value) {
        if (m_sentinelOption == SentinelOption.CUSTOM) {
            return value == m_sentinelValue;
        } else if (m_sentinelOption == SentinelOption.MAX_VAL) {
            if (type == Type.INTEGER) {
                return value == Integer.MAX_VALUE;
            } else if (type == Type.LONG) {
                return value == Long.MAX_VALUE;
            }
        } else {
            if (type == Type.INTEGER) {
                return value == Integer.MIN_VALUE;
            } else if (type == Type.LONG) {
                return value == Long.MIN_VALUE;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(m_serializerId, m_chunkSize, m_convertMissingToPython, m_convertMissingFromPython,
            m_sentinelOption, m_sentinelValue);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SerializationOptions other = (SerializationOptions)obj;
        final EqualsBuilder b = new EqualsBuilder();
        b.append(m_serializerId, other.m_serializerId);
        b.append(m_chunkSize, other.m_chunkSize);
        b.append(m_convertMissingToPython, other.m_convertMissingToPython);
        b.append(m_convertMissingFromPython, other.m_convertMissingFromPython);
        b.append(m_sentinelOption, other.m_sentinelOption);
        b.append(m_sentinelValue, other.m_sentinelValue);
        return b.isEquals();
    }
}
