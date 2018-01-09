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

import org.knime.python2.extensions.serializationlibrary.interfaces.Type;

/**
 * Options for controlling the serialization process. Missing values for Int and Long columns may be replaced by a
 * sentinel value in order to avoid automatic conversion to Double in python. The sentinel may be replaced with missing
 * values for data coming from python.
 *
 * @author Clemens von Schwerin, KNIME.com, Konstanz, Germany
 *
 */

public class SerializationOptions {

    /**
     * Do not convert missing values to sentinel value by default (to python)
     */
    public final static boolean DEFAULT_CONVERT_MISSING_TO_PYTHON = false;

    /**
     * Do not convert sentinel value to missing values by default (from python)
     */
    public final static boolean DEFAULT_CONVERT_MISSING_FROM_PYTHON = false;

    /**
     * Use minimum value of column's datatype by default
     */
    public final static SentinelOption DEFAULT_SENTINEL_OPTION = SentinelOption.MIN_VAL;

    /**
     * Use 0 as default custom sentinel value
     */
    public final static int DEFAULT_SENTINEL_VALUE = 0;

    private boolean m_convertMissingToPython = DEFAULT_CONVERT_MISSING_TO_PYTHON;

    private boolean m_convertMissingFromPython = DEFAULT_CONVERT_MISSING_FROM_PYTHON;

    private SentinelOption m_sentinelOption = DEFAULT_SENTINEL_OPTION;

    private int m_sentinelValue = DEFAULT_SENTINEL_VALUE;

    /**
     * Default Constructor.
     */
    public SerializationOptions() {

    }

    /**
     * Constructor.
     *
     * @param convertMissingToPython convert missing values to sentinel on the way to python
     * @param convertMissingFromPython convert sentinel to missing values on the way from python to KNIME
     * @param sentinelOption the sentinel option
     * @param sentinelValue the sentinel value (only used if sentinelOption is CUSTOM)
     */
    public SerializationOptions(final boolean convertMissingToPython, final boolean convertMissingFromPython,
        final SentinelOption sentinelOption, final int sentinelValue) {
        m_convertMissingFromPython = convertMissingFromPython;
        m_convertMissingToPython = convertMissingToPython;
        m_sentinelOption = sentinelOption;
        m_sentinelValue = sentinelValue;
    }

    /**
     * Copy constructor.
     *
     * @param other the options to copy
     */
    public SerializationOptions(final SerializationOptions other) {
        m_convertMissingFromPython = other.getConvertMissingFromPython();
        m_convertMissingToPython = other.getConvertMissingToPython();
        m_sentinelOption = other.getSentinelOption();
        m_sentinelValue = other.getSentinelValue();
    }

    /**
     * Gets the convert missing values to python option.
     *
     * @return the convert missing values to python option
     */
    public boolean getConvertMissingToPython() {
        return m_convertMissingToPython;
    }

    /**
     * Sets the convert missing values to python option.
     *
     * @param convertMissingToPython the convert missing values to python option
     */
    public void setConvertMissingToPython(final boolean convertMissingToPython) {
        this.m_convertMissingToPython = convertMissingToPython;
    }

    /**
     * Gets the convert missing values to python option.
     *
     * @return the convert missing values to python option
     */
    public boolean getConvertMissingFromPython() {
        return m_convertMissingFromPython;
    }

    /**
     * Sets the convert missing values to python option.
     *
     * @param convertMissingFromPython the convert missing values to python option
     */
    public void setConvertMissingFromPython(final boolean convertMissingFromPython) {
        this.m_convertMissingFromPython = convertMissingFromPython;
    }

    /**
     * Gets the sentinel option.
     *
     * @return the sentinel option
     */
    public SentinelOption getSentinelOption() {
        return m_sentinelOption;
    }

    /**
     * Sets the sentinel option.
     *
     * @param sentinelOption the new sentinel option
     */
    public void setSentinelOption(final SentinelOption sentinelOption) {
        this.m_sentinelOption = sentinelOption;
    }

    /**
     * Gets the sentinel value.
     *
     * @return the sentinel value
     */
    public int getSentinelValue() {
        return m_sentinelValue;
    }

    /**
     * Sets the sentinel value.
     *
     * @param sentinelValue the new sentinel value
     */
    public void setSentinelValue(final int sentinelValue) {
        this.m_sentinelValue = sentinelValue;
    }

    /**
     * Return the sentinel value for the given type.
     *
     * @param type a {@Type} (either INTEGER or LONG)
     * @return the sentinel value based on the stored options
     * @throws IllegalArgumentException if type cannot be processed
     */

    public long getSentinelForType(final Type type) throws IllegalArgumentException {
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
        throw new IllegalArgumentException("Sentinel does not exist for type " + type.toString());
    }

    /**
     * Indicate if the given value equals the sentinel value for the given type.
     *
     * @param type a {@Type} (INTEGER or LONG)
     * @param value the value to check
     * @return value == sentinel based on the stored options and the type
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
        final int prime = 31;
        int result = 1;
        result = prime * result + (m_convertMissingFromPython ? 1231 : 1237);
        result = prime * result + (m_convertMissingToPython ? 1231 : 1237);
        result = prime * result + ((m_sentinelOption == null) ? 0 : m_sentinelOption.hashCode());
        result = prime * result + m_sentinelValue;
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SerializationOptions other = (SerializationOptions)obj;
        if (m_convertMissingFromPython != other.m_convertMissingFromPython) {
            return false;
        }
        if (m_convertMissingToPython != other.m_convertMissingToPython) {
            return false;
        }
        if (m_sentinelOption != other.m_sentinelOption) {
            return false;
        }
        if (m_sentinelValue != other.m_sentinelValue) {
            return false;
        }
        return true;
    }
}
