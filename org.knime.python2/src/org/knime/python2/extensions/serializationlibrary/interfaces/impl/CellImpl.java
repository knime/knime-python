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

package org.knime.python2.extensions.serializationlibrary.interfaces.impl;

import org.knime.python2.extensions.serializationlibrary.interfaces.Cell;
import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibrary;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;

/**
 * A cell implementation holding an object of any Type that is natively serializable via a {@link SerializationLibrary}.
 *
 * @author Patrick Winter, Universität Konstanz, Konstanz, Germany
 */

public class CellImpl implements Cell {

    private final Type m_type;

    private final Object m_value;

    /**
     * Constructor for use with a Missing Value cell.
     */
    public CellImpl() {
        m_type = null;
        m_value = null;
    }

    /**
     * Instantiates a new cell impl.
     *
     * @param value the value
     */
    public CellImpl(final Boolean value) {
        m_type = Type.BOOLEAN;
        m_value = value;
    }

    /**
     * Instantiates a new cell impl.
     *
     * @param value the value
     * @param isSet true - values represent a set, false - a list
     */
    public CellImpl(final Boolean[] value, final boolean isSet) {
        m_type = isSet ? Type.BOOLEAN_SET : Type.BOOLEAN_LIST;
        m_value = value;
    }

    /**
     * Instantiates a new cell impl.
     *
     * @param value the value
     */
    public CellImpl(final Integer value) {
        m_type = Type.INTEGER;
        m_value = value;
    }

    /**
     * Instantiates a new cell impl.
     *
     * @param value the value
     * @param isSet true - values represent a set, false - a list
     */
    public CellImpl(final Integer[] value, final boolean isSet) {
        m_type = isSet ? Type.INTEGER_SET : Type.INTEGER_LIST;
        m_value = value;
    }

    /**
     * Instantiates a new cell impl.
     *
     * @param value the value
     */
    public CellImpl(final Long value) {
        m_type = Type.LONG;
        m_value = value;
    }

    /**
     * Instantiates a new cell impl.
     *
     * @param value the value
     * @param isSet true - values represent a set, false - a list
     */
    public CellImpl(final Long[] value, final boolean isSet) {
        m_type = isSet ? Type.LONG_SET : Type.LONG_LIST;
        m_value = value;
    }

    /**
     * Instantiates a new cell impl.
     *
     * @param value the value
     */
    public CellImpl(final Double value) {
        m_type = Type.DOUBLE;
        m_value = value;
    }

    /**
     * Instantiates a new cell impl.
     *
     * @param value the value
     * @param isSet true - values represent a set, false - a list
     */
    public CellImpl(final Double[] value, final boolean isSet) {
        m_type = isSet ? Type.DOUBLE_SET : Type.DOUBLE_LIST;
        m_value = value;
    }

    /**
     * Instantiates a new cell impl.
     *
     * @param value the value
     */
    public CellImpl(final String value) {
        m_type = Type.STRING;
        m_value = value;
    }

    /**
     * Instantiates a new cell impl.
     *
     * @param value the value
     * @param isSet true - values represent a set, false - a list
     */
    public CellImpl(final String[] value, final boolean isSet) {
        m_type = isSet ? Type.STRING_SET : Type.STRING_LIST;
        m_value = value;
    }

    /**
     * Instantiates a new cell impl.
     *
     * @param value the value
     */
    public CellImpl(final Byte[] value) {
        m_type = Type.BYTES;
        m_value = value;
    }

    /**
     * Instantiates a new cell impl.
     *
     * @param value the value
     * @param isSet true - values represent a set, false - a list
     */
    public CellImpl(final Byte[][] value, final boolean isSet) {
        m_type = isSet ? Type.BYTES_SET : Type.BYTES_LIST;
        m_value = value;
    }

    @Override
    public Type getColumnType() {
        return m_type;
    }

    @Override
    public boolean isMissing() {
        return m_type == null;
    }

    @Override
    public Boolean getBooleanValue() throws IllegalStateException {
        if (!(m_value instanceof Boolean)) {
            throw new IllegalStateException("Requested boolean value from cell with type: " + m_type);
        }
        return (Boolean)m_value;
    }

    @Override
    public Boolean[] getBooleanArrayValue() throws IllegalStateException {
        if (!(m_value instanceof Boolean[])) {
            throw new IllegalStateException("Requested boolean array value from cell with type: " + m_type);
        }
        return (Boolean[])m_value;
    }

    @Override
    public Integer getIntegerValue() throws IllegalStateException {
        if (!(m_value instanceof Integer)) {
            throw new IllegalStateException("Requested integer value from cell with type: " + m_type);
        }
        return (Integer)m_value;
    }

    @Override
    public Integer[] getIntegerArrayValue() throws IllegalStateException {
        if (!(m_value instanceof Integer[])) {
            throw new IllegalStateException("Requested integer array value from cell with type: " + m_type);
        }
        return (Integer[])m_value;
    }

    @Override
    public Long getLongValue() throws IllegalStateException {
        if (!(m_value instanceof Long)) {
            throw new IllegalStateException("Requested long value from cell with type: " + m_type);
        }
        return (Long)m_value;
    }

    @Override
    public Long[] getLongArrayValue() throws IllegalStateException {
        if (!(m_value instanceof Long[])) {
            throw new IllegalStateException("Requested long array value from cell with type: " + m_type);
        }
        return (Long[])m_value;
    }

    @Override
    public Double getDoubleValue() throws IllegalStateException {
        if (!(m_value instanceof Double)) {
            throw new IllegalStateException("Requested double value from cell with type: " + m_type);
        }
        return (Double)m_value;
    }

    @Override
    public Double[] getDoubleArrayValue() throws IllegalStateException {
        if (!(m_value instanceof Double[])) {
            throw new IllegalStateException("Requested double array value from cell with type: " + m_type);
        }
        return (Double[])m_value;
    }

    @Override
    public String getStringValue() throws IllegalStateException {
        if (!(m_value instanceof String)) {
            throw new IllegalStateException("Requested string value from cell with type: " + m_type);
        }
        return (String)m_value;
    }

    @Override
    public String[] getStringArrayValue() throws IllegalStateException {
        if (!(m_value instanceof String[])) {
            throw new IllegalStateException("Requested string array value from cell with type: " + m_type);
        }
        return (String[])m_value;
    }

    @Override
    public Byte[] getBytesValue() throws IllegalStateException {
        if (!(m_value instanceof Byte[])) {
            throw new IllegalStateException("Requested bytes value from cell with type: " + m_type);
        }
        return (Byte[])m_value;
    }

    @Override
    public Byte[][] getBytesArrayValue() throws IllegalStateException {
        if (!(m_value instanceof Byte[][])) {
            throw new IllegalStateException("Requested bytes array value from cell with type: " + m_type);
        }
        return (Byte[][])m_value;
    }

    @Override
    public String toString() {
        return "Type: " + m_type.toString() + ", Value: " + m_value.toString();
    }

}
