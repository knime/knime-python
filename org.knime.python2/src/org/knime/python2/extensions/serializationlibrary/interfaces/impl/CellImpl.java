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

    private final byte[] m_missing;

    private static byte B_ZERO = (byte) 0;
    private static byte B_ONE = (byte) 1;

    /**
     * Constructor for use with a Missing Value cell.
     */
    public CellImpl() {
        m_type = null;
        m_value = null;
        m_missing = null;
    }

    /**
     * Instantiates a new cell impl.
     *
     * @param value the value
     */
    public CellImpl(final boolean value) {
        m_type = Type.BOOLEAN;
        m_value = value;
        m_missing = null;
    }

    /**
     * Instantiates a new cell impl with an integer list.
     *
     * @param value the value
     * @param missings bit encoded missing values (0 missing, 1 available)
     */
    public CellImpl(final boolean[] value, final byte[] missings) {
        m_type = Type.BOOLEAN_LIST;
        m_value = value;
        m_missing = missings;
    }

    /**
     * Instantiates a new cell impl with an integer set.
     *
     * @param value the value
     * @param hasMissing true if the set contains a missing value
     */
    public CellImpl(final boolean[] value, final boolean hasMissing) {
        m_type = Type.BOOLEAN_SET;
        m_value = value;
        m_missing = new byte[1];
        m_missing[0] = hasMissing ? B_ZERO:B_ONE;
    }

    /**
     * Instantiates a new cell impl.
     *
     * @param value the value
     */
    public CellImpl(final int value) {
        m_type = Type.INTEGER;
        m_value = value;
        m_missing = null;
    }

    /**
     * Instantiates a new cell impl with an integer list.
     *
     * @param value the value
     * @param missings bit encoded missing values (0 missing, 1 available)
     */
    public CellImpl(final int[] value, final byte[] missings) {
        m_type = Type.INTEGER_LIST;
        m_value = value;
        m_missing = missings;
    }

    /**
     * Instantiates a new cell impl with an integer set.
     *
     * @param value the value
     * @param hasMissing true if the set contains a missing value
     */
    public CellImpl(final int[] value, final boolean hasMissing) {
        m_type = Type.INTEGER_SET;
        m_value = value;
        m_missing = new byte[1];
        m_missing[0] = hasMissing ? B_ZERO:B_ONE;
    }

    /**
     * Instantiates a new cell impl.
     *
     * @param value the value
     */
    public CellImpl(final long value) {
        m_type = Type.LONG;
        m_value = value;
        m_missing = null;
    }

    /**
     * Instantiates a new cell impl with an integer list.
     *
     * @param value the value
     * @param missings bit encoded missing values (0 missing, 1 available)
     */
    public CellImpl(final long[] value, final byte[] missings) {
        m_type = Type.LONG_LIST;
        m_value = value;
        m_missing = missings;
    }

    /**
     * Instantiates a new cell impl with an integer set.
     *
     * @param value the value
     * @param hasMissing true if the set contains a missing value
     */
    public CellImpl(final long[] value, final boolean hasMissing) {
        m_type = Type.LONG_SET;
        m_value = value;
        m_missing = new byte[1];
        m_missing[0] = hasMissing ? B_ZERO:B_ONE;
    }

    /**
     * Instantiates a new cell impl.
     *
     * @param value the value
     */
    public CellImpl(final double value) {
        m_type = Type.DOUBLE;
        m_value = value;
        m_missing = null;
    }

    /**
     * Instantiates a new cell impl with an integer list.
     *
     * @param value the value
     * @param missings bit encoded missing values (0 missing, 1 available)
     */
    public CellImpl(final double[] value, final byte[] missings) {
        m_type = Type.DOUBLE_LIST;
        m_value = value;
        m_missing = missings;
    }

    /**
     * Instantiates a new cell impl with an integer set.
     *
     * @param value the value
     * @param hasMissing true if the set contains a missing value
     */
    public CellImpl(final double[] value, final boolean hasMissing) {
        m_type = Type.DOUBLE_SET;
        m_value = value;
        m_missing = new byte[1];
        m_missing[0] = hasMissing ? B_ZERO:B_ONE;
    }
    /**
     * Instantiates a new cell impl.
     *
     * @param value the value
     */
    public CellImpl(final String value) {
        m_type = Type.STRING;
        m_value = value;
        m_missing = null;
    }

    /**
     * Instantiates a new cell impl with an integer list.
     *
     * @param value the value
     * @param missings bit encoded missing values (0 missing, 1 available)
     */
    public CellImpl(final String[] value, final byte[] missings) {
        m_type = Type.STRING_LIST;
        m_value = value;
        m_missing = missings;
    }

    /**
     * Instantiates a new cell impl with an integer set.
     *
     * @param value the value
     * @param hasMissing true if the set contains a missing value
     */
    public CellImpl(final String[] value, final boolean hasMissing) {
        m_type = Type.STRING_SET;
        m_value = value;
        m_missing = new byte[1];
        m_missing[0] = hasMissing ? B_ZERO:B_ONE;
    }

    /**
     * Instantiates a new cell impl.
     *
     * @param value the value
     */
    public CellImpl(final byte[] value) {
        m_type = Type.BYTES;
        m_value = value;
        m_missing = null;
    }

    /**
     * Instantiates a new cell impl with an integer list.
     *
     * @param value the value
     * @param missings bit encoded missing values (0 missing, 1 available)
     */
    public CellImpl(final byte[][] value, final byte[] missings) {
        m_type = Type.BYTES_LIST;
        m_value = value;
        m_missing = missings;
    }

    /**
     * Instantiates a new cell impl with an integer set.
     *
     * @param value the value
     * @param hasMissing true if the set contains a missing value
     */
    public CellImpl(final byte[][] value, final boolean hasMissing) {
        m_type = Type.BYTES_SET;
        m_value = value;
        m_missing = new byte[1];
        m_missing[0] = hasMissing ? B_ZERO:B_ONE;
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
    public boolean getBooleanValue() throws IllegalStateException {
        if (!(m_value instanceof Boolean)) {
            throw new IllegalStateException("Requested boolean value from cell with type: " + m_type);
        }
        return (boolean)m_value;
    }

    @Override
    public boolean[] getBooleanArrayValue() throws IllegalStateException {
        if (!(m_value instanceof boolean[])) {
            throw new IllegalStateException("Requested boolean array value from cell with type: " + m_type);
        }
        return (boolean[])m_value;
    }

    @Override
    public int getIntegerValue() throws IllegalStateException {
        if (!(m_value instanceof Integer)) {
            throw new IllegalStateException("Requested integer value from cell with type: " + m_type);
        }
        return (int)m_value;
    }

    @Override
    public int[] getIntegerArrayValue() throws IllegalStateException {
        if (!(m_value instanceof int[])) {
            throw new IllegalStateException("Requested integer array value from cell with type: " + m_type);
        }
        return (int[])m_value;
    }

    @Override
    public long getLongValue() throws IllegalStateException {
        if (!(m_value instanceof Long)) {
            throw new IllegalStateException("Requested long value from cell with type: " + m_type);
        }
        return (long)m_value;
    }

    @Override
    public long[] getLongArrayValue() throws IllegalStateException {
        if (!(m_value instanceof long[])) {
            throw new IllegalStateException("Requested long array value from cell with type: " + m_type);
        }
        return (long[])m_value;
    }

    @Override
    public double getDoubleValue() throws IllegalStateException {
        if (!(m_value instanceof Double)) {
            throw new IllegalStateException("Requested double value from cell with type: " + m_type);
        }
        return (double)m_value;
    }

    @Override
    public double[] getDoubleArrayValue() throws IllegalStateException {
        if (!(m_value instanceof double[])) {
            throw new IllegalStateException("Requested double array value from cell with type: " + m_type);
        }
        return (double[])m_value;
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
    public byte[] getBytesValue() throws IllegalStateException {
        if (!(m_value instanceof byte[])) {
            throw new IllegalStateException("Requested bytes value from cell with type: " + m_type);
        }
        return (byte[])m_value;
    }

    @Override
    public byte[][] getBytesArrayValue() throws IllegalStateException {
        if (!(m_value instanceof byte[][])) {
            throw new IllegalStateException("Requested bytes array value from cell with type: " + m_type);
        }
        return (byte[][])m_value;
    }

    @Override
    public String toString() {
        return "Type: " + m_type.toString() + ", Value: " + m_value.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isMissing(final int index) {
        return (m_missing[index / 8] & (1 << (index % 8))) == 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasMissingInSet() {
       return m_missing[0] == B_ZERO;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getBitEncodedMissingListValues() {
        return m_missing;
    }

}
