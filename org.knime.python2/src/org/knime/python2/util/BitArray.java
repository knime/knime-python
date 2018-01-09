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
 *   Aug 20, 2017 (clemens): created
 */
package org.knime.python2.util;

/**
 * Array that encodes each entry into a single bit.
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 */
public class BitArray {

    private byte[] m_values;

    private int m_size;

    /**
     * Constructor.
     * @param size the number of elements in the array
     */
    public BitArray(final int size) {
        m_values = new byte[size / 8 + (size % 8 == 0 ? 0:1)];
        m_size = size;
    }

    /**
     * Constructor.
     * @param values encoded byte array containing values
     */
    public BitArray(final byte[] values) {
        m_values = values;
        m_size = values.length * 8;
    }

    /**
     * Constructor.
     * @param values encoded byte array containing values
     * @param size the number of elements encoded
     */
    public BitArray(final byte[] values, final int size) {
        m_values = values;
        m_size = size;
    }

    /**
     * Set the array at the given position to 1.
     * NOTE: no range check is performed.
     * @param pos a position inside the array
     */
    public void setToOne(final int pos) {
        m_values[pos / 8] |= (1 << (pos % 8));
    }

    /**
     * Set the array at the given position to 0.
     * NOTE: no range check is performed.
     * @param pos a position inside the array
     */
    public void setToZero(final int pos) {
        m_values[pos / 8] &= (255 - (1 << (pos % 8)));
    }

    /**
     * Check the array at the given position.
     * NOTE: no range check is performed.
     * @param pos a position inside the array
     * @return true if value at the given position is 1, false otherwise
     */
    public boolean oneAt(final int pos) {
        return (m_values[pos / 8] & (1 << (pos % 8))) > 0;
    }

    /**
     * Return the underlying encoded byte array.
     * @return the underlying encoded byte array
     */
    public byte[] getEncodedByteArray() {
        return m_values;
    }

    /**
     * Converts the bit array to a boolean array interpreting each bit as a boolean value.
     * @return a boolean array
     */
    public boolean[] asBooleanArray() {
        boolean[] ret = new boolean[m_size];
        for(int i=0; i < m_size; i++) {
            ret[i] = oneAt(i);
        }
        return ret;
    }

    /**
     * Create a bit array by encoding each element of the given boolean array in a single bit.
     * @param ba a boolean array
     * @return the created bit array
     */
    public static BitArray fromBooleanArray(final boolean[] ba) {
        int primLn = ba.length / 8 + ((ba.length % 8 == 0) ? 0 : 1);
        byte[] primitives = new byte[primLn];

        for (int j = 0; j < ba.length; j++) {
            if (ba[j]) {
                primitives[j / 8] |= (1 << (j % 8));
            }
        }
        return new BitArray(primitives, ba.length);
    }

    /**
     * Get the size of the bit array.
     * @return the size
     */
    public int getSize() {
        return m_size;
    }

}
