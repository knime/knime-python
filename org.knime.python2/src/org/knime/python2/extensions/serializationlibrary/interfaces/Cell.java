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

package org.knime.python2.extensions.serializationlibrary.interfaces;

/**
 * A cell containing a single value (or array of values).
 *
 * @author Patrick Winter
 */
public interface Cell {

    /**
     * @return {@link Type} of the column containing this cell.
     */
    Type getColumnType();

    /**
     * @return true if the value of this cell is missing, false otherwise.
     */
    boolean isMissing();

    /**
     * Collection cell method.
     * @param index the index inside the underlying collection cell
     * @return true if the value of the underlying list at the given position is missing
     */
    boolean isMissing(int index);

    /**
     * @return the bit encoded missing values in the contained list (0 = missing)
     */
    byte[] getBitEncodedMissingListValues();


    /**
     * @return true if the underlying set contains a missing value, false otherwise
     */
    boolean hasMissingInSet();

    /**
     * @return The boolean value of this cell.
     * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not
     *             {@link Type#BOOLEAN}.
     */
    boolean getBooleanValue() throws IllegalStateException;

    /**
     * @return The boolean array value of this cell.
     * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not
     *             {@link Type#BOOLEAN_LIST} or {@link Type#BOOLEAN_SET}.
     */
    boolean[] getBooleanArrayValue() throws IllegalStateException;

    /**
     * @return The integer value of this cell.
     * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not
     *             {@link Type#INTEGER}.
     */
    int getIntegerValue() throws IllegalStateException;

    /**
     * @return The integer array value of this cell.
     * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not
     *             {@link Type#INTEGER_LIST} or {@link Type#INTEGER_SET}.
     */
    int[] getIntegerArrayValue() throws IllegalStateException;

    /**
     * @return The long value of this cell.
     * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not
     *             {@link Type#LONG}.
     */
    long getLongValue() throws IllegalStateException;

    /**
     * @return The long array value of this cell.
     * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not
     *             {@link Type#LONG_LIST} or {@link Type#LONG_SET}.
     */
    long[] getLongArrayValue() throws IllegalStateException;

    /**
     * @return The double value of this cell.
     * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not
     *             {@link Type#DOUBLE}.
     */
    double getDoubleValue() throws IllegalStateException;

    /**
     * @return The double array value of this cell.
     * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not
     *             {@link Type#DOUBLE_LIST} or {@link Type#DOUBLE_SET}.
     */
    double[] getDoubleArrayValue() throws IllegalStateException;

    /**
     * @return The string value of this cell.
     * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not
     *             {@link Type#STRING}.
     */
    String getStringValue() throws IllegalStateException;

    /**
     * @return The string array value of this cell.
     * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not
     *             {@link Type#STRING_LIST} or {@link Type#STRING_SET}.
     */
    String[] getStringArrayValue() throws IllegalStateException;

    /**
     * @return The bytes value of this cell.
     * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not
     *             {@link Type#BYTES}.
     */
    byte[] getBytesValue() throws IllegalStateException;

    /**
     * @return The bytes array value of this cell.
     * @throws IllegalStateException If {@link #isMissing()} is true or {@link #getColumnType()} is not
     *             {@link Type#BYTES_LIST} or {@link Type#BYTES_SET}.
     */
    byte[][] getBytesArrayValue() throws IllegalStateException;

}
