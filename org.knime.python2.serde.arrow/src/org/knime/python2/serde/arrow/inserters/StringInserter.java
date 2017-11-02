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

package org.knime.python2.serde.arrow.inserters;

import java.nio.charset.StandardCharsets;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.knime.python2.extensions.serializationlibrary.interfaces.Cell;

/**
 * Manages the data transfer between the python table format and the arrow table format. Works on String cells.
 *
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 */
public class StringInserter implements ArrowVectorInserter {

    private final NullableVarCharVector m_vec;

    private final NullableVarCharVector.Mutator m_mutator;

    private int m_ctr;

    private int m_byteCount;

    /**
     * Constructor.
     *
     * @param name the name of the managed vector
     * @param allocator an allocator for the underlying buffer
     * @param numRows the number of rows in the managed vector
     * @param bytesPerCellAssumption an initial assumption of the number of bytes per cell
     */
    public StringInserter(final String name, final BufferAllocator allocator, final int numRows,
        final int bytesPerCellAssumption) {

        m_vec = new NullableVarCharVector(name, allocator);
        m_vec.allocateNew(bytesPerCellAssumption * numRows, numRows);
        m_mutator = m_vec.getMutator();
    }

    @Override
    public void put(final Cell cell) {
        if (m_ctr >= m_vec.getValueCapacity()) {
            m_vec.getValuesVector().getOffsetVector().reAlloc();
            m_vec.getValidityVector().reAlloc();
        }
        if (!cell.isMissing()) {
            //Implicitly assumed to be missing
            byte[] bVal = cell.getStringValue().getBytes(StandardCharsets.UTF_8);
            m_byteCount += bVal.length;
            while (m_byteCount > m_vec.getByteCapacity()) {
                //TODO realloc only content vector (not offset vector), if possible with factor 2^x
                m_vec.getValuesVector().reAlloc();
            }
            m_mutator.set(m_ctr, bVal);
        }
        m_mutator.setValueCount(++m_ctr);
    }

    @Override
    public FieldVector retrieveVector() {
        return m_vec;
    }

}
