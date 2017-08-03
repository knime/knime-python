/*
 * ------------------------------------------------------------------------
 *
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
 * ---------------------------------------------------------------------
 *
 * History
 *   Aug 2, 2017 (clemens): created
 */
package org.knime.python2.serde.arrow.extractors;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.arrow.vector.NullableVarBinaryVector;
import org.knime.python2.extensions.serializationlibrary.interfaces.Cell;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.CellImpl;

/**
 * Base class for List types that are transferred between the arrow table format and the python table format.
 *
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 */
public abstract class ListExtractor implements VectorExtractor {

    private final NullableVarBinaryVector.Accessor m_accessor;

    private int m_ctr;

    /**
     * Constructor.
     *
     * @param vector the vector to extract from
     */
    protected ListExtractor(final NullableVarBinaryVector vector) {
        m_accessor = vector.getAccessor();
    }

    /**
     * Extract the list from the buffer (type specific).
     *
     * @param buffer the buffer containing the list
     * @param numVals the number of values to read
     * @return a reference to the extracted list
     */
    protected abstract Object[] fillInternalArray(final ByteBuffer buffer, int numVals);

    /**
     * Wrap the extracted list into a {@link Cell} representation.
     *
     * @return the {@link Cell} representation
     */
    protected abstract Cell getReturnValue();

    /**
     * Returns the length of the section in the buffer occupied by values
     *
     * @return the length of the section in the buffer occupied by values
     */
    protected abstract int getValuesLength();

    @Override
    public Cell extract() {
        if (m_accessor.isNull(m_ctr)) {
            m_ctr++;
            return new CellImpl();
        }

        ByteBuffer buffer = ByteBuffer.wrap(m_accessor.getObject(m_ctr)).order(ByteOrder.LITTLE_ENDIAN);
        int nVals = buffer.asIntBuffer().get();
        buffer.position(4);
        Object[] obj = fillInternalArray(buffer, nVals);
        buffer.position(getValuesLength() + 4);
        byte b = 0;
        for (int idx = 0; idx < nVals; idx++) {
            if (idx % 8 == 0) {
                b = buffer.get();
            }
            if ((b & (1 << (idx % 8))) == 0) {
                obj[idx] = null;
            }
        }
        m_ctr++;
        return getReturnValue();
    }

}
