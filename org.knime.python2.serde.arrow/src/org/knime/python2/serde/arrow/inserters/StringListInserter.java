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
 *   Aug 2, 2017 (clemens): created
 */
package org.knime.python2.serde.arrow.inserters;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.arrow.memory.BufferAllocator;
import org.knime.python2.extensions.serializationlibrary.interfaces.Cell;

/**
 * Manages the data transfer between the python table format and the arrow table format. Works on Strint list cells.
 *
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 */
public class StringListInserter extends ListInserter {

    private String[] m_objs;

    private int[] m_offsets;

    /**
     * Constructor.
     *
     * @param name the name of the managed vector
     * @param allocator an allocator for the underlying buffer
     * @param numRows the number of rows in the managed vector
     * @param bytesPerCellAssumption an initial assumption of the number of bytes per cell
     */
    public StringListInserter(final String name, final BufferAllocator allocator, final int numRows,
        final int bytesPerCellAssumption) {
        super(name, allocator, numRows, bytesPerCellAssumption);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int[] fillInternalArrayAndGetSize(final Cell cell) {
        m_objs = cell.getStringArrayValue();

        m_offsets = new int[m_objs.length + 1];

        for (int j = 0; j < m_objs.length; j++) {
            if (m_objs[j] == null) {
                m_offsets[j + 1] = m_offsets[j];
            } else {
                m_offsets[j + 1] = m_offsets[j] + m_objs[j].length();
            }
        }

        //total length = length of variable size block + length of offset vector
        return new int[]{m_objs.length, m_offsets[m_objs.length] + 4 * m_offsets.length};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void putCollection(final ByteBuffer buffer, final Cell cell) {

        IntBuffer intBuffer = buffer.asIntBuffer();
        //put values
        intBuffer.put(m_offsets);

        buffer.position(4 + m_offsets.length * 4);
        for (String obj : m_objs) {
            if (obj != null) {
                buffer.put(obj.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

}
