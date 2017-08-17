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
 *   Aug 17, 2017 (clemens): created
 */
package org.knime.python2.serde.flatbuffers.inserters;

import java.nio.DoubleBuffer;

import org.knime.python2.extensions.serializationlibrary.interfaces.Cell;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;
import org.knime.python2.serde.flatbuffers.flatc.Column;
import org.knime.python2.serde.flatbuffers.flatc.DoubleCollectionCell;
import org.knime.python2.serde.flatbuffers.flatc.DoubleCollectionColumn;

import com.google.flatbuffers.FlatBufferBuilder;

/**
 * Manages inserting a long set column into the flatbuffers table.
 *
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 */
public class DoubleSetInserter implements FlatbuffersVectorInserter {

    private Double[][] m_values;

    private boolean[] m_missings;

    private int m_ctr;

    /**
     * Constructor.
     *
     * @param numRows the number of rows in the table
     */
    public DoubleSetInserter(final int numRows) {
        m_values = new Double[numRows][];
        m_missings = new boolean[numRows];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(final Cell cell) {
        if (cell.isMissing()) {
            m_missings[m_ctr] = true;
        } else {
            m_values[m_ctr] = cell.getDoubleArrayValue();
        }
        m_ctr++;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int createColumn(final FlatBufferBuilder builder) {
        final int[] cellOffsets = new int[m_values.length];
        int ctr = 0;
        for (final Double[] o : m_values) {

            boolean addMissingValue = false;
            double[] values;
            boolean[] missingCells;
            if (o == null) {
                values = new double[0];
                missingCells = new boolean[0];
            } else {
                DoubleBuffer ibuff = DoubleBuffer.allocate(o.length);
                for (final Double c : o) {
                    if (c == null) {
                        addMissingValue = true;
                    } else {
                        ibuff.put(c.doubleValue());
                    }
                }

                if (addMissingValue) {
                    values = new double[o.length - 1];
                    ibuff.position(0);
                    ibuff.get(values);
                } else {
                    values = ibuff.array();
                }
                missingCells = new boolean[o.length];
            }
            final int valuesOffset = DoubleCollectionCell.createValueVector(builder, values);
            final int missingCellsOffset = DoubleCollectionCell.createMissingVector(builder, missingCells);
            cellOffsets[ctr] = DoubleCollectionCell.createDoubleCollectionCell(builder, valuesOffset,
                missingCellsOffset, addMissingValue);
            ctr++;
        }

        final int valuesVector = DoubleCollectionColumn.createValuesVector(builder, cellOffsets);
        final int missingOffset = DoubleCollectionColumn.createMissingVector(builder, m_missings);
        final int colOffset = DoubleCollectionColumn.createDoubleCollectionColumn(builder, valuesVector, missingOffset);
        Column.startColumn(builder);
        Column.addType(builder, Type.DOUBLE_SET.getId());
        Column.addDoubleSetColumn(builder, colOffset);
        return Column.endColumn(builder);
    }

}
