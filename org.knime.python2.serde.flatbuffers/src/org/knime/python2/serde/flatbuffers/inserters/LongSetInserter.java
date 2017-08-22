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

import org.knime.python2.extensions.serializationlibrary.interfaces.Cell;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;
import org.knime.python2.serde.flatbuffers.flatc.Column;
import org.knime.python2.serde.flatbuffers.flatc.LongCollectionCell;
import org.knime.python2.serde.flatbuffers.flatc.LongCollectionColumn;

import com.google.flatbuffers.FlatBufferBuilder;

/**
 * Manages inserting a long set column into the flatbuffers table.
 *
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 */
public class LongSetInserter extends AbstractCollectionInserter {

    /**
     * Constructor.
     *
     * @param numRows the number of rows in the table
     */
    public LongSetInserter(final int numRows) {
       super(numRows);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int createColumn(final FlatBufferBuilder builder) {
        final int[] cellOffsets = new int[m_column.length];
        final boolean[] missings = new boolean[m_column.length];
        int ctr = 0;
        for (final Cell c : m_column) {

            boolean addMissingValue = false;
            long[] values;
            boolean[] missingCells;
            if (c.isMissing()) {
                values = new long[0];
                missingCells = new boolean[0];
                missings[ctr] = true;
            } else {
                values = c.getLongArrayValue();
                addMissingValue = c.hasMissingInSet();
                missingCells = new boolean[values.length];
            }
            final int valuesOffset = LongCollectionCell.createValueVector(builder, values);
            final int missingCellsOffset = LongCollectionCell.createMissingVector(builder, missingCells);
            cellOffsets[ctr] =
                LongCollectionCell.createLongCollectionCell(builder, valuesOffset, missingCellsOffset, addMissingValue);
            ctr++;
        }

        final int valuesVector = LongCollectionColumn.createValuesVector(builder, cellOffsets);
        final int missingOffset = LongCollectionColumn.createMissingVector(builder, missings);
        final int colOffset = LongCollectionColumn.createLongCollectionColumn(builder, valuesVector, missingOffset);
        Column.startColumn(builder);
        Column.addType(builder, Type.LONG_SET.getId());
        Column.addLongSetColumn(builder, colOffset);
        return Column.endColumn(builder);
    }

}
