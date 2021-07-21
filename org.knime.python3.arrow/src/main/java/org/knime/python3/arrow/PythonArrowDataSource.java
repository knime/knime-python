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
 *   Apr 19, 2021 (benjamin): created
 */
package org.knime.python3.arrow;

import java.util.List;

import org.knime.python3.PythonDataSource;

/**
 * A source of Arrow data to a Python process.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public interface PythonArrowDataSource extends PythonDataSource {

    @Override
    default String getIdentifier() {
        return "org.knime.python3.arrow";
    }

    /**
     * @return the absolute path to the file in the Arrow IPC file format
     */
    String getAbsolutePath();

    /**
     * @return true if the footer of the file has been written. In this case accessing the methods
     *         {@link #getRecordBatchOffset(int)} and {@link #getDictionaryBatchOffsets(int)} is forbidden because the
     *         offsets can be read from the footer.
     */
    boolean isFooterWritten();

    /**
     * Get the offset of the record batch at the given index. This method can lock if the record batch is not yet
     * written to the file. When it returns it guarantees that the batch can be read at the retuned offset from the
     * file.
     *
     * @param index the index of the record batch
     * @return the offset of the record batch for the given index
     */
    long getRecordBatchOffset(int index);

    /**
     * Get the offsets of the dictionary batches relating to the record batch at the given index. This method can lock
     * if the dictionary batches are not yet written to the file. When it returns it guarantees that the dictionary
     * batches can be read at the returned offsets from the file.
     *
     * @param index the index of the dictionary batches
     * @return the offsets of all dictionary batches for the given index
     */
    List<Long> getDictionaryBatchOffsets(int index);

    /**
     * @return the total number of batches
     */
    int numBatches();
}
