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

import org.knime.core.table.schema.ColumnarSchema;
import org.knime.python3.PythonDataSink;

/**
 * A sink for Arrow data from a Python process.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public interface PythonArrowDataSink extends PythonDataSink {

    @Override
    default String getIdentifier() {
        return "org.knime.python3.arrow";
    }

    /**
     * @return the path the output file should be written to.
     */
    String getAbsolutePath();

    /**
     * Report that the next batch has been written to the file. Must be called by Python each time a new batch was
     * written. Must be called for each batch in ascending order.
     *
     * @param offset the offset of the batch
     * @throws Exception
     */
    void reportBatchWritten(long offset) throws Exception; // TODO(dictionary) add offsets for dictionary batches

    /**
     * TODO check if this can be removed. We can now also read the schema from the arrow file directly
     *
     * @param schema the schema of the data that is written to the file
     */
    void setColumnarSchema(ColumnarSchema schema);

    /**
     * @param size the final size of the table
     */
    void setFinalSize(final long size);
}
