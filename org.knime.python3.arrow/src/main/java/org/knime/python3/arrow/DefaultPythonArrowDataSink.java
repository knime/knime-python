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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * A simple default implementation of the {@link PythonArrowDataSink}. Use {@link PythonArrowDataUtils} to create an
 * instance and to convert it to a {@link BatchReadStore}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class DefaultPythonArrowDataSink implements PythonArrowDataSink {

    private final Path m_path;

    private final List<Long> m_recordBatchOffsets;

    private ColumnarSchema m_schema;

    DefaultPythonArrowDataSink(final Path path) {
        m_path = path;
        m_recordBatchOffsets = new ArrayList<>();
    }

    @Override
    public String getAbsolutePath() {
        return m_path.toAbsolutePath().toString();
    }

    @Override
    public void reportBatchWritten(final long offset) {
        m_recordBatchOffsets.add(offset);
    }

    @Override
    public void setColumnarSchema(final ColumnarSchema schema) {
        m_schema = schema;
    }

    List<Long> getRecordBatchOffsets() {
        return m_recordBatchOffsets;
    }

    ColumnarSchema getSchema() {
        if (m_schema == null) {
            throw new IllegalStateException(
                "Cannot get the schema before it has been set. This is an implementation error.");
        }
        return m_schema;
    }

    Path getPath() {
        return m_path;
    }
}
