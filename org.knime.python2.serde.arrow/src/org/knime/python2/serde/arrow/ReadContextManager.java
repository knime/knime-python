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
 *   Nov 14, 2017 (clemens): created
 */
package org.knime.python2.serde.arrow;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.stream.ArrowStreamReader;
import org.knime.core.node.NodeLogger;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;

/**
 * Manages context objects for the arrow serialization library. Context objects are used for efficient resource
 * sharing between the tableSpecFromBytes() and the bytesIntoTable() method. Can be used in a multi-threaded context.
 *
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 */
class ReadContextManager {

    static Map<String, ReadContext> m_contextMap = new HashMap<String, ReadContext>();

    /**
     * Get or create the {@link ReadContext} for the given file. The absolute file path is used as a key.
     * @param file a file containing serialized data
     * @return the corresponding {@link ReadContext}
     * @throws FileNotFoundException If the given file does not exist
     */
    static synchronized ReadContext createForFile(final File file) throws FileNotFoundException {
        String path = file.getAbsolutePath();
        if(!m_contextMap.containsKey(path)) {
            ReadContext rc = new ReadContext(file);
            m_contextMap.put(file.getAbsolutePath(), rc);
            return rc;
        }
        return m_contextMap.get(path);
    }

    /**
     * Destroy a {@link ReadContext} for the given file. The absolute file path is used as a key.
     * @param file a file containing serialized data
     * @return true if a reading context for the file could be found and destroyed, false otherwise
     */
    static synchronized boolean destroy(final File file) {
        ReadContext rc = m_contextMap.remove(file.getAbsolutePath());
        if(rc == null) {
            return false;
        }
        rc.destroy();
        return true;
    }

    static class ReadContext {

        private static NodeLogger LOGGER = NodeLogger.getLogger(ReadContext.class);

        private RandomAccessFile m_raFile;

        private RootAllocator m_rootAllocator;

        private ArrowStreamReader m_streamReader;

        private TableSpec m_spec;

        private int m_numRows;

        private ReadContext(final File file) throws FileNotFoundException {
            m_raFile = new RandomAccessFile(file, "rw");
            m_rootAllocator = new RootAllocator(Long.MAX_VALUE);
            m_streamReader = new ArrowStreamReader(m_raFile.getChannel(), m_rootAllocator);
            m_numRows = 0;
        }

        ArrowStreamReader getReader() {
            return m_streamReader;
        }

        TableSpec getTableSpec() {
            return m_spec;
        }

        void setTableSpec(final TableSpec spec) {
            m_spec = spec;
        }

        /**
         * Close all open resources.
         */
        void destroy() {
            try {
                m_streamReader.close();
            } catch (IOException ex) {
                LOGGER.warn("Could not close stream reader!");
            }
            try {
                m_raFile.close();
            } catch (IOException ex) {
                LOGGER.warn("Could not close arrow file!");
            }
            m_rootAllocator.close();
        }

        /**
         * Sets the number of rows.
         *
         * @param numRows the new number of rows
         */
        void setNumRows(final int numRows) {
            m_numRows = numRows;
        }

        /**
         * Get the number of rows.
         *
         * @return the new number of rows
         */
        int getNumRows() {
            return m_numRows;
        }
    }
}