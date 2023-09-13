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
 *   Sep 14, 2023 (benjamin): created
 */
package org.knime.python3;

import java.io.IOException;
import java.util.UUID;

import org.knime.core.data.filestore.FileStore;
import org.knime.core.data.filestore.FileStoreKey;
import org.knime.core.data.filestore.internal.IFileStoreHandler;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.node.NodeLogger;

/**
 * Utilities to handle file store access in Python nodes and Python script nodes.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class PythonFileStoreUtils {

    private PythonFileStoreUtils() {
        // Static utility class
    }

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonFileStoreUtils.class);

    /**
     * Create a new file store with the given file store handler and a random UUID as key. If the handler is a
     * {@link NotInWorkflowWriteFileStoreHandler}, the file store will not have look information.
     *
     * @param fsHandler the file store handler to use
     * @return a new file store
     * @throws IOException if there was an I/O Error creating the file store with the handler
     */
    public static FileStore createFileStore(final IWriteFileStoreHandler fsHandler) throws IOException {
        final var uuid = UUID.randomUUID().toString();
        if (fsHandler instanceof NotInWorkflowWriteFileStoreHandler) {
            // If we have a NotInWorkflowWriteFileStoreHandler then we are only creating a temporary copy of the
            // table (e.g. for the Python Script Dialog) and don't need nested loop information anyways.
            return fsHandler.createFileStore(uuid, null, -1);
        } else {
            try {
                return fsHandler.createFileStore(uuid);
            } catch (IllegalStateException ex) {
                LOGGER.debug("FileStore seemed to be readonly, creating FileStore without loop information. "
                    + "This can happen in the Python Script dialog if the node was saved "
                    + "and the dialog is opened afterwards.", ex);
                return fsHandler.createFileStore(uuid, null, -1);
            }
        }
    }

    /**
     * Find the file store for the given key and return the absolute path to the file store as a String.
     *
     * @param fsHandler the file store handler containing the file store
     * @param fileStoreKey the key
     * @return the absolute path to the file store as a String
     */
    public static String getAbsolutePathForKey(final IFileStoreHandler fsHandler, final FileStoreKey fileStoreKey) {
        return fsHandler.getFileStore(fileStoreKey).getFile().getAbsolutePath();
    }
}
