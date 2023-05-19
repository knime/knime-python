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
 * ------------------------------------------------------------------------
 *
 */
package org.knime.python3.nodes.ports;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.zip.ZipEntry;

import javax.swing.JComponent;

import org.knime.core.data.filestore.FileStore;
import org.knime.core.data.filestore.FileStorePortObject;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectZipInputStream;
import org.knime.core.node.port.PortObjectZipOutputStream;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.PortTypeRegistry;

/**
 * FileStore-based port object type for Python nodes. The data is never read on the Java side.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 * @since 4.6
 */
public class PythonBinaryBlobFileStorePortObject extends FileStorePortObject {

    /**
     * The type of this port.
     */
    @SuppressWarnings("hiding")
    public static final PortType TYPE =
        PortTypeRegistry.getInstance().getPortType(PythonBinaryBlobFileStorePortObject.class);

    /**
     * The corresponding spec of this port object
     */
    protected final PythonBinaryBlobPortObjectSpec m_spec;

    /**
     * Deserialization constructor
     * @param spec The {@link PythonBinaryBlobPortObjectSpec} of this port object
     */
    protected PythonBinaryBlobFileStorePortObject(final PythonBinaryBlobPortObjectSpec spec) {
        m_spec = spec;
    }

    /**
     * Construction with data inside a FileStore and spec
     *
     * @param fileStore the {@link FileStore} holding the data
     * @param spec of the port object
     */
    protected PythonBinaryBlobFileStorePortObject(final FileStore fileStore,
        final PythonBinaryBlobPortObjectSpec spec) {
        super(Arrays.asList(fileStore));
        m_spec = spec;
    }

    /**
     * Construction with data inside a FileStore and spec
     *
     * @param fileStore the {@link FileStore} holding the data
     * @param spec of the port object
     * @return Newly created {@link PythonBinaryBlobFileStorePortObject}
     * @throws IOException
     */
    public static PythonBinaryBlobFileStorePortObject create(final FileStore fileStore,
        final PythonBinaryBlobPortObjectSpec spec) throws IOException {
        if (fileStore == null) {
            throw new IOException("FileStore cannot be null for PythonBinaryBlobFileStorePortObject");
        }
        return new PythonBinaryBlobFileStorePortObject(fileStore, spec);
    }

    /**
     * @return The path to the file on disk that contains the binary data, needed to access the file in Python
     */
    public String getFilePath() {
        return getFileStore(0).getFile().getAbsolutePath();
    }

    @Override
    public String getSummary() {
        return shortenString(m_spec.getId(), 60, "...");
    }

    @Override
    public PythonBinaryBlobPortObjectSpec getSpec() {
        return m_spec;
    }

    @Override
    public JComponent[] getViews() {
        return m_spec.getViews();
    }

    private static String shortenString(final String string, final int maxLength, final String suffix) {
        return string.length() > maxLength ? (string.substring(0, maxLength - suffix.length()) + suffix) : string;
    }

    @Override
    public int hashCode() {
        return Objects.hash(m_spec);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        final PythonBinaryBlobFileStorePortObject other = (PythonBinaryBlobFileStorePortObject)obj;
        return Objects.equals(m_spec, other.m_spec);
    }

    /**
     * Serializer of {@link PythonBinaryBlobFileStorePortObject}.
     */
    public static final class Serializer extends PortObjectSerializer<PythonBinaryBlobFileStorePortObject> {

        private static final String ZIP_ENTRY_NAME = "PythonBinaryBlobFileStorePortObject";

        private static final int CURRENT_VERSION = 1;

        @Override
        public void savePortObject(final PythonBinaryBlobFileStorePortObject portObject,
            final PortObjectZipOutputStream out, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
            out.putNextEntry(new ZipEntry(ZIP_ENTRY_NAME));
            var dataOut = new DataOutputStream(out);
            // Save "version" for forward compatibility reasons.
            dataOut.writeInt(CURRENT_VERSION);
            dataOut.flush();
        }

        @Override
        public PythonBinaryBlobFileStorePortObject loadPortObject(final PortObjectZipInputStream in,
            final PortObjectSpec spec, final ExecutionMonitor exec) throws IOException, CanceledExecutionException {
            final ZipEntry entry = in.getNextEntry();
            if (!ZIP_ENTRY_NAME.equals(entry.getName())) {
                throw new IOException("Failed to load pickled object port object. Invalid zip entry name '"
                    + entry.getName() + "', expected '" + ZIP_ENTRY_NAME + "'.");
            }
            var dataIn = new DataInputStream(in);
            int version = dataIn.readInt(); // NOSONAR
            if (version == 1) {
                return new PythonBinaryBlobFileStorePortObject((PythonBinaryBlobPortObjectSpec)spec);
            } else {
                throw new IllegalStateException("Unsupported version: " + version);
            }
        }
    }
}
