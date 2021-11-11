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
 * History
 *   Sep 25, 2014 (Patrick Winter): created
 */
package org.knime.python2.port;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;
import org.knime.core.util.FileUtil;
import org.knime.python2.kernel.PythonKernel;

/**
 * Container for a pickled python object consisting of the object's byte representation, python type and a string
 * representation.
 *
 * @author Patrick Winter, Universität Konstanz, Konstanz, Germany
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
@Deprecated
public class PickledObject {

    private static final String CFG_PICKLED_OBJECT = "pickledObject";

    private static final String CFG_TYPE = "type";

    private static final String CFG_STRING_REPRESENTATION = "stringRepresentation";

    private final byte[] m_pickledObject;

    private final String m_type;

    private final String m_stringRepresentation;

    /**
     * Constructor.
     *
     * @param pickledObject the byte representation of the actual pickled object
     * @param type the type of the pickled object (in python)
     * @param stringRepresentation a representation of the pickled object as a string
     */
    public PickledObject(final byte[] pickledObject, final String type, final String stringRepresentation) {
        m_pickledObject = pickledObject;
        m_type = type;
        m_stringRepresentation = stringRepresentation;
    }

    /**
     * Constructor.
     *
     * @param model the model content to initialize the object from
     * @throws InvalidSettingsException
     * @deprecated since 3.6.1 - use {@link #PickledObject(InputStream)} instead for performance reasons
     */
    @Deprecated
    public PickledObject(final ModelContentRO model) throws InvalidSettingsException {
        m_pickledObject = model.getByteArray(CFG_PICKLED_OBJECT);
        m_type = model.getString(CFG_TYPE);
        m_stringRepresentation = model.getString(CFG_STRING_REPRESENTATION);
    }

    /**
     * Reads a {@link PickledObject} from a given input stream.
     *
     * @param in the stream to read the pickled object from. It is the caller's responsibility to close the stream.
     * @throws IOException if failed to read the pickled object from the given stream
     * @since 3.6.1
     * @see #save(OutputStream)
     */
    public PickledObject(final InputStream in) throws IOException {
        final DataInputStream objIn = new DataInputStream(in);
        final int pickledObjectLength = objIn.readInt();
        m_pickledObject = new byte[pickledObjectLength];
        if (objIn.read(m_pickledObject) < pickledObjectLength) {
            throw new IOException("Failed to read in pickled object.");
        }
        m_type = objIn.readUTF();
        final int stringRepresentationLength = objIn.readInt();
        final byte[] stringRepresentationBytes = new byte[stringRepresentationLength];
        if (objIn.read(stringRepresentationBytes) < stringRepresentationLength) {
            throw new IOException("Failed to read in pickled object.");
        }
        m_stringRepresentation = new String(stringRepresentationBytes, StandardCharsets.UTF_8);
    }

    PickledObjectFile toPickledObjectFile() throws IOException {
        final var tmpFile = FileUtil.createTempFile("pickle", "object");
        return toPickledObjectFile(tmpFile);
    }

    /**
     * Writes the object held by this instance into the provided file and returns a {@link PickledObjectFile} representing it.
     *
     * @param file to write to
     * @return the {@link PickledObjectFile}
     * @throws IOException if writing to the provided file fails
     */
    public PickledObjectFile toPickledObjectFile(final File file) throws IOException {
        try (var outputStream = new FileOutputStream(file)) {
            outputStream.write(m_pickledObject);
        }
        return new PickledObjectFile(file, m_type, m_stringRepresentation);
    }

    /**
     * Creates a {@link PickledObject} from the provided {@link PickledObjectFile}.
     *
     * @param pickledObjectFile to create a {@link PickledObject} from
     * @return the {@link PickledObject}
     * @throws IOException if reading the pickledObjectFile fails
     */
    public static PickledObject fromPickledObjectFile(final PickledObjectFile pickledObjectFile) throws IOException {
        byte[] pickledObject = FileUtils.readFileToByteArray(pickledObjectFile.getFile());
        return new PickledObject(pickledObject, pickledObjectFile.getType(), pickledObjectFile.getRepresentation());
    }

    /**
     * Utility function for deprecated nodes that need to create a PickledObject with the new PythonKernel API.
     *
     * @param name of the object in the workspace
     * @param kernel to use for retrieval
     * @param exec for cancellation and progress reporting
     * @return the {@link PickledObject} with the provided name
     * @throws IOException if no temporary file could be created or reading the pickled object failed
     * @throws CanceledExecutionException if the user cancels the execution
     */
    public static PickledObject getObject(final String name, final PythonKernel kernel, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        final var tmpFile = FileUtil.createTempFile("pickle", "object");
        var pickledObjectFile = kernel.getObject(name, tmpFile, exec);
        return fromPickledObjectFile(pickledObjectFile);
    }

    /**
     * Gets the actual pickled object.
     *
     * @return the actual pickled object
     */
    public byte[] getPickledObject() {
        return m_pickledObject;
    }

    /**
     * Gets the type.
     *
     * @return the type
     */
    public String getType() {
        return m_type;
    }

    /**
     * Gets the string representation.
     *
     * @return the string representation
     */
    public String getStringRepresentation() {
        return m_stringRepresentation;
    }

    /**
     * Checks if the pickled object is None in python.
     *
     * @return true, if is None
     */
    public boolean isNone() {
        return m_type.equals("NoneType");
    }

    /**
     * Save the pickled object as model content.
     *
     * @param model the content to save to
     * @deprecated since 3.6.1 - use {@link #save(OutputStream)} instead for performance reasons
     */
    @Deprecated
    public void save(final ModelContentWO model) {
        model.addByteArray(CFG_PICKLED_OBJECT, m_pickledObject);
        model.addString(CFG_TYPE, m_type);
        model.addString(CFG_STRING_REPRESENTATION, m_stringRepresentation);
    }

    /**
     * Save the pickled object to an output stream.
     *
     * @param out the stream to write the pickled object to. It is the caller's responsibility to close the stream.
     * @throws IOException if writing the pickled object to the given stream failed
     * @since 3.6.1
     * @see #PickledObject(InputStream)
     */
    public void save(final OutputStream out) throws IOException {
        final DataOutputStream objOut = new DataOutputStream(out);
        objOut.writeInt(m_pickledObject.length);
        objOut.write(m_pickledObject);
        objOut.writeUTF(m_type);
        // String representation can get pretty long. This may not be supported by DataOutputStream#writeUTF(String).
        final byte[] stringRepresentationBytes = m_stringRepresentation.getBytes(StandardCharsets.UTF_8);
        objOut.writeInt(stringRepresentationBytes.length);
        objOut.write(stringRepresentationBytes);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PickledObject)) {
            return false;
        }
        final PickledObject con = (PickledObject)obj;
        final EqualsBuilder eb = new EqualsBuilder();
        eb.append(m_pickledObject, con.m_pickledObject);
        eb.append(m_type, con.m_type);
        eb.append(m_stringRepresentation, con.m_stringRepresentation);
        return eb.isEquals();
    }

    @Override
    public int hashCode() {
        final HashCodeBuilder hcb = new HashCodeBuilder();
        hcb.append(m_pickledObject);
        hcb.append(m_type);
        hcb.append(m_stringRepresentation);
        return hcb.hashCode();
    }

    @Override
    public String toString() {
        return m_type + "\n" + m_stringRepresentation;
    }
}
