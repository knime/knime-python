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
 *   May 10, 2018 (marcel): created
 */
package org.knime.python2.kernel.messaging;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
final class PythonMessagingUtils {

    private PythonMessagingUtils() {
        // utility class
    }

    /**
     * Reads the given number of bytes from the given input stream.
     *
     * @param numBytes the number of bytes to read
     * @param inputStream the stream to read from
     * @return the read bytes of length <code>numBytes</code>
     * @throws IOException if reading failed for I/O reasons
     */
    public static byte[] readBytes(final int numBytes, final DataInputStream inputStream) throws IOException {
        final byte[] bytes = new byte[numBytes];
        inputStream.readFully(bytes);
        return bytes;
    }

    /**
     * Reads an integer value from the given input stream.
     *
     * @param inputStream the stream to read from
     * @return the read integer
     * @throws IOException if reading failed for I/O reasons
     */
    public static int readInt(final DataInputStream inputStream) throws IOException {
        final byte[] bytes = new byte[4];
        inputStream.readFully(bytes);
        return ByteBuffer.wrap(bytes).getInt();
    }

    /**
     * Reads a string of the given number of bytes from the given input stream.
     *
     * @param numBytes the number of bytes that make up the string to read
     * @param inputStream the stream to read from
     * @return the read string
     * @throws IOException if reading failed for I/O reasons
     */
    public static String readUtf8String(final int numBytes, final DataInputStream inputStream) throws IOException {
        return utf8StringFromBytes(readBytes(numBytes, inputStream));
    }

    /**
     * Decodes the given byte array to a UTF-8 string and returns the string.
     *
     * @param bytes the byte array
     * @return the encoded string
     */
    public static String utf8StringFromBytes(final byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static byte[] utf8StringToBytes(final String string) {
        return string.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Writes the given integer value into the given output stream.
     *
     * @param value the integer
     * @param outputStream the stream to write to
     * @throws IOException if writing failed for I/O reasons
     */
    public static void writeInt(final int value, final DataOutputStream outputStream) throws IOException {
        outputStream.write(ByteBuffer.allocate(4).putInt(value).array());
        outputStream.flush();
    }
}
