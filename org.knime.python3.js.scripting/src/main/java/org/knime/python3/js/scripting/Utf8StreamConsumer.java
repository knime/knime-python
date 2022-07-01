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
 *   Aug 11, 2022 (benjamin): created
 */
package org.knime.python3.js.scripting;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * A {@link Runnable} that reads text from a stream as soon as text is available and gives the text to a
 * {@link Consumer}. Runs until the stream ends or an error occurs.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class Utf8StreamConsumer implements Runnable {

    private static final int BUFFER_SIZE = 1024;

    private final InputStream m_inputStream;

    private final Consumer<String> m_consumer;

    private final Consumer<IOException> m_errorHandler;

    /**
     * Create a new {@link Utf8StreamConsumer}.
     *
     * @param inputStream the stdout or stderr stream of a process that contains UTF-8 encoded data
     * @param consumer a consumer which is called every time a String is read from the stream
     * @param errorHandler a handler that is called once if an IOException is thrown while reading the stream
     */
    public Utf8StreamConsumer(final InputStream inputStream, final Consumer<String> consumer,
        final Consumer<IOException> errorHandler) {
        m_inputStream = inputStream;
        m_consumer = consumer;
        m_errorHandler = errorHandler;
    }

    @Override
    public void run() {
        final var buffer = new byte[BUFFER_SIZE];
        int readBytes = 0;
        int offset = 0;
        try {
            // NB: readBytes is -1 if the Python process ended
            while ((readBytes = m_inputStream.read(buffer, offset, BUFFER_SIZE - offset)) != -1) {
                readBytes += offset;
                var validLength = readBytes;
                if (buffer[readBytes - 1] < 0) {
                    // 0b 1xxx xxxx -> last byte is not a single byte character

                    final int charStartBytePos = findStartCharacterPos(buffer, readBytes);
                    int lastCharLength = findUtf8CharacterLength(buffer[charStartBytePos]);
                    if (lastCharLength == -1) {
                        // Invalid UTF-8: Just give all bytes to the decoder
                        lastCharLength = readBytes - charStartBytePos;
                    }

                    if (charStartBytePos + lastCharLength > readBytes) {
                        // Parts of the character are missing
                        validLength = charStartBytePos;
                    }
                }

                // Send to the consumer if there is at least one complete character
                if (validLength > 0) {
                    m_consumer.accept(new String(buffer, 0, validLength, StandardCharsets.UTF_8));
                }

                // Move the unused bytes to the beginning of the buffer
                offset = readBytes - validLength;
                for (int i = 0; i < offset; i++) {
                    buffer[i] = buffer[validLength + i];
                }
            }
        } catch (final IOException e) {
            // Reading the output stream of the Python process failed
            // We give up (stop looping) and log the error
            m_errorHandler.accept(e);
        }
    }

    private static int findStartCharacterPos(final byte[] buffer, final int readBytes) {
        int charStartBytePos = readBytes - 1;
        while (buffer[charStartBytePos] < -64 && charStartBytePos > 0) {
            // 0b 10xx xxxx
            charStartBytePos--;
        }
        return charStartBytePos;
    }

    private static int findUtf8CharacterLength(final byte b) {
        if (b < -32) {
            return 2;
        } else if (b < -16) {
            return 3;
        } else if (b < -8) {
            return 4;
        }
        // Invalid UTF-8
        return -1;
    }
}
