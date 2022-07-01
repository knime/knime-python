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

import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import org.junit.Test;

/**
 * Tests for the {@link Utf8StreamConsumer}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings({"javadoc", "static-method"})
public final class Utf8StreamConsumerTest {

    private static void rethrowUnchecked(final Exception e) {
        throw new IllegalStateException(e);
    }

    private static void testStreamConsumer(final InputStream stream, final String... expected) {
        final var consumed = new ArrayList<String>();
        new Utf8StreamConsumer(stream, consumed::add, Utf8StreamConsumerTest::rethrowUnchecked).run();
        assertArrayEquals(expected, consumed.toArray(String[]::new));
    }

    @Test
    @SuppressWarnings("resource")
    public void testSimpleStrings() {
        final var strings = new String[]{"foo", "bar"};
        testStreamConsumer(new TestStream(strings), strings);
    }

    @Test
    @SuppressWarnings("resource")
    public void testByteUtf8Chars() {
        final var strings = new String[]{ //
            "ג", // single 2 bytes
            "㒆", // single 3 bytes
            "𐍈", // single 4 bytes
            "foo£", // at the end 2 bytes
            "foo€", // at the end 3 bytes
            "foo𐍈", // at the end 4 bytes
            "شbar", // at the beginning 2 bytes
            "€bar", // at the beginning 3 bytes
            "𐍈bar", // at the beginning 4 bytes
            "abߖ‎d", // in the center 2 bytes
            "ab€d", // in the center 3 bytes
            "ab𐍈d" // in the center 4 bytes
        };
        testStreamConsumer(new TestStream(strings), strings);
    }

    @Test
    @SuppressWarnings("resource")
    public void testSplitUtf8Chars() {
        final var data0 = new byte[]{102, 111, 111, -62}; // "foo" + first byte of £
        final var data1 = new byte[]{-93, 98, 97, 114}; // second byte of £ + "bar"
        final var data2 = new byte[]{-30, -126}; // first two bytes of €
        final var data3 = new byte[]{-84, 102, 111, 111}; // last byte of € + "foo"
        final var data4 = new byte[]{97, 98, -16}; // "ab" + first byte of 𐍈
        final var data5 = new byte[]{-112, -115}; // second and third byte of 𐍈
        final var data6 = new byte[]{-120, 99, 100}; // last byte of 𐍈 and "cd"
        testStreamConsumer(new TestStream(data0, data1, data2, data3, data4, data5, data6), //
            "foo", "£bar", "€foo", "ab", "𐍈cd");
    }

    @Test
    @SuppressWarnings("resource")
    public void testInvalidUtf8() {
        final var data0 = new byte[]{98, 97, 114}; // "bar"
        final var data1 = new byte[]{102, -93, -102, -120}; // "f" + invalid
        final var data2 = new byte[]{-93, -120}; // invalid
        final var data3 = new byte[]{102, 111, 111}; // "foo"
        testStreamConsumer(new TestStream(data0, data1, data2, data3), "bar", "f���", "��", "foo");
    }

    private static final class TestStream extends InputStream {

        private static byte[][] encode(final String... strings) {
            final var data = new byte[strings.length][];
            for (int i = 0; i < strings.length; i++) {
                data[i] = strings[i].getBytes(StandardCharsets.UTF_8);
            }
            return data;
        }

        private final byte[][] m_data;

        private int m_nextIndex;

        private TestStream(final String... strings) {
            this(encode(strings));
        }

        private TestStream(final byte[]... data) {
            m_data = data;
            m_nextIndex = 0;
        }

        @Override
        public int read(final byte[] b, final int off, final int len) throws IOException {
            // Nothing left
            if (m_nextIndex >= m_data.length) {
                return -1;
            }

            final byte[] data = m_data[m_nextIndex++];
            if (len < data.length) {
                throw new IOException("Buffer too small");
            }
            System.arraycopy(data, 0, b, off, data.length);
            return data.length;
        }

        @Override
        public int read() throws IOException {
            throw new IOException("Reading a single byte is not supported by the test stream");
        }
    }
}
