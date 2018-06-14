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
 *   Sep 27, 2017 (clemens): created
 */
package org.knime.python2.kernel.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Default implementation of {@link Message}.
 *
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class DefaultMessage implements Message {

    /**
     * The key for the message's <code>id</code> header field.
     */
    public static final String KEY_ID = "id";

    /**
     * The key for the message's <code>category</code> header field.
     */
    public static final String KEY_CATEGORY = "category";

    private final LinkedHashMap<String, String> m_headerFields;

    /**
     * Also part of header fields. Cached to speed up access.
     */
    private final int m_id;

    /**
     * Also part of header fields. Cached to speed up access.
     */
    private final String m_category;

    private final byte[] m_payload;

    /**
     * @param id the message's identifier, must be unique within the same kernel session
     * @param category the message's category which is used to forward the message to an appropriate
     *            {@link MessageHandler handler}, must not contain the characters '@' or '='. These conditions are not
     *            checked and must be ensured by the caller.
     * @param payload the message's payload, may be <code>null</code>, is not copied
     * @param additionalOptions custom fields to include in the message's {@link #getHeaderField(String) header fields},
     *            may be <code>null</code>. If non-<code>null</code>, must not contain strings (neither keys nor values)
     *            which contain the characters '@' or '='. Must not contain fields with keys {@link #KEY_ID} or
     *            {@link #KEY_CATEGORY}}. These conditions are not checked and must be ensured by the caller.
     */
    public DefaultMessage(final int id, final String category, final byte[] payload,
        final Map<String, String> additionalOptions) {
        m_id = id;
        m_category = checkNotNull(category);
        m_payload = payload;

        m_headerFields = new LinkedHashMap<>(2 + (additionalOptions != null ? additionalOptions.size() : 0));
        m_headerFields.put(KEY_ID, Integer.toString(id));
        m_headerFields.put(KEY_CATEGORY, category);
        if (additionalOptions != null) {
            m_headerFields.putAll(additionalOptions);
        }
    }

    /**
     * @param header the message header that consists of fields in the form @&ltkey&gt=&ltvalue&gt . At least the
     *            {@link #KEY_ID id} and {@link #KEY_CATEGORY category} fields need to be given.
     * @param payload the message's payload as a byte array, may be <code>null</code>. {@link PayloadEncoder} can be
     *            used to encode the payload.
     */
    public DefaultMessage(final String header, final byte[] payload) {
        m_payload = payload;

        m_headerFields = new LinkedHashMap<>();
        final String[] fields = header.split("@");
        for (final String field : fields) {
            if (field.isEmpty()) {
                continue;
            }
            final int indexOfDelimiter = field.indexOf('=');
            final String key = field.substring(0, indexOfDelimiter);
            final String value = field.substring(indexOfDelimiter + 1);
            m_headerFields.put(key, value);
        }

        final String id = m_headerFields.get(KEY_ID);
        if (id == null) {
            throw new IllegalArgumentException("No id in message header '" + header + "'.");
        }
        m_id = Integer.parseInt(id);

        final String category = m_headerFields.get(KEY_CATEGORY);
        if (category == null) {
            throw new IllegalArgumentException("No category in message header '" + header + "'.");
        }
        m_category = category;
    }

    @Override
    public int getId() {
        return m_id;
    }

    @Override
    public String getCategory() {
        return m_category;
    }

    @Override
    public String getHeader() {
        String header = "";
        for (final Entry<String, String> entry : m_headerFields.entrySet()) {
            header += "@" + entry.getKey() + "=" + entry.getValue();
        }
        return header;
    }

    @Override
    public String getHeaderField(final String fieldKey) {
        return m_headerFields.get(fieldKey);
    }

    @Override
    public byte[] getPayload() {
        return m_payload;
    }

    @Override
    public String toString() {
        return getHeader();
    }

    /**
     * Utility class for decoding a {@link Message#getPayload() message's payload}.
     */
    public static final class PayloadDecoder {

        private final ByteBuffer m_buffer;

        /**
         * @param bytes the payload byte array
         */
        public PayloadDecoder(final byte[] bytes) {
            m_buffer = ByteBuffer.wrap(bytes);
        }

        /**
         * @return the next byte array
         */
        public byte[] getNextBytes() {
            final int len = m_buffer.getInt();
            final byte[] value = new byte[len];
            m_buffer.get(value);
            return value;
        }

        /**
         * @return the next decoded integer
         */
        public int getNextInt() {
            return m_buffer.getInt();
        }

        /**
         * @return the next decoded long
         */
        public long getNextLong() {
            return m_buffer.getLong();
        }

        /**
         * @return the next decoded string
         */
        public String getNextString() {
            final int len = m_buffer.getInt();
            final byte[] bytes = new byte[len];
            m_buffer.get(bytes);
            return PythonMessagingUtils.utf8StringFromBytes(bytes);
        }
    }

    /**
     * Utility class for encoding a {@link Message#getPayload() message's payload}.
     * <P>
     * Encoding schema:
     * <ul>
     * <li>variable size types: (length: int32)(object)</li>
     * <li>fixed size types: (object)</li>
     * </ul>
     */
    public static final class PayloadEncoder {

        private ByteBuffer m_buffer;

        private int m_position;

        /**
         * Allocate initial buffer of 1024 byte.
         */
        public PayloadEncoder() {
            m_buffer = ByteBuffer.allocate(1024);
            m_position = 0;
        }

        /**
         * Get the encoded payload.
         *
         * @return the encoded payload.
         */
        public byte[] get() {
            final byte[] payload = new byte[m_position];
            m_buffer.position(0);
            m_buffer.get(payload);
            return payload;
        }

        /**
         * Write a byte array to the payload buffer.
         *
         * @param value a byte array
         * @return this instance
         */
        public PayloadEncoder putBytes(final byte[] value) {
            makeSpace(value.length + 4);
            m_buffer.putInt(m_position, value.length);
            m_position += 4;
            m_buffer.position(m_position);
            m_buffer.put(value);
            m_position += value.length;
            return this;
        }

        /**
         * Encode and write an integer to the payload buffer.
         *
         * @param value the integer
         * @return this instance
         */
        public PayloadEncoder putInt(final int value) {
            makeSpace(4);
            m_buffer.putInt(m_position, value);
            m_position += 4;
            return this;
        }

        /**
         * Encode and write a long to the payload buffer.
         *
         * @param value the long
         * @return this instance
         */
        public PayloadEncoder putLong(final long value) {
            makeSpace(8);
            m_buffer.putLong(m_position, value);
            m_position += 8;
            return this;
        }

        /**
         * Encode and write a string to the payload buffer.
         *
         * @param value the string
         * @return this instance
         */
        public PayloadEncoder putString(final String value) {
            final byte[] bytes = PythonMessagingUtils.utf8StringToBytes(value);
            putBytes(bytes);
            return this;
        }

        /**
         * If the buffer has less capacity then needed to accommodate <code>requiredSpace</code> bytes, double the
         * buffer's capacity.
         *
         * @param requiredSpace the size of the next entry to write
         */
        private void makeSpace(final int requiredSpace) {
            while (m_buffer.capacity() - m_position < requiredSpace) {
                final ByteBuffer tmp = m_buffer;
                m_buffer = ByteBuffer.allocate(tmp.capacity() * 2);
                tmp.position(0);
                m_buffer.put(tmp);
                m_buffer.position(m_position);
            }
        }
    }
}
