/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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
 */

package org.knime.python2.kernel;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.knime.core.node.NodeLogger;

public class Commands {
	
	private final OutputStream m_outToServer;
	private final InputStream m_inFromServer;
	
	private final DataInputStream m_bufferedInFromServer;
	private final DataOutputStream m_bufferedOutToServer;
	
	public Commands(final OutputStream outToServer, final InputStream inFromServer) {
		m_outToServer = outToServer;
		m_inFromServer = inFromServer;
		m_bufferedInFromServer = new DataInputStream(m_inFromServer);
		m_bufferedOutToServer = new DataOutputStream(m_outToServer);
	}
	
	synchronized public int getPid() throws IOException {
		return readInt();
	}
	
	synchronized public String[] execute(final String sourceCode) throws IOException {
		writeString("execute");
		writeString(sourceCode);
		String[] output = new String[2];
		output[0] = readString();
		output[1] = readString();
		return output;
	}
	
	synchronized public void putFlowVariables(final String name, final byte[] variables) throws IOException {
		writeString("putFlowVariables");
		writeString(name);
		writeBytes(variables);
		readBytes();
	}
	
	synchronized public byte[] getFlowVariables(final String name) throws IOException {
		writeString("getFlowVariables");
		writeString(name);
		return readBytes();
	}
	
	synchronized public void putTable(final String name, final byte[] table) throws IOException {
		writeString("putTable");
		writeString(name);
		writeBytes(table);
		readBytes();
	}
	
	synchronized public void appendToTable(final String name, final byte[] table) throws IOException {
		writeString("appendToTable");
		writeString(name);
		writeBytes(table);
		readBytes();
	}
	
	synchronized public int getTableSize(final String name) throws IOException {
		writeString("getTableSize");
		writeString(name);
		return readInt();
	}
	
	synchronized public byte[] getTable(final String name) throws IOException {
		writeString("getTable");
		writeString(name);
		return readBytes();
	}
	
	synchronized public byte[] getTableChunk(final String name, final int start, final int end) throws IOException {
		writeString("getTableChunk");
		writeString(name);
		writeInt(start);
		writeInt(end);
		return readBytes();
	}
	
	synchronized public byte[] listVariables() throws IOException {
		writeString("listVariables");
		return readBytes();
	}
	
	synchronized public void reset() throws IOException {
		writeString("reset");
		readBytes();
	}
	
	synchronized public boolean hasAutoComplete() throws IOException {
		writeString("hasAutoComplete");
		return readInt() > 0;
	}
	
	synchronized public byte[] autoComplete(final String sourceCode, final int line, final int column) throws IOException {
		writeString("autoComplete");
		writeString(sourceCode);
		writeInt(line);
		writeInt(column);
		return readBytes();
	}
	
	synchronized public byte[] getImage(final String name) throws IOException {
		writeString("getImage");
		writeString(name);
		return readBytes();
	}
	
	synchronized public byte[] getObject(final String name) throws IOException {
		writeString("getObject");
		writeString(name);
		return readBytes();
	}
	
	synchronized public void putObject(final String name, final byte[] object) throws IOException {
		writeString("putObject");
		writeString(name);
		writeBytes(object);
		readBytes();
	}
	
	synchronized public void addSerializer(final String id, final String type, final String path) throws IOException {
		writeString("addSerializer");
		writeString(id);
		writeString(type);
		writeString(path);
		readBytes();
	}
	
	synchronized public void addDeserializer(final String id, final String path) throws IOException {
		writeString("addDeserializer");
		writeString(id);
		writeString(path);
		readBytes();
	}
	
	synchronized public void shutdown() throws IOException {
		writeString("shutdown");
	}
	
	synchronized public void putSql(final String name, final byte[] sql) throws IOException {
		writeString("putSql");
		writeString(name);
		writeBytes(sql);
		readBytes();
	}
	
	synchronized public String getSql(final String name) throws IOException {
		writeString("getSql");
		writeString(name);
		return readString();
	}
	
	private byte[] stringToBytes(final String string) {
		return string.getBytes();
	}
	
	private String stringFromBytes(final byte[] bytes) {
		return new String(bytes);
	}
	
	private byte[] intToBytes(final int integer) {
		return ByteBuffer.allocate(4).putInt(integer).array();
	}
	
	private int intFromBytes(final byte[] bytes) {
		return ByteBuffer.wrap(bytes).getInt();
	}
	
	private void writeString(final String string) throws IOException {
		writeMessageBytes(stringToBytes(string), m_bufferedOutToServer);
	}
	
	private String readString() throws IOException {
		return stringFromBytes(readMessageBytes(m_bufferedInFromServer));
	}
	
	private void writeInt(final int integer) throws IOException {
		writeMessageBytes(intToBytes(integer), m_bufferedOutToServer);
	}
	
	private int readInt() throws IOException {
		return intFromBytes(readMessageBytes(m_bufferedInFromServer));
	}
	
	private void writeBytes(final byte[] bytes) throws IOException {
		writeMessageBytes(bytes, m_bufferedOutToServer);
	}
	
	private byte[] readBytes() throws IOException {
		return readMessageBytes(m_bufferedInFromServer);
	}

	/**
	 * Writes the given message size as 32 bit integer into the output stream.
	 *
	 * @param size
	 *            The size to write
	 * @param outputStream
	 *            The stream to write to
	 * @throws IOException
	 *             If an error occured
	 */
	private static void writeSize(final int size, final DataOutputStream outputStream) throws IOException {
		outputStream.write(ByteBuffer.allocate(4).putInt(size).array());
		outputStream.flush();
	}

	/**
	 * Writes the given message to the output stream.
	 *
	 * @param bytes
	 *            The message as byte array
	 * @param outputStream
	 *            The stream to write to
	 * @throws IOException
	 *             If an error occured
	 */
	private static void writeMessageBytes(final byte[] bytes, final DataOutputStream outputStream) throws IOException {
		writeSize(bytes.length, outputStream);
		outputStream.write(bytes);
		outputStream.flush();
	}

	/**
	 * Reads the next 32 bit from the input stream and interprets them as
	 * integer.
	 *
	 * @param inputReader
	 *            The stream to read from
	 * @return The read size
	 * @throws IOException
	 *             If an error occured
	 */
	private static int readSize(final DataInputStream inputStream) throws IOException {
		final byte[] bytes = new byte[4];
		//long millis = System.currentTimeMillis();
		inputStream.readFully(bytes);
		//NodeLogger.getLogger("readSize").warn("Spent " + (System.currentTimeMillis() - millis) + "ms in readSize().");
		return ByteBuffer.wrap(bytes).getInt();
	}

	/**
	 * Reads the next message from the input stream.
	 *
	 * @param inputStream
	 *            The stream to read from
	 * @return The message as byte array
	 * @throws IOException
	 *             If an error occured
	 */
	private static byte[] readMessageBytes(final DataInputStream inputStream) throws IOException {
		final int size = readSize(inputStream);
		final byte[] bytes = new byte[size];
		//NodeLogger.getLogger("readMessageBytes").warn("Received " + size + " bytes.");
		inputStream.readFully(bytes);
		return bytes;
	}

}
