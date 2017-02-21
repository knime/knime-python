package org.knime.python2.kernel;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class Commands {
	
	private final OutputStream m_outToServer;
	private final InputStream m_inFromServer;
	
	public Commands(final OutputStream outToServer, final InputStream inFromServer) {
		m_outToServer = outToServer;
		m_inFromServer = inFromServer;
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
	
	synchronized public byte[] getSql(final String name) throws IOException {
		writeString("getSql");
		writeString(name);
		return readBytes();
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
		writeMessageBytes(stringToBytes(string), m_outToServer);
	}
	
	private String readString() throws IOException {
		return stringFromBytes(readMessageBytes(m_inFromServer));
	}
	
	private void writeInt(final int integer) throws IOException {
		writeMessageBytes(intToBytes(integer), m_outToServer);
	}
	
	private int readInt() throws IOException {
		return intFromBytes(readMessageBytes(m_inFromServer));
	}
	
	private void writeBytes(final byte[] bytes) throws IOException {
		writeMessageBytes(bytes, m_outToServer);
	}
	
	private byte[] readBytes() throws IOException {
		return readMessageBytes(m_inFromServer);
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
	private static void writeSize(final int size, final OutputStream outputStream) throws IOException {
		outputStream.write(ByteBuffer.allocate(4).putInt(size).array());
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
	private static void writeMessageBytes(final byte[] bytes, final OutputStream outputStream) throws IOException {
		writeSize(bytes.length, outputStream);
		outputStream.write(bytes);
	}

	/**
	 * Reads the next 32 bit from the input stream and interprets them as
	 * integer.
	 *
	 * @param inputStream
	 *            The stream to read from
	 * @return The read size
	 * @throws IOException
	 *             If an error occured
	 */
	private static int readSize(final InputStream inputStream) throws IOException {
		final byte[] bytes = new byte[4];
		int bytesRead = 0;
		while (bytesRead < bytes.length) {
			bytesRead += inputStream.read(bytes, bytesRead, bytes.length - bytesRead);
		}
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
	private static byte[] readMessageBytes(final InputStream inputStream) throws IOException {
		final int size = readSize(inputStream);
		final byte[] bytes = new byte[size];
		int bytesRead = 0;
		while (bytesRead < bytes.length) {
			bytesRead += inputStream.read(bytes, bytesRead, bytes.length - bytesRead);
		}
		return bytes;
	}

}
