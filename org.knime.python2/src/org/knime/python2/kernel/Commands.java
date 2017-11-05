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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Used for communicating with the python kernel via commands sent over sockets.
 *
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 */
public class Commands {

    private final OutputStream m_outToServer;

    private final InputStream m_inFromServer;

    private final DataInputStream m_bufferedInFromServer;

    private final DataOutputStream m_bufferedOutToServer;

    private final List<AbstractPythonToJavaMessageHandler> m_msgHandlers;

    private boolean m_responseHandlingActive;

    private boolean m_answered;

    /**
     * Constructor.
     *
     * @param outToServer output stream of the socket used for communication with the python kernel
     * @param inFromServer input stream of the socket used for communication with the python kernel
     */
    public Commands(final OutputStream outToServer, final InputStream inFromServer) {
        m_outToServer = outToServer;
        m_inFromServer = inFromServer;
        m_bufferedInFromServer = new DataInputStream(m_inFromServer);
        m_bufferedOutToServer = new DataOutputStream(m_outToServer);
        m_msgHandlers = new ArrayList<AbstractPythonToJavaMessageHandler>();
        registerMessageHandler(new AbstractPythonToJavaMessageHandler("success") {

            @Override
            protected void handle(final PythonToJavaMessage msg) {}
        });
    }

    /**
     * Register a handler for dealing with a subset of possible {@link PythonToJavaMessage}s from python.
     *
     * @param handler handles {@link PythonToJavaMessage}s having a specific command
     */
    synchronized public void registerMessageHandler(final AbstractPythonToJavaMessageHandler handler) {
        m_msgHandlers.add(handler);
    }

    /**
     * Unregister an existing {@link AbstractPythonToJavaMessageHandler}. If it is not present in the internal list nothing happens.
     *
     * @param handler a {@link AbstractPythonToJavaMessageHandler}
     */
    synchronized public void unregisterMessageHandler(final AbstractPythonToJavaMessageHandler handler) {
        m_msgHandlers.remove(handler);
    }

    /**
     * Direct a {@link PythonToJavaMessage} to the appropriate registered {@link AbstractPythonToJavaMessageHandler}. If the message
     * is a request it has to be answered by calling answer() exactly once.
     *
     * @param msg a message from the python process
     * @return true if the message was a request, false otherwise
     */
    private boolean handleResponse(final PythonToJavaMessage msg) {
        m_responseHandlingActive = true;
        m_answered = false;
        boolean handled = false;
        for(AbstractPythonToJavaMessageHandler handler:m_msgHandlers) {
            handled = handler.tryHandle(msg);
            if(handled) {
                break;
            }
        }
        if(!handled) {
            throw new IllegalStateException("Python response message was not handled. Command: " + msg.getCommand());
        } else if(msg.isRequest() && !m_answered) {
            throw new IllegalStateException("Python request was not answered. Command: " + msg.getCommand());
        }
        m_responseHandlingActive = false;
        return msg.isRequest();
    }

    /**
     * Answer the currently processed {@link PythonToJavaMessage} with the passed string.
     * Can only be called once per message.
     *
     * @param answerStr an answer to the currently processed {@link PythonToJavaMessage}
     * @throws IOException
     */
    public synchronized void answer(final String answerStr) throws IOException {
        if(m_responseHandlingActive) {
            writeString(answerStr);
            m_answered = true;
            m_responseHandlingActive = false;
            return;
        }
        throw new IllegalStateException("answer() may only be called once while answering each ResponseMessage!");
    }

    /**
     * Get the python kernel's process id.
     *
     * @return the process id
     * @throws IOException
     */
    synchronized public int getPid() throws IOException {
        return readInt();
    }

    /**
     * Execute a source code snippet in the python kernel.
     *
     * @param sourceCode the snippet to execute
     * @return warning or error messages that were emitted during execution
     * @throws IOException
     */
    synchronized public String[] execute(final String sourceCode) throws IOException {
        writeString("execute");
        writeString(sourceCode);
        while(handleResponse(readResponseMessage())) {}
        final String[] output = new String[2];
        output[0] = readString();
        output[1] = readString();
        return output;
    }

    /**
     * Put some serialized flow variables into the python workspace. The flow variables should be serialized using the
     * currently active serialization library.
     *
     * @param name the name of the variable in the python workspace
     * @param variables the serialized variables table as bytearray
     * @throws IOException
     */
    synchronized public void putFlowVariables(final String name, final byte[] variables) throws IOException {
        writeString("putFlowVariables");
        writeString(name);
        writeBytes(variables);
        readBytes();
    }

    /**
     * Get some serialized flow variables from the python workspace.
     *
     * @param name the variable name in the python workspace
     * @return the serialized variables table as bytearray
     * @throws IOException
     */
    synchronized public byte[] getFlowVariables(final String name) throws IOException {
        writeString("getFlowVariables");
        writeString(name);
        return readBytes();
    }

    /**
     * Put a serialized KNIME table into the python workspace (as pandas.DataFrame). The table should be serialized
     * using the currently active serialization library.
     *
     * @param name the name of the variable in python workspace
     * @param table the serialized KNIME table as bytearray
     * @throws IOException
     */
    synchronized public void putTable(final String name, final byte[] table) throws IOException {
        writeString("putTable");
        writeString(name);
        writeBytes(table);
        while(handleResponse(readResponseMessage())) {}
    }

    /**
     * Append a chunk of table rows to a table represented as pandas.DataFrame in the python workspace. The table chunk
     * should be serialized using the currently active serialization library.
     *
     * @param name the name of the variable in the python workspace
     * @param table the serialized table chunk as bytearray
     * @throws IOException
     */
    synchronized public void appendToTable(final String name, final byte[] table) throws IOException {
        writeString("appendToTable");
        writeString(name);
        writeBytes(table);
        while(handleResponse(readResponseMessage())) {}
    }

    /**
     * Get the size in bytes of a serialized table from the python workspace.
     *
     * @param name the variable name
     * @return the size in bytes
     * @throws IOException
     */
    synchronized public int getTableSize(final String name) throws IOException {
        writeString("getTableSize");
        writeString(name);
        return readInt();
    }

    /**
     * Get a serialized KNIME table from the python workspace.
     *
     * @param name the name of the variable in the python workspace
     * @return the serialized table as bytearray
     * @throws IOException
     */
    synchronized public byte[] getTable(final String name) throws IOException {
        //TODO rewrite in prepare / get
        writeString("getTable");
        writeString(name);
        //success message is sent before table is transmitted
        while(handleResponse(readResponseMessage())) {}
        return readBytes();
    }

    /**
     * Get a chunk of a serialized KNIME table from the python workspace.
     *
     * @param name the name of the variable in the python workspace
     * @param start the starting row of the chunk
     * @param end the last row of the chunk
     * @return the serialized table as bytearray
     * @throws IOException
     */
    synchronized public byte[] getTableChunk(final String name, final int start, final int end) throws IOException {
        writeString("getTableChunk");
        writeString(name);
        writeInt(start);
        writeInt(end);
        //success message is sent before table is transmitted
        while(handleResponse(readResponseMessage())) {}
        return readBytes();
    }

    /**
     * Get a list of the variable names in the python workspace.
     *
     * @return the serialized list of variable names
     * @throws IOException
     */
    synchronized public byte[] listVariables() throws IOException {
        writeString("listVariables");
        return readBytes();
    }

    /**
     * Reset the python workspace by emptying the variable definitions.
     *
     * @throws IOException
     */
    synchronized public void reset() throws IOException {
        writeString("reset");
        readBytes();
    }

    /**
     * Indicates if python supports autocompletion.
     *
     * @return autocompletion yes/no
     * @throws IOException
     */
    synchronized public boolean hasAutoComplete() throws IOException {
        writeString("hasAutoComplete");
        return readInt() > 0;
    }

    /**
     * Get a list of autocompletion suggestions for the given source code snippet.
     *
     * @param sourceCode the source code snippet in which the auto completion should be done
     * @param line the line number in the snippet for which auto completion is requested
     * @param column the cursor position in the line
     * @return serialized list of autocompletion suggestions
     * @throws IOException
     */
    synchronized public byte[] autoComplete(final String sourceCode, final int line, final int column)
            throws IOException {
        writeString("autoComplete");
        writeString(sourceCode);
        writeInt(line);
        writeInt(column);
        return readBytes();
    }

    /**
     * Get an image from the python workspace
     *
     * @param name the name of the variable in the python workspace
     * @return a serialized image
     * @throws IOException
     */
    synchronized public byte[] getImage(final String name) throws IOException {
        writeString("getImage");
        writeString(name);
        return readBytes();
    }

    /**
     * Get a python object from the python workspace. The object consists of a pickled representation, a type and a
     * string representation.
     *
     * @param name the name of the variable in the python workspace
     * @return a serialized python object
     * @throws IOException
     */
    synchronized public byte[] getObject(final String name) throws IOException {
        writeString("getObject");
        writeString(name);
        return readBytes();
    }

    /**
     * Put a python object into the python workspace. The object consists of a pickled representation, a type and a
     * string representation.
     *
     * @param name the name of the variable in the python workspace
     * @param object a serialized python object
     * @throws IOException
     */
    synchronized public void putObject(final String name, final byte[] object) throws IOException {
        writeString("putObject");
        writeString(name);
        writeBytes(object);
        readBytes();
    }

    /**
     * Add a serializer for an extension type to the python workspace.
     *
     * @param id the extension id (in java)
     * @param type the python type identifier
     * @param path the path to the code file containing the serializer function
     * @throws IOException
     */
    synchronized public void addSerializer(final String id, final String type, final String path) throws IOException {
        writeString("addSerializer");
        writeString(id);
        writeString(type);
        writeString(path);
        readBytes();
    }

    /**
     * Add a deserializer for an extension type to the python workspace.
     *
     * @param id the extension id (in java)
     * @param path the path to the code file containing the deserializer function
     * @throws IOException
     */
    synchronized public void addDeserializer(final String id, final String path) throws IOException {
        writeString("addDeserializer");
        writeString(id);
        writeString(path);
        readBytes();
    }

    /**
     * Shut down the python kernel to properly end the connection.
     *
     * @throws IOException
     */
    synchronized public void shutdown() throws IOException {
        writeString("shutdown");
    }

    /**
     * Send information on how to connect to a specific SQL database alongside a query to the python workspace.
     *
     * @param name the name of the variable in the python workspace
     * @param sql the serialized table containing the the entries: driver, jdbcurl, username, password, jars, query,
     *            dbidentifier
     * @throws IOException
     */
    synchronized public void putSql(final String name, final byte[] sql) throws IOException {
        writeString("putSql");
        writeString(name);
        writeBytes(sql);
        readBytes();
    }

    /**
     * Gets a SQL query from the python workspace.
     *
     * @param name the name of the variable in the python workspace
     * @return the SQL query
     * @throws IOException
     */
    synchronized public String getSql(final String name) throws IOException {
        writeString("getSql");
        writeString(name);
        return readString();
    }

    /**
     * Transmit the paths to all custom module directories and make them available via the pythonpath.
     *
     * @param paths ';' separated list of directories
     * @throws IOException
     */
    synchronized public void addToPythonPath(final String paths) throws IOException {
        writeString("setCustomModulePaths");
        writeString(paths);
        readBytes();
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
     * @param size The size to write
     * @param outputStream The stream to write to
     * @throws IOException If an error occured
     */
    private static void writeSize(final int size, final DataOutputStream outputStream) throws IOException {
        outputStream.write(ByteBuffer.allocate(4).putInt(size).array());
        outputStream.flush();
    }

    /**
     * Writes the given message to the output stream.
     *
     * @param bytes The message as byte array
     * @param outputStream The stream to write to
     * @throws IOException If an error occured
     */
    private static void writeMessageBytes(final byte[] bytes, final DataOutputStream outputStream) throws IOException {
        writeSize(bytes.length, outputStream);
        outputStream.write(bytes);
        outputStream.flush();
    }

    private PythonToJavaMessage readResponseMessage() throws IOException {
        byte[] bytes = readMessageBytes(m_bufferedInFromServer);
        String str = new String(bytes, StandardCharsets.UTF_8);
        String[] reqCmdVal = str.split(":");
        return new PythonToJavaMessage(reqCmdVal[1], reqCmdVal[2], reqCmdVal[0].equals("r"));
    }

    /**
     * Reads the next 32 bit from the input stream and interprets them as integer.
     *
     * @param inputReader The stream to read from
     * @return The read size
     * @throws IOException If an error occured
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
     * @param inputStream The stream to read from
     * @return The message as byte array
     * @throws IOException If an error occured
     */
    private static byte[] readMessageBytes(final DataInputStream inputStream) throws IOException {
        final int size = readSize(inputStream);
        final byte[] bytes = new byte[size];
        //NodeLogger.getLogger("readMessageBytes").warn("Received " + size + " bytes.");
        inputStream.readFully(bytes);
        return bytes;
    }

}
