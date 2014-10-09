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
 *
 * History
 *   Sep 25, 2014 (Patrick Winter): created
 */
package org.knime.python.kernel;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ProcessBuilder.Redirect;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.imageio.ImageIO;

import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.python.Activator;
import org.knime.python.kernel.proto.ProtobufAutocompleteSuggestions.AutocompleteSuggestions;
import org.knime.python.kernel.proto.ProtobufAutocompleteSuggestions.AutocompleteSuggestions.AutocompleteSuggestion;
import org.knime.python.kernel.proto.ProtobufExecuteResponse.ExecuteResponse;
import org.knime.python.kernel.proto.ProtobufImage.Image;
import org.knime.python.kernel.proto.ProtobufKnimeTable.Table;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.AppendToTable;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.AutoComplete;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.Execute;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.GetImage;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.GetTable;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.HasAutoComplete;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.ListVariables;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.PutFlowVariables;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.PutFlowVariables.DoubleVariable;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.PutFlowVariables.IntegerVariable;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.PutFlowVariables.StringVariable;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.PutTable;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.Reset;
import org.knime.python.kernel.proto.ProtobufSimpleResponse.SimpleResponse;
import org.knime.python.kernel.proto.ProtobufVariableList.VariableList;
import org.knime.python.kernel.proto.ProtobufVariableList.VariableList.Variable;

/**
 * Provides operations on a python kernel running in another process.
 * 
 * @author Patrick Winter, KNIME.com, Zurich, Switzerland
 */
public class PythonKernel {

	private static final int CHUNK_SIZE = 1000;

	private Process m_process;
	private ServerSocket m_serverSocket;
	private Socket m_socket;
	private boolean m_hasAutocomplete = false;
	private int m_pid = -1;

	/**
	 * Creates a python kernel by starting a python process and connecting to
	 * it.
	 * 
	 * Important: Call the {@link #close()} method when this kernel is no longer
	 * needed to shut down the python process in the background
	 * 
	 * @throws IOException
	 */
	public PythonKernel() throws IOException {
		if (Activator.testPythonInstallation().hasError()) {
			throw new IOException("Could not start python kernel");
		}
		// Create socket to listen on
		m_serverSocket = new ServerSocket(0);
		int port = m_serverSocket.getLocalPort();
		m_serverSocket.setSoTimeout(10000);
		Thread thread = new Thread(new Runnable() {
			public void run() {
				try {
					m_socket = m_serverSocket.accept();
				} catch (IOException e) {
					m_socket = null;
				}
			}
		});
		// Start listening
		thread.start();
		// Get path to python kernel script
		String scriptPath = Activator.getFile("org.knime.python", "py" + File.separator + "PythonKernel.py").getAbsolutePath();
		// Start python kernel that listens to the given port
		ProcessBuilder pb = new ProcessBuilder(Activator.getPythonCommand(), scriptPath, "" + port);
		pb.redirectError(Redirect.INHERIT);
		pb.redirectOutput(Redirect.INHERIT);
		// Start python
		m_process = pb.start();
		try {
			// Wait for python to connect
			thread.join();
		} catch (InterruptedException e) {
		}
		if (m_socket == null) {
			// Python did not connect this kernel is invalid
			close();
			throw new IOException("Could not start python kernel");
		}
		// First get PID of Python process
		m_pid = SimpleResponse.parseFrom(readMessageBytes(m_socket.getInputStream())).getInteger();
		try {
			// Check if python kernel supports autocompletion (this depends
			// on the optional module Jedi)
			Command.Builder commandBuilder = Command.newBuilder();
			commandBuilder.setHasAutoComplete(HasAutoComplete.newBuilder());
			SimpleResponse response;
			synchronized (this) {
				OutputStream outToServer = m_socket.getOutputStream();
				InputStream inFromServer = m_socket.getInputStream();
				writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
				response = SimpleResponse.parseFrom(readMessageBytes(inFromServer));
			}
			m_hasAutocomplete = response.getBoolean();
		} catch (Exception e) {
			//
		}
	}

	/**
	 * Execute the given source code.
	 * 
	 * @param sourceCode
	 *            The source code to execute
	 * @return Standard console output
	 * @throws IOException
	 *             If an error occured
	 */
	public String[] execute(final String sourceCode) throws IOException {
		Command.Builder commandBuilder = Command.newBuilder();
		commandBuilder.setExecute(Execute.newBuilder().setSourceCode(sourceCode));
		ExecuteResponse response;
		synchronized (this) {
			OutputStream outToServer = m_socket.getOutputStream();
			InputStream inFromServer = m_socket.getInputStream();
			writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
			response = ExecuteResponse.parseFrom(readMessageBytes(inFromServer));
		}
		return new String[] { response.getOutput(), response.getError() };
	}

	/**
	 * Execute the given source code while still checking if the given execution
	 * context has been canceled
	 * 
	 * @param sourceCode
	 *            The source code to execute
	 * @param exec
	 *            The execution context to check if execution has been canceled
	 * @throws Exception
	 *             If something goes wrong during execution or if execution has
	 *             been canceled
	 */
	public void execute(final String sourceCode, final ExecutionContext exec) throws Exception {
		final AtomicBoolean done = new AtomicBoolean(false);
		final AtomicReference<Exception> exception = new AtomicReference<Exception>(null);
		final Thread nodeExecutionThread = Thread.currentThread();
		// Thread running the execute
		new Thread(new Runnable() {
			public void run() {
				String[] out;
				try {
					out = execute(sourceCode);
					// If the error log has content throw it as exception
					if (!out[1].isEmpty()) {
						throw new Exception(out[1]);
					}
				} catch (Exception e) {
					exception.set(e);
				}
				done.set(true);
				// Wake up waiting thread
				nodeExecutionThread.interrupt();
			}
		}).start();
		// Wait until execution is done
		while (done.get() != true) {
			try {
				// Wake up once a second to check if execution has been canceled
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// Happens if python thread is done
			}
			exec.checkCanceled();
		}
		// If their was an exception in the execution thread throw it here
		if (exception.get() != null) {
			throw exception.get();
		}
	}

	/**
	 * Put the given flow variables into the workspace.
	 * 
	 * The given flow variables will be available as a dict with the given name
	 * 
	 * @param name
	 *            The name of the dict
	 * @param flowVariables
	 *            The flow variables to put
	 * @throws IOException
	 *             If an error occured
	 */
	public void putFlowVariables(final String name, final Collection<FlowVariable> flowVariables) throws IOException {
		Command.Builder commandBuilder = Command.newBuilder();
		PutFlowVariables.Builder putFlowVariablesBuilder = PutFlowVariables.newBuilder().setKey(name);
		for (FlowVariable flowVariable : flowVariables) {
			String key = flowVariable.getName();
			Object value;
			switch (flowVariable.getType()) {
			case INTEGER:
				value = flowVariable.getIntValue();
				break;
			case DOUBLE:
				value = flowVariable.getDoubleValue();
				break;
			case STRING:
				value = flowVariable.getStringValue();
				break;
			default:
				value = flowVariable.getValueAsString();
				break;
			}
			if (value instanceof Integer) {
				IntegerVariable variable = IntegerVariable.newBuilder().setKey(key).setValue((Integer) value).build();
				putFlowVariablesBuilder.addIntegerVariable(variable);
			} else if (value instanceof Double) {
				DoubleVariable variable = DoubleVariable.newBuilder().setKey(key).setValue((Double) value).build();
				putFlowVariablesBuilder.addDoubleVariable(variable);
			} else if (value instanceof String) {
				StringVariable variable = StringVariable.newBuilder().setKey(key).setValue((String) value).build();
				putFlowVariablesBuilder.addStringVariable(variable);
			}
		}
		commandBuilder.setPutFlowVariables(putFlowVariablesBuilder);
		synchronized (this) {
			OutputStream outToServer = m_socket.getOutputStream();
			writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
			InputStream inFromServer = m_socket.getInputStream();
			readMessageBytes(inFromServer);
		}
	}

	/**
	 * Put the given {@link BufferedDataTable} into the workspace.
	 * 
	 * The table will be available as a pandas.DataFrame.
	 * 
	 * @param name
	 *            The name of the table
	 * @param table
	 *            The table
	 * @param executionMonitor
	 *            The monitor that will be updated about progress
	 * @param rowLimit
	 *            The amount of rows that will be transfered
	 * @throws IOException
	 *             If an error occured
	 */
	public void putDataTable(final String name, final BufferedDataTable table, final ExecutionMonitor executionMonitor,
			int rowLimit) throws IOException {
		if (rowLimit > table.getRowCount()) {
			rowLimit = table.getRowCount();
		}
		ExecutionMonitor serializationMonitor = executionMonitor.createSubProgress(0.5);
		ExecutionMonitor deserializationMonitor = executionMonitor.createSubProgress(0.5);
		int rowsDeserialized = 0;
		CloseableRowIterator rowIterator = table.iteratorFailProve();
		int chunk = 0;
		Table tableMessage = ProtobufConverter.dataTableToProtobuf(table, CHUNK_SIZE, rowIterator, chunk++,
				serializationMonitor, rowLimit);
		Command.Builder commandBuilder = Command.newBuilder();
		commandBuilder.setPutTable(PutTable.newBuilder().setKey(name).setTable(tableMessage));
		synchronized (this) {
			OutputStream outToServer = m_socket.getOutputStream();
			writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
			InputStream inFromServer = m_socket.getInputStream();
			readMessageBytes(inFromServer);
			rowsDeserialized += tableMessage.getNumRows();
			deserializationMonitor.setProgress(rowsDeserialized / (double) rowLimit);
			while (rowsDeserialized < rowLimit) {
				tableMessage = ProtobufConverter.dataTableToProtobuf(table, CHUNK_SIZE, rowIterator, chunk++,
						serializationMonitor, rowLimit);
				commandBuilder = Command.newBuilder();
				commandBuilder.setAppendToTable(AppendToTable.newBuilder().setKey(name).setTable(tableMessage));
				writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
				readMessageBytes(inFromServer);
				rowsDeserialized += tableMessage.getNumRows();
				deserializationMonitor.setProgress(rowsDeserialized / (double) rowLimit);
			}
		}
		rowIterator.close();
	}

	/**
	 * Put the given {@link BufferedDataTable} into the workspace.
	 * 
	 * The table will be available as a pandas.DataFrame.
	 * 
	 * @param name
	 *            The name of the table
	 * @param table
	 *            The table
	 * @param executionMonitor
	 *            The monitor that will be updated about progress
	 * @throws IOException
	 *             If an error occured
	 */
	public void putDataTable(final String name, final BufferedDataTable table, final ExecutionMonitor executionMonitor)
			throws IOException {
		putDataTable(name, table, executionMonitor, table.getRowCount());
	}

	/**
	 * Get a {@link BufferedDataTable} from the workspace.
	 * 
	 * @param name
	 *            The name of the table to get
	 * @return The table
	 * @param executionMonitor
	 *            The monitor that will be updated about progress
	 * @throws IOException
	 *             If an error occured
	 */
	public BufferedDataTable getDataTable(final String name, final ExecutionContext exec,
			final ExecutionMonitor executionMonitor) throws IOException {
		ExecutionMonitor serializationMonitor = executionMonitor.createSubProgress(0.5);
		ExecutionMonitor deserializationMonitor = executionMonitor.createSubProgress(0.5);
		int rowsSerialized = 0;
		Command.Builder commandBuilder = Command.newBuilder();
		commandBuilder.setGetTable(GetTable.newBuilder().setKey(name).setChunkSize(CHUNK_SIZE));
		BufferedDataContainer container = null;
		synchronized (this) {
			OutputStream outToServer = m_socket.getOutputStream();
			InputStream inFromServer = m_socket.getInputStream();
			writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
			int rows = SimpleResponse.parseFrom(readMessageBytes(inFromServer)).getInteger();
			int chunks = (int) Math.ceil(rows / (double) CHUNK_SIZE);
			if (chunks == 0) {
				// this happens if the table has no rows, we still want to
				// receive the specs
				chunks = 1;
			}
			for (int i = 0; i < chunks; i++) {
				Table table = Table.parseFrom(readMessageBytes(inFromServer));
				rowsSerialized += table.getNumRows();
				serializationMonitor.setProgress(rowsSerialized / (double) rows);
				if (container == null) {
					// The first time we need to create the container based on
					// the specs of the table
					container = ProtobufConverter.createContainerFromProtobuf(table, exec);
				}
				ProtobufConverter.addRowsFromProtobuf(table, container, rows, deserializationMonitor);
			}
		}
		container.close();
		return container.getTable();
	}

	/**
	 * Get an image from the workspace.
	 * 
	 * The variable on the python site has to hold a byte string representing an
	 * image.
	 * 
	 * @param name
	 *            The name of the image
	 * @throws IOException
	 *             If an error occured
	 */
	public BufferedImage getImage(final String name) throws IOException {
		Command.Builder commandBuilder = Command.newBuilder();
		commandBuilder.setGetImage(GetImage.newBuilder().setKey(name));
		Image img;
		synchronized (this) {
			OutputStream outToServer = m_socket.getOutputStream();
			InputStream inFromServer = m_socket.getInputStream();
			writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
			img = Image.parseFrom(readMessageBytes(inFromServer));
		}
		if (img.hasError()) {
			throw new IOException(img.getError());
		}
		return ImageIO.read(img.getBytes().newInput());
	}

	/**
	 * Returns the list of all defined variables, functions, classes and loaded
	 * modules.
	 * 
	 * Each variable map contains the fields 'name', 'type' and 'value'.
	 * 
	 * @return List of variables currently defined in the workspace
	 * @throws IOException
	 *             If an error occured
	 */
	public List<Map<String, String>> listVariables() throws IOException {
		Command.Builder commandBuilder = Command.newBuilder();
		commandBuilder.setListVariables(ListVariables.newBuilder());
		VariableList response;
		synchronized (this) {
			OutputStream outToServer = m_socket.getOutputStream();
			InputStream inFromServer = m_socket.getInputStream();
			writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
			response = VariableList.parseFrom(readMessageBytes(inFromServer));
		}
		List<Map<String, String>> variables = new ArrayList<Map<String, String>>();
		for (Variable variable : response.getVariableList()) {
			Map<String, String> map = new HashMap<String, String>();
			map.put("name", variable.getKey());
			map.put("type", variable.getType());
			map.put("value", variable.getValue());
			variables.add(map);
		}
		return variables;
	}

	/**
	 * Resets the workspace of the python kernel.
	 * 
	 * @throws IOException
	 *             If an error occured
	 */
	public void resetWorkspace() throws IOException {
		Command.Builder commandBuilder = Command.newBuilder();
		commandBuilder.setReset(Reset.newBuilder());
		synchronized (this) {
			OutputStream outToServer = m_socket.getOutputStream();
			writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
		}
	}

	/**
	 * Returns the list of possible auto completions to the given source at the
	 * given position.
	 * 
	 * Each auto completion contains the fields 'name', 'type' and 'doc'.
	 * 
	 * @param sourceCode
	 *            The source code
	 * @param line
	 *            Cursor position (line)
	 * @param column
	 *            Cursor position (column)
	 * @return Possible auto completions.
	 * @throws IOException
	 *             If an error occured
	 */
	public List<Map<String, String>> autoComplete(final String sourceCode, final int line, final int column)
			throws IOException {
		// If auto completion is not supported just return an empty list
		if (!m_hasAutocomplete) {
			return new ArrayList<Map<String, String>>(0);
		}
		Command.Builder commandBuilder = Command.newBuilder();
		commandBuilder.setAutoComplete(AutoComplete.newBuilder().setSourceCode(sourceCode).setLine(line)
				.setColumn(column));
		AutocompleteSuggestions response;
		synchronized (this) {
			OutputStream outToServer = m_socket.getOutputStream();
			InputStream inFromServer = m_socket.getInputStream();
			writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
			response = AutocompleteSuggestions.parseFrom(readMessageBytes(inFromServer));
		}
		List<Map<String, String>> autocompleteSuggestions = new ArrayList<Map<String, String>>();
		for (AutocompleteSuggestion suggestion : response.getAutocompleteSuggestionList()) {
			Map<String, String> map = new HashMap<String, String>();
			map.put("name", suggestion.getName());
			map.put("type", suggestion.getType());
			map.put("doc", suggestion.getDoc());
			autocompleteSuggestions.add(map);
		}
		return autocompleteSuggestions;
	}

	/**
	 * Shuts down the python kernel.
	 * 
	 * This shuts down the python background process and closes the sockets used
	 * for communication.
	 */
	public void close() {
		try {
			m_serverSocket.close();
		} catch (Throwable t) {
		}
		try {
			m_socket.close();
		} catch (Throwable t) {
		}
		m_process.destroy();
		// If the original process was a script we have to kill the actual Python process by PID
		if (m_pid >= 0) {
			try {
				ProcessBuilder pb;
				if (System.getProperty("os.name").toLowerCase().contains("win")) {
					pb = new ProcessBuilder("taskkill", "/F", "/PID", "" + m_pid);
				} else {
					pb = new ProcessBuilder("kill", "-KILL", "" + m_pid);
				}
				pb.start();
			} catch (IOException e) {
				//
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void finalize() throws Throwable {
		close();
		super.finalize();
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
		byte[] bytes = new byte[4];
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
		int size = readSize(inputStream);
		byte[] bytes = new byte[size];
		int bytesRead = 0;
		while (bytesRead < bytes.length) {
			bytesRead += inputStream.read(bytes, bytesRead, bytes.length - bytesRead);
		}
		return bytes;
	}

}
