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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.imageio.ImageIO;

import org.apache.batik.dom.svg.SAXSVGDocumentFactory;
import org.apache.batik.transcoder.TranscoderException;
import org.apache.batik.util.XMLResourceDescriptor;
import org.knime.code.generic.ImageContainer;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.filestore.FileStoreFactory;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.util.ThreadUtils;
import org.knime.python.Activator;
import org.knime.python.PythonKernelTestResult;
import org.knime.python.kernel.proto.ProtobufAutocompleteSuggestions.AutocompleteSuggestions;
import org.knime.python.kernel.proto.ProtobufAutocompleteSuggestions.AutocompleteSuggestions.AutocompleteSuggestion;
import org.knime.python.kernel.proto.ProtobufExecuteResponse.ExecuteResponse;
import org.knime.python.kernel.proto.ProtobufImage.Image;
import org.knime.python.kernel.proto.ProtobufKnimeTable.Table;
import org.knime.python.kernel.proto.ProtobufPickledObject;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.AddDeserializers;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.AddSerializers;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.AppendToTable;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.AutoComplete;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.Deserializer;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.Execute;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.GetImage;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.GetObject;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.GetTable;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.HasAutoComplete;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.ListVariables;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.PutFlowVariables;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.PutFlowVariables.DoubleVariable;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.PutFlowVariables.IntegerVariable;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.PutFlowVariables.StringVariable;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.PutObject;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.PutTable;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.Reset;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.Serializer;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.Shutdown;
import org.knime.python.kernel.proto.ProtobufSimpleResponse.SimpleResponse;
import org.knime.python.kernel.proto.ProtobufVariableList.VariableList;
import org.knime.python.kernel.proto.ProtobufVariableList.VariableList.Variable;
import org.knime.python.port.PickledObject;
import org.knime.python.typeextension.KnimeToPythonExtension;
import org.knime.python.typeextension.KnimeToPythonExtensions;
import org.knime.python.typeextension.PythonToKnimeExtension;
import org.knime.python.typeextension.PythonToKnimeExtensions;
import org.w3c.dom.svg.SVGDocument;

import com.google.protobuf.ByteString;

/**
 * Provides operations on a python kernel running in another process.
 *
 * @author Patrick Winter, KNIME.com, Zurich, Switzerland
 */
public class PythonKernel {

	private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonKernel.class);

	private static final int CHUNK_SIZE = 1000;

	private static final AtomicInteger THREAD_UNIQUE_ID = new AtomicInteger();

	private final Process m_process;
	private final ServerSocket m_serverSocket;
	private Socket m_socket;
	private boolean m_hasAutocomplete = false;
	private int m_pid = -1;
	private boolean m_closed = false;
	private final KnimeToPythonExtensions knimeToPythonExtensions = new KnimeToPythonExtensions();
	private final PythonToKnimeExtensions pythonToKnimeExtensions = new PythonToKnimeExtensions();

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
		final PythonKernelTestResult testResult = Activator.testPythonInstallation();
		if (testResult.hasError()) {
			throw new IOException("Could not start python kernel:\n" + testResult.getMessage());
		}
		// Create socket to listen on
		m_serverSocket = new ServerSocket(0);
		final int port = m_serverSocket.getLocalPort();
		m_serverSocket.setSoTimeout(10000);
		final Thread thread = new Thread(new Runnable() {
			@Override
            public void run() {
				try {
					m_socket = m_serverSocket.accept();
				} catch (final IOException e) {
					m_socket = null;
				}
			}
		});
		// Start listening
		thread.start();
		// Get path to python kernel script
		final String scriptPath = Activator.getFile("org.knime.python", "py/PythonKernel.py").getAbsolutePath();
		// Start python kernel that listens to the given port
		final ProcessBuilder pb = new ProcessBuilder(Activator.getPythonCommand(), scriptPath, "" + port);
		// Add all python modules to PYTHONPATH variable
		String existingPath = pb.environment().get("PYTHONPATH");
//		existingPath = existingPath == null ? "" : existingPath;
//		final String externalPythonPath = PythonModuleExtensions.getPythonPath();
//		if (externalPythonPath != null && !externalPythonPath.isEmpty()) {
//			existingPath = existingPath + File.pathSeparator + externalPythonPath;
//		}
		existingPath = existingPath == null ? "" : existingPath + File.pathSeparator;
		pb.environment().put("PYTHONPATH", existingPath);
		// Start python
		m_process = pb.start();
		try {
			// Wait for python to connect
			thread.join();
		} catch (final InterruptedException e) {
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
			final Command.Builder commandBuilder = Command.newBuilder();
			commandBuilder.setHasAutoComplete(HasAutoComplete.newBuilder());
			final OutputStream outToServer = m_socket.getOutputStream();
			final InputStream inFromServer = m_socket.getInputStream();
			writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
			final SimpleResponse response = SimpleResponse.parseFrom(readMessageBytes(inFromServer));
			m_hasAutocomplete = response.getBoolean();
		} catch (final Exception e) {
			//
		}
		// Python serializers
		Command.Builder commandBuilder = Command.newBuilder();
		final AddSerializers.Builder addSerializers = AddSerializers.newBuilder();
		for (final PythonToKnimeExtension typeExtension : PythonToKnimeExtensions.getExtensions()) {
			addSerializers.addSerializer(Serializer.newBuilder().setId(typeExtension.getId()).setType(typeExtension.getType())
					.setPath(typeExtension.getPythonSerializerPath()));
		}
		commandBuilder.setAddSerializers(addSerializers);
		OutputStream outToServer = m_socket.getOutputStream();
		InputStream inFromServer = m_socket.getInputStream();
		writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
		readMessageBytes(inFromServer);
		// Python deserializers
		commandBuilder = Command.newBuilder();
		final AddDeserializers.Builder addDeserializers = AddDeserializers.newBuilder();
		for (final KnimeToPythonExtension typeExtension : KnimeToPythonExtensions.getExtensions()) {
			addDeserializers.addDeserializer(Deserializer.newBuilder().setId(typeExtension.getId())
					.setPath(typeExtension.getPythonDeserializerPath()));
		}
		commandBuilder.setAddDeserializers(addDeserializers);
		outToServer = m_socket.getOutputStream();
		inFromServer = m_socket.getInputStream();
		writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
		readMessageBytes(inFromServer);
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
		final Command.Builder commandBuilder = Command.newBuilder();
		commandBuilder.setExecute(Execute.newBuilder().setSourceCode(sourceCode));
		ExecuteResponse response;
		synchronized (this) {
			final OutputStream outToServer = m_socket.getOutputStream();
			final InputStream inFromServer = m_socket.getInputStream();
			writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
			response = ExecuteResponse.parseFrom(readMessageBytes(inFromServer));
		}
		if (response.getOutput().length() > 0) {
			LOGGER.debug(response.getOutput());
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
	public String[] execute(final String sourceCode, final ExecutionContext exec) throws Exception {
		final AtomicBoolean done = new AtomicBoolean(false);
		final AtomicReference<Exception> exception = new AtomicReference<Exception>(null);
		final Thread nodeExecutionThread = Thread.currentThread();
		final AtomicReference<String[]> output = new AtomicReference<String[]>();
		// Thread running the execute
		ThreadUtils.threadWithContext(new Runnable() {
			@Override
            public void run() {
				String[] out;
				try {
					out = execute(sourceCode);
					output.set(out);
					// If the error log has content throw it as exception
					if (!out[1].isEmpty()) {
						throw new Exception(out[1]);
					}
				} catch (final Exception e) {
					exception.set(e);
				}
				done.set(true);
				// Wake up waiting thread
				nodeExecutionThread.interrupt();
			}
		}, "KNIME-Python-Exec-" + THREAD_UNIQUE_ID.incrementAndGet()).start();
		// Wait until execution is done
		while (done.get() != true) {
			try {
				// Wake up once a second to check if execution has been canceled
				Thread.sleep(1000);
			} catch (final InterruptedException e) {
				// Happens if python thread is done
			}
			exec.checkCanceled();
		}
		// If their was an exception in the execution thread throw it here
		if (exception.get() != null) {
			throw exception.get();
		}
		return output.get();
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
		final Command.Builder commandBuilder = Command.newBuilder();
		final PutFlowVariables.Builder putFlowVariablesBuilder = PutFlowVariables.newBuilder().setKey(name);
		for (final FlowVariable flowVariable : flowVariables) {
			final String key = flowVariable.getName();
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
				final IntegerVariable variable = IntegerVariable.newBuilder().setKey(key).setValue((Integer) value).build();
				putFlowVariablesBuilder.addIntegerVariable(variable);
			} else if (value instanceof Double) {
				final DoubleVariable variable = DoubleVariable.newBuilder().setKey(key).setValue((Double) value).build();
				putFlowVariablesBuilder.addDoubleVariable(variable);
			} else if (value instanceof String) {
				final StringVariable variable = StringVariable.newBuilder().setKey(key).setValue((String) value).build();
				putFlowVariablesBuilder.addStringVariable(variable);
			}
		}
		commandBuilder.setPutFlowVariables(putFlowVariablesBuilder);
		synchronized (this) {
			final OutputStream outToServer = m_socket.getOutputStream();
			writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
			final InputStream inFromServer = m_socket.getInputStream();
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
		if (table == null) {
			throw new IOException("Table " + name + " is not available.");
		}
		if (rowLimit > table.getRowCount()) {
			rowLimit = table.getRowCount();
		}
		final ExecutionMonitor serializationMonitor = executionMonitor.createSubProgress(0.5);
		final ExecutionMonitor deserializationMonitor = executionMonitor.createSubProgress(0.5);
		int rowsDeserialized = 0;
		final CloseableRowIterator rowIterator = table.iteratorFailProve();
		int chunk = 0;
		Table tableMessage = ProtobufConverter.dataTableToProtobuf(table, CHUNK_SIZE, rowIterator, chunk++,
				serializationMonitor, rowLimit, knimeToPythonExtensions);
		Command.Builder commandBuilder = Command.newBuilder();
		commandBuilder.setPutTable(PutTable.newBuilder().setKey(name).setTable(tableMessage));
		synchronized (this) {
			final OutputStream outToServer = m_socket.getOutputStream();
			writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
			final InputStream inFromServer = m_socket.getInputStream();
			SimpleResponse response = SimpleResponse.parseFrom(readMessageBytes(inFromServer));
			if (response.hasString()) {
				throw new IOException(response.getString());
			}
			rowsDeserialized += tableMessage.getNumRows();
			deserializationMonitor.setProgress(rowsDeserialized / (double) rowLimit);
			while (rowsDeserialized < rowLimit) {
				tableMessage = ProtobufConverter.dataTableToProtobuf(table, CHUNK_SIZE, rowIterator, chunk++,
						serializationMonitor, rowLimit, knimeToPythonExtensions);
				commandBuilder = Command.newBuilder();
				commandBuilder.setAppendToTable(AppendToTable.newBuilder().setKey(name).setTable(tableMessage));
				writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
				response = SimpleResponse.parseFrom(readMessageBytes(inFromServer));
				if (response.hasString()) {
					throw new IOException(response.getString());
				}
				rowsDeserialized += tableMessage.getNumRows();
				deserializationMonitor.setProgress(rowsDeserialized / (double) rowLimit);
			}
		}
		rowIterator.close();
	}

	/**
	 * @param name the name of the sql Python variable
	 * @param object the {@link DatabaseQueryConnectionSettings} to transfer
	 * @param cp the {@link CredentialsProvider}
	 * @throws Exception if the object can not be serialised
	 */
	@SuppressWarnings("resource")
	public void putGeneralObject(final EditorObjectWriter object)
			throws Exception {
		final byte[] message = object.getMessage();
		synchronized (this) {
			final OutputStream outToServer = m_socket.getOutputStream();
			writeMessageBytes(message, outToServer);
			final InputStream inFromServer = m_socket.getInputStream();
			final SimpleResponse response = SimpleResponse.parseFrom(readMessageBytes(inFromServer));
			if (response.hasString()) {
				throw new IOException(response.getString());
			}
		}
	}

	public void putObject(final EditorObjectWriter object, final ExecutionContext exec) throws Exception {
		final AtomicBoolean done = new AtomicBoolean(false);
		final AtomicReference<Exception> exception = new AtomicReference<Exception>(null);
		final Thread nodeExecutionThread = Thread.currentThread();
		// Thread running the execute
		new Thread(new Runnable() {
			@Override
            public void run() {
				try {
					putGeneralObject(object);
				} catch (final Exception e) {
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
			} catch (final InterruptedException e) {
				// Happens if python thread is done
			}
			exec.checkCanceled();
		}
		// If their was an exception in the execution thread throw it here
		if (exception.get() != null) {
			throw exception.get();
		}
	}


	@SuppressWarnings("resource")
	public void getGeneralObject(final EditorObjectReader reader) throws IOException {
		final Command command = reader.getCommand();
		synchronized (this) {
			final OutputStream outToServer = m_socket.getOutputStream();
			final InputStream inFromServer = m_socket.getInputStream();
			writeMessageBytes(command.toByteArray(), outToServer);
			final byte[] readMessageBytes = readMessageBytes(inFromServer);
			reader.read(readMessageBytes);
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
		final ExecutionMonitor serializationMonitor = executionMonitor.createSubProgress(0.5);
		final ExecutionMonitor deserializationMonitor = executionMonitor.createSubProgress(0.5);
		int rowsSerialized = 0;
		final Command.Builder commandBuilder = Command.newBuilder();
		commandBuilder.setGetTable(GetTable.newBuilder().setKey(name).setChunkSize(CHUNK_SIZE));
		BufferedDataContainer container = null;
		synchronized (this) {
			final OutputStream outToServer = m_socket.getOutputStream();
			final InputStream inFromServer = m_socket.getInputStream();
			writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
			final int rows = SimpleResponse.parseFrom(readMessageBytes(inFromServer)).getInteger();
			int chunks = (int) Math.ceil(rows / (double) CHUNK_SIZE);
			if (chunks == 0) {
				// this happens if the table has no rows, we still want to
				// receive the specs
				chunks = 1;
			}
			for (int i = 0; i < chunks; i++) {
				final Table table = Table.parseFrom(readMessageBytes(inFromServer));
				rowsSerialized += table.getNumRows();
				serializationMonitor.setProgress(rowsSerialized / (double) rows);
				if (container == null) {
					// The first time we need to create the container based on
					// the specs of the table
					container = ProtobufConverter.createContainerFromProtobuf(table, exec);
				}
				ProtobufConverter.addRowsFromProtobuf(table, container, rows, deserializationMonitor, FileStoreFactory.createWorkflowFileStoreFactory(exec), pythonToKnimeExtensions);
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
	public ImageContainer getImage(final String name) throws IOException {
		final Command.Builder commandBuilder = Command.newBuilder();
		commandBuilder.setGetImage(GetImage.newBuilder().setKey(name));
		Image img;
		synchronized (this) {
			final OutputStream outToServer = m_socket.getOutputStream();
			final InputStream inFromServer = m_socket.getInputStream();
			writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
			img = Image.parseFrom(readMessageBytes(inFromServer));
		}
		if (img.hasError()) {
			throw new IOException(img.getError());
		}
		final ByteString bytes = img.getBytes();
		if (bytes.isValidUtf8() && bytes.toStringUtf8().startsWith("<?xml")) {
			try {
				return new ImageContainer(stringToSVG(bytes.toStringUtf8()));
			} catch (final TranscoderException e) {
				throw new IOException(e.getMessage(), e);
			}
		} else {
			return new ImageContainer(ImageIO.read(bytes.newInput()));
		}
	}

	private SVGDocument stringToSVG(final String svgString) throws IOException {
		SVGDocument doc = null;
		final StringReader reader = new StringReader(svgString);
		try {
			final String parser = XMLResourceDescriptor.getXMLParserClassName();
			final SAXSVGDocumentFactory f = new SAXSVGDocumentFactory(parser);
			doc = f.createSVGDocument("file:/file.svg", reader);
		} finally {
			reader.close();
		}
		return doc;
	}

	public PickledObject getObject(final String name) throws IOException {
		final Command.Builder commandBuilder = Command.newBuilder();
		commandBuilder.setGetObject(GetObject.newBuilder().setKey(name));
		ProtobufPickledObject.PickledObject pickledObject;
		synchronized (this) {
			final OutputStream outToServer = m_socket.getOutputStream();
			final InputStream inFromServer = m_socket.getInputStream();
			writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
			pickledObject = ProtobufPickledObject.PickledObject.parseFrom(readMessageBytes(inFromServer));
		}
		return new PickledObject(pickledObject.getPickledObject().toByteArray(), pickledObject.getType(),
				pickledObject.getStringRepresentation());
	}

	public PickledObject getObject(final String name, final ExecutionContext exec) throws Exception {
		final AtomicBoolean done = new AtomicBoolean(false);
		final AtomicReference<Exception> exception = new AtomicReference<Exception>(null);
		final AtomicReference<PickledObject> pickledObject = new AtomicReference<PickledObject>(null);
		final Thread nodeExecutionThread = Thread.currentThread();
		// Thread running the execute
		new Thread(new Runnable() {
			@Override
            public void run() {
				try {
					pickledObject.set(getObject(name));
				} catch (final Exception e) {
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
			} catch (final InterruptedException e) {
				// Happens if python thread is done
			}
			exec.checkCanceled();
		}
		// If their was an exception in the execution thread throw it here
		if (exception.get() != null) {
			throw exception.get();
		}
		return pickledObject.get();
	}

	public void putObject(final String name, final PickledObject object) throws IOException {
		if (object == null) {
			throw new IOException("Object " + name + " is not available.");
		}
		final Command.Builder commandBuilder = Command.newBuilder();
		commandBuilder.setPutObject(PutObject.newBuilder().setKey(name).setPickledObject(ByteString.copyFrom(object.getPickledObject())));
		synchronized (this) {
			final OutputStream outToServer = m_socket.getOutputStream();
			final InputStream inFromServer = m_socket.getInputStream();
			writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
			readMessageBytes(inFromServer);
		}
	}

	public void putObject(final String name, final PickledObject object, final ExecutionContext exec) throws Exception {
		final AtomicBoolean done = new AtomicBoolean(false);
		final AtomicReference<Exception> exception = new AtomicReference<Exception>(null);
		final Thread nodeExecutionThread = Thread.currentThread();
		// Thread running the execute
		new Thread(new Runnable() {
			@Override
            public void run() {
				try {
					putObject(name, object);
				} catch (final Exception e) {
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
			} catch (final InterruptedException e) {
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
		final Command.Builder commandBuilder = Command.newBuilder();
		commandBuilder.setListVariables(ListVariables.newBuilder());
		VariableList response;
		synchronized (this) {
			final OutputStream outToServer = m_socket.getOutputStream();
			final InputStream inFromServer = m_socket.getInputStream();
			writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
			response = VariableList.parseFrom(readMessageBytes(inFromServer));
		}
		final List<Map<String, String>> variables = new ArrayList<Map<String, String>>();
		for (final Variable variable : response.getVariableList()) {
			final Map<String, String> map = new HashMap<String, String>();
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
		final Command.Builder commandBuilder = Command.newBuilder();
		commandBuilder.setReset(Reset.newBuilder());
		synchronized (this) {
			final OutputStream outToServer = m_socket.getOutputStream();
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
		final Command.Builder commandBuilder = Command.newBuilder();
		commandBuilder.setAutoComplete(AutoComplete.newBuilder().setSourceCode(sourceCode).setLine(line)
				.setColumn(column));
		AutocompleteSuggestions response;
		synchronized (this) {
			final OutputStream outToServer = m_socket.getOutputStream();
			final InputStream inFromServer = m_socket.getInputStream();
			writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
			response = AutocompleteSuggestions.parseFrom(readMessageBytes(inFromServer));
		}
		final List<Map<String, String>> autocompleteSuggestions = new ArrayList<Map<String, String>>();
		for (final AutocompleteSuggestion suggestion : response.getAutocompleteSuggestionList()) {
			final Map<String, String> map = new HashMap<String, String>();
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
		if (!m_closed) {
			m_closed = true;
			new Thread(new Runnable() {
				@Override
                public void run() {
					printStreamToLog();
					// Send shutdown
					try {
						final Command.Builder commandBuilder = Command.newBuilder();
						commandBuilder.setShutdown(Shutdown.newBuilder());
						final OutputStream outToServer = m_socket.getOutputStream();
						writeMessageBytes(commandBuilder.build().toByteArray(), outToServer);
						// Give it some time to shutdown before we force it
						try {
							Thread.sleep(1000);
						} catch (final InterruptedException e) {
							//
						}
					} catch (final Throwable t) {
						// continue with killing
					}
					try {
						m_serverSocket.close();
					} catch (final Throwable t) {
					}
					try {
						m_socket.close();
					} catch (final Throwable t) {
					}
					// If the original process was a script we have to kill the actual
					// Python process by PID
					if (m_pid >= 0) {
						try {
							ProcessBuilder pb;
							if (System.getProperty("os.name").toLowerCase().contains("win")) {
								pb = new ProcessBuilder("taskkill", "/F", "/PID", "" + m_pid);
							} else {
								pb = new ProcessBuilder("kill", "-KILL", "" + m_pid);
							}
							pb.start();
						} catch (final IOException e) {
							//
						}
					} else {
						m_process.destroy();
					}
					try {
						Thread.sleep(1000);
					} catch (final InterruptedException e) {
						//
					}
					printStreamToLog();
				}
			}).start();
		}
	}

	private void printStreamToLog() {
		if (m_process != null) {
			try {
				final String out = readAvailableBytesFromStream(m_process.getInputStream());
				final String error = readAvailableBytesFromStream(m_process.getErrorStream());
				if (!out.isEmpty()) {
					LOGGER.info(out);
				}
				if (!error.isEmpty()) {
					LOGGER.error(error);
				}
			} catch (final IOException e) {
				// ignore
			}
		}
	}

	private String readAvailableBytesFromStream(final InputStream stream) throws IOException {
		final byte[] bytes = new byte[1024];
		final StringBuilder sb = new StringBuilder();
		while(stream.available() > 0) {
			final int read = stream.read(bytes);
			sb.append(new String(bytes, 0, read));
		}
		return sb.toString();
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
