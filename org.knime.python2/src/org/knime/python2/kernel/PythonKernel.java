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
package org.knime.python2.kernel;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.lang.ProcessBuilder.Redirect;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.imageio.ImageIO;

import org.apache.batik.dom.svg.SAXSVGDocumentFactory;
import org.apache.batik.transcoder.TranscoderException;
import org.apache.batik.util.XMLResourceDescriptor;
import org.apache.commons.lang.ArrayUtils;
import org.knime.code2.generic.ImageContainer;
import org.knime.code2.generic.ScriptingNodeUtils;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.util.ThreadUtils;
import org.knime.python2.Activator;
import org.knime.python2.PythonKernelTestResult;
import org.knime.python2.PythonPreferencePage;
import org.knime.python2.extensions.serializationlibrary.SerializationLibraryExtensions;
import org.knime.python2.extensions.serializationlibrary.interfaces.Cell;
import org.knime.python2.extensions.serializationlibrary.interfaces.Row;
import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibrary;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableIterator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.BufferedDataTableCreator;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.BufferedDataTableIterator;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.CellImpl;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.KeyValueTableCreator;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.KeyValueTableIterator;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.RowImpl;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.TableSpecImpl;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.TemporaryTableCreator;
import org.knime.python2.port.PickledObject;
import org.knime.python2.typeextension.KnimeToPythonExtension;
import org.knime.python2.typeextension.KnimeToPythonExtensions;
import org.knime.python2.typeextension.PythonToKnimeExtension;
import org.knime.python2.typeextension.PythonToKnimeExtensions;
import org.w3c.dom.svg.SVGDocument;

/**
 * Provides operations on a python kernel running in another process.
 *
 * @author Patrick Winter, KNIME.com, Zurich, Switzerland
 */
public class PythonKernel {

	private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonKernel.class);

	private static final int CHUNK_SIZE = 500000;

	private static final AtomicInteger THREAD_UNIQUE_ID = new AtomicInteger();

	private final Process m_process;
	private final ServerSocket m_serverSocket;
	private Socket m_socket;
	private boolean m_hasAutocomplete = false;
	private int m_pid = -1;
	private boolean m_closed = false;
	private final KnimeToPythonExtensions knimeToPythonExtensions = new KnimeToPythonExtensions();
	private final PythonToKnimeExtensions pythonToKnimeExtensions = new PythonToKnimeExtensions();
	
	private final Commands m_commands;
	private final SerializationLibrary m_serializer;
	private final SerializationLibraryExtensions m_serializationLibraryExtensions;

	/**
	 * Creates a python kernel by starting a python process and connecting to
	 * it.
	 *
	 * Important: Call the {@link #close()} method when this kernel is no longer
	 * needed to shut down the python process in the background
	 *
	 * @throws IOException
	 */
	public PythonKernel(final boolean usePython3) throws IOException {
		final PythonKernelTestResult testResult = usePython3 ? Activator.testPython3Installation() : Activator.retestPython2Installation();
		if (testResult.hasError()) {
			throw new IOException("Could not start python kernel:\n" + testResult.getMessage());
		}
		// Create serialization library instance
		m_serializationLibraryExtensions = new SerializationLibraryExtensions();
		m_serializer = m_serializationLibraryExtensions.getSerializationLibrary(PythonPreferencePage.getSerializerId());
		String serializerPythonPath = SerializationLibraryExtensions.getSerializationLibraryPath(PythonPreferencePage.getSerializerId());
		// Create socket to listen on
		m_serverSocket = new ServerSocket(0);
		final int port = m_serverSocket.getLocalPort();
		m_serverSocket.setSoTimeout(10000);
		final AtomicReference<IOException> exception = new AtomicReference<IOException>();
		final Thread thread = new Thread(new Runnable() {
			@Override
            public void run() {
				try {
					m_socket = m_serverSocket.accept();
				} catch (final IOException e) {
					m_socket = null;
					exception.set(e);
				}
			}
		});
		// Start listening
		thread.start();
		// Get path to python kernel script
		final String scriptPath = Activator.getFile(Activator.PLUGIN_ID, "py/PythonKernel.py").getAbsolutePath();
		// Start python kernel that listens to the given port
		final ProcessBuilder pb = new ProcessBuilder(usePython3 ? Activator.getPython3Command() : Activator.getPython2Command(), scriptPath, "" + port, serializerPythonPath);
		// Add all python modules to PYTHONPATH variable
		String existingPath = pb.environment().get("PYTHONPATH");
		existingPath = existingPath == null ? "" : existingPath;
		final String externalPythonPath = PythonModuleExtensions.getPythonPath();
		if (externalPythonPath != null && !externalPythonPath.isEmpty()) {
			if (existingPath.isEmpty()) {
				existingPath = externalPythonPath;
			} else {
				existingPath = existingPath + File.pathSeparator + externalPythonPath;
			}
		}
		existingPath = existingPath == null ? "" : existingPath + File.pathSeparator;
		pb.environment().put("PYTHONPATH", existingPath);
		
		// TODO remove redirect
		pb.redirectOutput(Redirect.INHERIT);
		pb.redirectError(Redirect.INHERIT);
		
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
			throw new IOException("Could not start python kernel", exception.get());
		}
		m_commands = new Commands(m_socket.getOutputStream(), m_socket.getInputStream());
		// First get PID of Python process
		m_pid = m_commands.getPid();
		try {
			// Check if python kernel supports autocompletion (this depends
			// on the optional module Jedi)
			m_hasAutocomplete = m_commands.hasAutoComplete();
		} catch (final Exception e) {
			//
		}
		// Python serializers
		for (final PythonToKnimeExtension typeExtension : PythonToKnimeExtensions.getExtensions()) {
			m_commands.addSerializer(typeExtension.getId(), typeExtension.getType(), typeExtension.getPythonSerializerPath());
		}
		// Python deserializers
		for (final KnimeToPythonExtension typeExtension : KnimeToPythonExtensions.getExtensions()) {
			m_commands.addDeserializer(typeExtension.getId(), typeExtension.getPythonDeserializerPath());
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
		String[] output = m_commands.execute(sourceCode);
		if (output[0].length() > 0) {
			LOGGER.debug(ScriptingNodeUtils.shortenString(output[0], 1000));
		}
		return output;
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
		byte[] bytes = flowVariablesToBytes(flowVariables);
		m_commands.putFlowVariables(name, bytes);
	}
	
	private byte[] flowVariablesToBytes(final Collection<FlowVariable> flowVariables) {
		Type[] types = new Type[flowVariables.size()];
		String[] columnNames = new String[flowVariables.size()];
		RowImpl row = new RowImpl("0", flowVariables.size());
		int i = 0;
		for (final FlowVariable flowVariable : flowVariables) {
			final String key = flowVariable.getName();
			columnNames[i] = key;
			switch (flowVariable.getType()) {
			case INTEGER:
				types[i] = Type.INTEGER;
				int iValue = flowVariable.getIntValue();
				row.setCell(new CellImpl(iValue), i);
				break;
			case DOUBLE:
				types[i] = Type.DOUBLE;
				double dValue = flowVariable.getDoubleValue();
				row.setCell(new CellImpl(dValue), i);
				break;
			case STRING:
				types[i] = Type.STRING;
				String sValue = flowVariable.getStringValue();
				row.setCell(new CellImpl(sValue), i);
				break;
			default:
				types[i] = Type.STRING;
				String defValue = flowVariable.getValueAsString();
				row.setCell(new CellImpl(defValue), i);
				break;
			}
			i++;
		}
		TableSpec spec = new TableSpecImpl(types, columnNames);
		TableIterator tableIterator = new KeyValueTableIterator(spec, row);
		return m_serializer.tableToBytes(tableIterator);
	}
	
	private Collection<FlowVariable> bytesToFlowVariables(final byte[] bytes) {
		TableSpec spec = m_serializer.tableSpecFromBytes(bytes);
		KeyValueTableCreator tableCreator = new KeyValueTableCreator(spec);
		m_serializer.bytesIntoTable(tableCreator, bytes);
		Set<FlowVariable> flowVariables = new HashSet<FlowVariable>();
		if (tableCreator.getRow() == null) {
			return flowVariables;
		}
		int i = 0;
		for (Cell cell : tableCreator.getRow()) {
			String columnName = tableCreator.getTableSpec().getColumnNames()[i++];
			switch (cell.getColumnType()) {
			case INTEGER:
				if (isValidFlowVariableName(columnName)) {
					flowVariables.add(new FlowVariable(columnName, cell.getIntegerValue()));
				}
				break;
			case DOUBLE:
				if (isValidFlowVariableName(columnName)) {
					flowVariables.add(new FlowVariable(columnName, cell.getDoubleValue()));
				}
				break;
			case STRING:
				if (isValidFlowVariableName(columnName)) {
					flowVariables.add(new FlowVariable(columnName, cell.getStringValue()));
				}
				break;
			default:
				break;
			}
		}
		return flowVariables;
	}
	
	/**
	 * Returns the list of defined flow variables
	 * 
	 * @param name Variable name of the flow variable dict in Python
	 * @return Collection of flow variables
	 * @throws IOException If an error occured
	 */
	public Collection<FlowVariable> getFlowVariables(final String name) throws IOException {
		byte[] bytes = m_commands.getFlowVariables(name);
		Collection<FlowVariable> flowVariables = bytesToFlowVariables(bytes);
		return flowVariables;
	}
	
	private boolean isValidFlowVariableName(final String name) {
		if (name.startsWith(FlowVariable.Scope.Global.getPrefix()) || name.startsWith(FlowVariable.Scope.Local.getPrefix())) {
			// name is reserved
			return false;
		}
		return true;
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
	 *             If an error occurred
	 */
	@SuppressWarnings("deprecation")
	public void putDataTable(final String name, final BufferedDataTable table, final ExecutionMonitor executionMonitor,
			int rowLimit) throws IOException {
		if (table == null) {
			throw new IOException("Table " + name + " is not available.");
		}
		final ExecutionMonitor serializationMonitor = executionMonitor.createSubProgress(0.5);
		final ExecutionMonitor deserializationMonitor = executionMonitor.createSubProgress(0.5);
		CloseableRowIterator iterator = table.iterator();
		int numberRows = Math.min(rowLimit, table.getRowCount());
		int numberChunks = (int)Math.ceil(numberRows / (double)CHUNK_SIZE);
		int rowsDone = 0;
		for (int i = 0; i < numberChunks; i++) {
			int rowsInThisIteration = Math.min(numberRows-rowsDone, CHUNK_SIZE);
			ExecutionMonitor chunkProgress = serializationMonitor.createSubProgress(rowsInThisIteration/(double)numberRows);
			TableIterator tableIterator = new BufferedDataTableIterator(table.getDataTableSpec(), iterator, rowsInThisIteration, chunkProgress);
			byte[] bytes = m_serializer.tableToBytes(tableIterator);
			chunkProgress.setProgress(1);
			rowsDone += rowsInThisIteration;
			serializationMonitor.setProgress(rowsDone / (double)numberRows);
			if (i == 0) {
				m_commands.putTable(name, bytes);
			} else {
				m_commands.appendToTable(name, bytes);
			}
			deserializationMonitor.setProgress(rowsDone / (double)numberRows);
			try {
				executionMonitor.checkCanceled();
			} catch (CanceledExecutionException e) {
				throw new IOException(e.getMessage(), e);
			}
		}
		iterator.close();
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
	 *             If an error occurred
	 */
	@SuppressWarnings("deprecation")
	public void putDataTable(final String name, final BufferedDataTable table, final ExecutionMonitor executionMonitor) throws IOException {
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
		int tableSize = m_commands.getTableSize(name);
		int numberChunks = (int)Math.ceil(tableSize/(double)CHUNK_SIZE);
		BufferedDataTableCreator tableCreator = null;
		for (int i = 0; i < numberChunks; i++) {
			int start = CHUNK_SIZE * i;
			int end = Math.min(tableSize, start + CHUNK_SIZE - 1);
			byte[] bytes = m_commands.getTableChunk(name, start, end);
			serializationMonitor.setProgress((end+1)/(double)tableSize);
			if (tableCreator == null) {
				TableSpec spec = m_serializer.tableSpecFromBytes(bytes);
				tableCreator = new BufferedDataTableCreator(spec, exec, deserializationMonitor, tableSize);
			}
			m_serializer.bytesIntoTable(tableCreator, bytes);
			deserializationMonitor.setProgress((end+1)/(double)tableSize);
		}
		return tableCreator.getTable();
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
		byte[] bytes = m_commands.getImage(name);
		String string = new String(bytes, "UTF-8");
		if (string.startsWith("<?xml")) {
			try {
				return new ImageContainer(stringToSVG(string));
			} catch (final TranscoderException e) {
				throw new IOException(e.getMessage(), e);
			}
		} else {
			return new ImageContainer(ImageIO.read(new ByteArrayInputStream(bytes)));
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

	public PickledObject getObject(final String name, final ExecutionContext exec) throws IOException {
		byte[] bytes = m_commands.getObject(name);
		TableSpec spec = m_serializer.tableSpecFromBytes(bytes);
		KeyValueTableCreator tableCreator = new KeyValueTableCreator(spec);
		m_serializer.bytesIntoTable(tableCreator, bytes);
		Row row = tableCreator.getRow();
		int bytesIndex = spec.findColumn("bytes");
		int typeIndex = spec.findColumn("type");
		int representationIndex = spec.findColumn("representation");
		byte[] objectBytes = ArrayUtils.toPrimitive(row.getCell(bytesIndex).getBytesValue());
		return new PickledObject(objectBytes, row.getCell(typeIndex).getStringValue(), row.getCell(representationIndex).getStringValue());
	}

	public void putObject(final String name, final PickledObject object) throws IOException {
		m_commands.putObject(name, object.getPickledObject());
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
		byte[] bytes = m_commands.listVariables();
		TableSpec spec = m_serializer.tableSpecFromBytes(bytes);
		TemporaryTableCreator tableCreator = new TemporaryTableCreator(spec);
		m_serializer.bytesIntoTable(tableCreator, bytes);
		int nameIndex = spec.findColumn("name");
		int typeIndex = spec.findColumn("type");
		int valueIndex = spec.findColumn("value");
		final List<Map<String, String>> variables = new ArrayList<Map<String, String>>();
		for (Row variable : tableCreator.getRows()) {
			final Map<String, String> map = new HashMap<String, String>();
			map.put("name", variable.getCell(nameIndex).getStringValue());
			map.put("type", variable.getCell(typeIndex).getStringValue());
			map.put("value", variable.getCell(valueIndex).getStringValue());
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
		m_commands.reset();
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
		final List<Map<String, String>> suggestions = new ArrayList<Map<String, String>>();
		if (m_hasAutocomplete) {
			byte[] bytes = m_commands.autoComplete(sourceCode, line, column);
			TableSpec spec = m_serializer.tableSpecFromBytes(bytes);
			TemporaryTableCreator tableCreator = new TemporaryTableCreator(spec);
			m_serializer.bytesIntoTable(tableCreator, bytes);
			int nameIndex = spec.findColumn("name");
			int typeIndex = spec.findColumn("type");
			int docIndex = spec.findColumn("doc");
			for (Row suggestion : tableCreator.getRows()) {
				final Map<String, String> map = new HashMap<String, String>();
				map.put("name", suggestion.getCell(nameIndex).getStringValue());
				map.put("type", suggestion.getCell(typeIndex).getStringValue());
				map.put("doc", suggestion.getCell(docIndex).getStringValue());
				suggestions.add(map);
			}
		}
		return suggestions;
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
					// Give it some time to finish writing into the stream
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
					}
					printStreamToLog();
					// Send shutdown
					try {
						m_commands.shutdown();
						// Give it some time to shutdown before we force it
						try {
							Thread.sleep(10000);
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
					} else if (m_process != null) {
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
}
