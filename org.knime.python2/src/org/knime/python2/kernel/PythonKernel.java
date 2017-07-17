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
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
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
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.util.ThreadUtils;
import org.knime.python.typeextension.KnimeToPythonExtension;
import org.knime.python.typeextension.KnimeToPythonExtensions;
import org.knime.python.typeextension.PythonToKnimeExtension;
import org.knime.python.typeextension.PythonToKnimeExtensions;
import org.knime.python2.Activator;
import org.knime.python2.PythonKernelTestResult;
import org.knime.python2.PythonPreferencePage;
import org.knime.python2.extensions.serializationlibrary.SentinelOption;
import org.knime.python2.extensions.serializationlibrary.SerializationLibraryExtensions;
import org.knime.python2.extensions.serializationlibrary.interfaces.Cell;
import org.knime.python2.extensions.serializationlibrary.interfaces.Row;
import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibrary;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableChunker;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableCreator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableCreatorFactory;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableIterator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.BufferedDataTableChunker;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.BufferedDataTableCreator;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.CellImpl;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.KeyValueTableCreator;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.KeyValueTableIterator;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.RowImpl;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.TableSpecImpl;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.TemporaryTableCreator;
import org.knime.python2.generic.ImageContainer;
import org.knime.python2.generic.ScriptingNodeUtils;
import org.knime.python2.port.PickledObject;
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

    private final Commands m_commands;

    private final SerializationLibrary m_serializer;

    private final SerializationLibraryExtensions m_serializationLibraryExtensions;

    private final PythonKernelOptions m_kernelOptions;

    /**
     * Creates a python kernel by starting a python process and connecting to it.
     *
     * Important: Call the {@link #close()} method when this kernel is no longer needed to shut down the python process
     * in the background
     *
     * @param kernelOptions all configurable options
     *
     * @throws IOException
     */
    public PythonKernel(final PythonKernelOptions kernelOptions) throws IOException {
        m_kernelOptions = kernelOptions;
        final PythonKernelTestResult testResult = m_kernelOptions.getUsePython3() ? Activator.testPython3Installation()
            : Activator.retestPython2Installation();
        if (testResult.hasError()) {
            throw new IOException("Could not start python kernel:\n" + testResult.getMessage());
        }
        // Create serialization library instance
        m_serializationLibraryExtensions = new SerializationLibraryExtensions();
        m_serializer = m_serializationLibraryExtensions.getSerializationLibrary(getSerializerId());
        final String serializerPythonPath = SerializationLibraryExtensions.getSerializationLibraryPath(getSerializerId());
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
        final ProcessBuilder pb = new ProcessBuilder(
            m_kernelOptions.getUsePython3() ? Activator.getPython3Command() : Activator.getPython2Command(), scriptPath,
                "" + port, serializerPythonPath);
        // Add all python modules to PYTHONPATH variable
        String existingPath = pb.environment().get("PYTHONPATH");
        existingPath = existingPath == null ? "" : existingPath;
        final String externalPythonPath = PythonModuleExtensions.getPythonPath();
        if ((externalPythonPath != null) && !externalPythonPath.isEmpty()) {
            if (existingPath.isEmpty()) {
                existingPath = externalPythonPath;
            } else {
                existingPath = existingPath + File.pathSeparator + externalPythonPath;
            }
        }
        existingPath = existingPath + File.pathSeparator;
        pb.environment().put("PYTHONPATH", existingPath);

        // Uncomment for debugging
        // pb.redirectOutput(Redirect.INHERIT);
        // pb.redirectError(Redirect.INHERIT);

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
            m_commands.addSerializer(typeExtension.getId(), typeExtension.getType(),
                typeExtension.getPythonSerializerPath());
        }
        // Python deserializers
        for (final KnimeToPythonExtension typeExtension : KnimeToPythonExtensions.getExtensions()) {
            m_commands.addDeserializer(typeExtension.getId(), typeExtension.getPythonDeserializerPath());
        }
        //Add sentinel constants
        if (m_kernelOptions.getSentinelOption() == SentinelOption.MAX_VAL) {
            m_commands.execute("INT_SENTINEL = 2**31 - 1; LONG_SENTINEL = 2**63 - 1");
        } else if (m_kernelOptions.getSentinelOption() == SentinelOption.MIN_VAL) {
            m_commands.execute("INT_SENTINEL = -2**31; LONG_SENTINEL = -2**63");
        } else {
            m_commands.execute("INT_SENTINEL = " + m_kernelOptions.getSentinelValue() + "; LONG_SENTINEL = "
                    + m_kernelOptions.getSentinelValue());
        }
    }

    /**
     * Execute the given source code.
     *
     * @param sourceCode The source code to execute
     * @return Standard console output
     * @throws IOException If an error occured
     */
    public String[] execute(final String sourceCode) throws IOException {
        final String[] output = m_commands.execute(sourceCode);
        if (output[0].length() > 0) {
            LOGGER.debug(ScriptingNodeUtils.shortenString(output[0], 1000));
        }
        return output;
    }

    /**
     * Execute the given source code while still checking if the given execution context has been canceled
     *
     * @param sourceCode The source code to execute
     * @param exec The execution context to check if execution has been canceled
     * @return Standard console output
     * @throws Exception If something goes wrong during execution or if execution has been canceled
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
     * @param name The name of the dict
     * @param flowVariables The flow variables to put
     * @throws IOException If an error occured
     */
    public void putFlowVariables(final String name, final Collection<FlowVariable> flowVariables) throws IOException {
        final byte[] bytes = flowVariablesToBytes(flowVariables);
        m_commands.putFlowVariables(name, bytes);
    }

    /**
     * Serialize a collection of flow variables to a {@link Row}.
     *
     * @param flowVariables
     * @return
     */
    private byte[] flowVariablesToBytes(final Collection<FlowVariable> flowVariables) {
        final Type[] types = new Type[flowVariables.size()];
        final String[] columnNames = new String[flowVariables.size()];
        final RowImpl row = new RowImpl("0", flowVariables.size());
        int i = 0;
        for (final FlowVariable flowVariable : flowVariables) {
            final String key = flowVariable.getName();
            columnNames[i] = key;
            switch (flowVariable.getType()) {
                case INTEGER:
                    types[i] = Type.INTEGER;
                    final int iValue = flowVariable.getIntValue();
                    row.setCell(new CellImpl(iValue), i);
                    break;
                case DOUBLE:
                    types[i] = Type.DOUBLE;
                    final double dValue = flowVariable.getDoubleValue();
                    row.setCell(new CellImpl(dValue), i);
                    break;
                case STRING:
                    types[i] = Type.STRING;
                    final String sValue = flowVariable.getStringValue();
                    row.setCell(new CellImpl(sValue), i);
                    break;
                default:
                    types[i] = Type.STRING;
                    final String defValue = flowVariable.getValueAsString();
                    row.setCell(new CellImpl(defValue), i);
                    break;
            }
            i++;
        }
        final TableSpec spec = new TableSpecImpl(types, columnNames, new HashMap<String, String>());
        final TableIterator tableIterator = new KeyValueTableIterator(spec, row);
        return m_serializer.tableToBytes(tableIterator, m_kernelOptions.getSerializationOptions());
    }

    /**
     * Deserialize a collection of flow variables received from the python workspace.
     *
     * @param bytes the serialized representation of the flow variables
     * @return a collection of {@link FlowVariable}s
     */

    private Collection<FlowVariable> bytesToFlowVariables(final byte[] bytes) {
        final TableSpec spec = m_serializer.tableSpecFromBytes(bytes);
        final KeyValueTableCreator tableCreator = new KeyValueTableCreator(spec);
        m_serializer.bytesIntoTable(tableCreator, bytes, m_kernelOptions.getSerializationOptions());
        final Set<FlowVariable> flowVariables = new HashSet<FlowVariable>();
        if (tableCreator.getTable() == null) {
            return flowVariables;
        }
        int i = 0;
        for (final Cell cell : tableCreator.getTable()) {
            final String columnName = tableCreator.getTableSpec().getColumnNames()[i++];
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
        final byte[] bytes = m_commands.getFlowVariables(name);
        final Collection<FlowVariable> flowVariables = bytesToFlowVariables(bytes);
        return flowVariables;
    }

    /**
     * Check if input is a valid flow variable name.
     *
     * @param name a potential flow variable name
     * @return valid
     */
    private boolean isValidFlowVariableName(final String name) {
        if (name.startsWith(FlowVariable.Scope.Global.getPrefix())
                || name.startsWith(FlowVariable.Scope.Local.getPrefix())) {
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
     * @param name The name of the table
     * @param table The table
     * @param executionMonitor The monitor that will be updated about progress
     * @param rowLimit The amount of rows that will be transfered
     * @throws IOException If an error occurred
     */
    public void putDataTable(final String name, final BufferedDataTable table, final ExecutionMonitor executionMonitor,
        final int rowLimit) throws IOException {
        if (table == null) {
            throw new IOException("Table " + name + " is not available.");
        }
        final ExecutionMonitor serializationMonitor = executionMonitor.createSubProgress(0.5);
        final ExecutionMonitor deserializationMonitor = executionMonitor.createSubProgress(0.5);
        final CloseableRowIterator iterator = table.iterator();
        if(table.size() > Integer.MAX_VALUE) {
            throw new IOException("Number of rows exceeds maximum of " + Integer.MAX_VALUE + " rows for input table!");
        }
        final int rowCount = (int) table.size();
        final int numberRows = Math.min(rowLimit, rowCount);
        int numberChunks = (int)Math.ceil(numberRows / (double)CHUNK_SIZE);
        if (numberChunks == 0) {
            numberChunks = 1;
        }
        int rowsDone = 0;
        final TableChunker tableChunker =
                new BufferedDataTableChunker(table.getDataTableSpec(), iterator, rowCount);
        for (int i = 0; i < numberChunks; i++) {
            final int rowsInThisIteration = Math.min(numberRows - rowsDone, CHUNK_SIZE);
            final ExecutionMonitor chunkProgress =
                    serializationMonitor.createSubProgress(rowsInThisIteration / (double)numberRows);
            final TableIterator tableIterator =
                    ((BufferedDataTableChunker)tableChunker).nextChunk(rowsInThisIteration, chunkProgress);
            final byte[] bytes = m_serializer.tableToBytes(tableIterator, m_kernelOptions.getSerializationOptions());
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
            } catch (final CanceledExecutionException e) {
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
     * @param name The name of the table
     * @param table The table
     * @param executionMonitor The monitor that will be updated about progress
     * @throws IOException If an error occurred
     */
    public void putDataTable(final String name, final BufferedDataTable table, final ExecutionMonitor executionMonitor)
            throws IOException {
        if(table.size() > Integer.MAX_VALUE) {
            throw new IOException("Number of rows exceeds maximum of " + Integer.MAX_VALUE + " rows for input table!");
        }
        putDataTable(name, table, executionMonitor, (int)table.size());
    }

    /**
     * Put the data underlying the given {@link TableChunker} into the workspace.
     *
     * The data will be available as a pandas.DataFrame.
     *
     * @param name The name of the table
     * @param tableChunker A {@link TableChunker}
     * @param rowsPerChunk The number of rows to send per chunk
     * @throws IOException If an error occurred
     */
    public void putData(final String name, final TableChunker tableChunker, final int rowsPerChunk) throws IOException {
        final int numberRows = Math.min(rowsPerChunk, tableChunker.getNumberRemainingRows());
        int numberChunks = (int)Math.ceil(numberRows / (double)CHUNK_SIZE);
        if (numberChunks == 0) {
            numberChunks = 1;
        }
        int rowsDone = 0;
        for (int i = 0; i < numberChunks; i++) {
            final int rowsInThisIteration = Math.min(numberRows - rowsDone, CHUNK_SIZE);
            final TableIterator tableIterator = tableChunker.nextChunk(rowsInThisIteration);
            final byte[] bytes = m_serializer.tableToBytes(tableIterator, m_kernelOptions.getSerializationOptions());
            rowsDone += rowsInThisIteration;
            if (i == 0) {
                m_commands.putTable(name, bytes);
            } else {
                m_commands.appendToTable(name, bytes);
            }
        }
    }

    /**
     * Get a {@link BufferedDataTable} from the workspace.
     *
     * @param name The name of the table to get
     * @param exec The calling node's execution context
     * @return The table
     * @param executionMonitor The monitor that will be updated about progress
     * @throws Exception If an error occured
     *
     */
    public BufferedDataTable getDataTable(final String name, final ExecutionContext exec,
        final ExecutionMonitor executionMonitor) throws Exception {
        final ExecutionMonitor serializationMonitor = executionMonitor.createSubProgress(0.5);
        final ExecutionMonitor deserializationMonitor = executionMonitor.createSubProgress(0.5);
        try {
            final int tableSize = m_commands.getTableSize(name);
            int numberChunks = (int)Math.ceil(tableSize / (double)CHUNK_SIZE);
            if (numberChunks == 0) {
                numberChunks = 1;
            }
            BufferedDataTableCreator tableCreator = null;
            for (int i = 0; i < numberChunks; i++) {
                final int start = CHUNK_SIZE * i;
                final int end = Math.min(tableSize, (start + CHUNK_SIZE) - 1);
                final byte[] bytes = m_commands.getTableChunk(name, start, end);
                serializationMonitor.setProgress((end + 1) / (double)tableSize);
                if (tableCreator == null) {
                    final TableSpec spec = m_serializer.tableSpecFromBytes(bytes);
                    tableCreator = new BufferedDataTableCreator(spec, exec, deserializationMonitor, tableSize);
                }
                m_serializer.bytesIntoTable(tableCreator, bytes, m_kernelOptions.getSerializationOptions());
                deserializationMonitor.setProgress((end + 1) / (double)tableSize);
            }
            if (tableCreator != null) {
                return tableCreator.getTable();
            }
            throw new Exception("Invalid serialized table received.");
        } catch (final EOFException ex) {
            throw new Exception("An exception occured while running the python kernel.");
        }
    }

    /**
     * Get an object from the workspace.
     *
     * @param name The name of the object to get
     * @return The object
     * @param tcf A {@link TableCreatorFactory} that can be used to create the requested {@link TableCreator}
     * @throws IOException If an error occured
     */
    public TableCreator<?> getData(final String name, final TableCreatorFactory tcf) throws IOException {
        final int tableSize = m_commands.getTableSize(name);
        int numberChunks = (int)Math.ceil(tableSize / (double)CHUNK_SIZE);
        if (numberChunks == 0) {
            numberChunks = 1;
        }
        TableCreator<?> tableCreator = null;
        for (int i = 0; i < numberChunks; i++) {
            final int start = CHUNK_SIZE * i;
            final int end = Math.min(tableSize, (start + CHUNK_SIZE) - 1);
            final byte[] bytes = m_commands.getTableChunk(name, start, end);
            if (tableCreator == null) {
                final TableSpec spec = m_serializer.tableSpecFromBytes(bytes);
                tableCreator = tcf.createTableCreator(spec, tableSize);
            }
            m_serializer.bytesIntoTable(tableCreator, bytes, m_kernelOptions.getSerializationOptions());
        }
        return tableCreator;
    }

    /**
     * Get an image from the workspace.
     *
     * The variable on the python site has to hold a byte string representing an image.
     *
     * @param name The name of the image
     * @return the image
     * @throws IOException If an error occured
     */
    public ImageContainer getImage(final String name) throws IOException {
        final byte[] bytes = m_commands.getImage(name);
        final String string = new String(bytes, "UTF-8");
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

    /**
     * Convert a string containing the XML content of a svg image to a {@link SVGDocument}.
     *
     * @param svgString a string containing the XML content of a svg image
     * @return a {@link SVGDocument}
     * @throws IOException
     */
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

    /**
     * Get a {@link PickledObject} from the python workspace.
     *
     * @param name the name of the variable in the python workspace
     * @param exec the {@link ExecutionContext} of the calling KNIME node
     * @return a {@link PickledObject} containing the pickled object representation, the objects type and a string
     *         representation of the object
     * @throws IOException
     */
    public PickledObject getObject(final String name, final ExecutionContext exec) throws IOException {
        final byte[] bytes = m_commands.getObject(name);
        final TableSpec spec = m_serializer.tableSpecFromBytes(bytes);
        final KeyValueTableCreator tableCreator = new KeyValueTableCreator(spec);
        m_serializer.bytesIntoTable(tableCreator, bytes, m_kernelOptions.getSerializationOptions());
        final Row row = tableCreator.getTable();
        final int bytesIndex = spec.findColumn("bytes");
        final int typeIndex = spec.findColumn("type");
        final int representationIndex = spec.findColumn("representation");
        final byte[] objectBytes = ArrayUtils.toPrimitive(row.getCell(bytesIndex).getBytesValue());
        return new PickledObject(objectBytes, row.getCell(typeIndex).getStringValue(),
            row.getCell(representationIndex).getStringValue());
    }

    /**
     * Put a {@link PickledObject} into the python workspace.
     *
     * @param name the name of the variable in the python workspace
     * @param object the {@link PickledObject}
     * @throws IOException
     */
    public void putObject(final String name, final PickledObject object) throws IOException {
        m_commands.putObject(name, object.getPickledObject());
    }

    /**
     * Put a {@link PickledObject} into the python workspace in an extra thread and monitor the progress.
     *
     * @param name the name of the variable in the python workspace
     * @param object the {@link PickledObject}
     * @param exec the {@link ExecutionContext} of the calling node
     * @throws Exception if something went wrong
     */
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
        // If there was an exception in the execution thread throw it here
        if (exception.get() != null) {
            throw exception.get();
        }
    }

    /**
     * Returns the list of all defined variables, functions, classes and loaded modules.
     *
     * Each variable map contains the fields 'name', 'type' and 'value'.
     *
     * @return List of variables currently defined in the workspace
     * @throws IOException If an error occured
     */
    public List<Map<String, String>> listVariables() throws IOException {
        final byte[] bytes = m_commands.listVariables();
        final TableSpec spec = m_serializer.tableSpecFromBytes(bytes);
        final TemporaryTableCreator tableCreator = new TemporaryTableCreator(spec);
        m_serializer.bytesIntoTable(tableCreator, bytes, m_kernelOptions.getSerializationOptions());
        final int nameIndex = spec.findColumn("name");
        final int typeIndex = spec.findColumn("type");
        final int valueIndex = spec.findColumn("value");
        final List<Map<String, String>> variables = new ArrayList<Map<String, String>>();
        for (final Row variable : tableCreator.getTable()) {
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
     * @throws IOException If an error occured
     */
    public void resetWorkspace() throws IOException {
        m_commands.reset();
    }

    /**
     * Returns the list of possible auto completions to the given source at the given position.
     *
     * Each auto completion contains the fields 'name', 'type' and 'doc'.
     *
     * @param sourceCode The source code
     * @param line Cursor position (line)
     * @param column Cursor position (column)
     * @return Possible auto completions.
     * @throws IOException If an error occured
     */
    public List<Map<String, String>> autoComplete(final String sourceCode, final int line, final int column)
            throws IOException {
        final List<Map<String, String>> suggestions = new ArrayList<Map<String, String>>();
        if (m_hasAutocomplete) {
            final byte[] bytes = m_commands.autoComplete(sourceCode, line, column);
            final TableSpec spec = m_serializer.tableSpecFromBytes(bytes);
            final TemporaryTableCreator tableCreator = new TemporaryTableCreator(spec);
            m_serializer.bytesIntoTable(tableCreator, bytes, m_kernelOptions.getSerializationOptions());
            final int nameIndex = spec.findColumn("name");
            final int typeIndex = spec.findColumn("type");
            final int docIndex = spec.findColumn("doc");
            for (final Row suggestion : tableCreator.getTable()) {
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
     * This shuts down the python background process and closes the sockets used for communication.
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
                    } catch (final InterruptedException e1) {
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
                    // If the original process was a script we have to kill the
                    // actual
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

    /**
     * Print the contents of the python kernel's stdout and stderror to the KNIME console
     */
    private void printStreamToLog() {
        if (m_process != null) {
            try {
                final String out = readAvailableBytesFromStream(m_process.getInputStream());
                final String error = readAvailableBytesFromStream(m_process.getErrorStream());
                if (!out.isEmpty()) {
                    LOGGER.info(out);
                }
                if (!error.isEmpty()) {
                    //Only show the error message to the user, push full error with stacktrace to log
                    final String[] lines = error.split("\n");
                    final StringBuilder errorMessage = new StringBuilder();
                    for (final String line : lines) {
                        if (!line.startsWith("Traceback") && !line.startsWith(" ")) {
                            errorMessage.append(line + "\n");
                        }
                    }
                    LOGGER.debug("Python error with stacktrace:\n\n" + error);
                    LOGGER.error(errorMessage);
                }
            } catch (final IOException e) {
                // ignore
            }
        }
    }

    /**
     * Read available information from an {@link InputStream}.
     *
     * @param stream an {@link InputStream}
     * @return the incoming bytes as a string
     * @throws IOException
     */
    private String readAvailableBytesFromStream(final InputStream stream) throws IOException {
        final byte[] bytes = new byte[1024];
        final StringBuilder sb = new StringBuilder();
        while (stream.available() > 0) {
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
     * Send a "SQL-Table" to the python workspace that is used to connect to a database.
     *
     * @param name the name of the variable in the python workspace
     * @param conn the database connection to use
     * @param cp a credential provider for username and password
     * @param jars a list of jar files needed for invoking the jdbc driver on pyhton side
     * @throws Exception
     */
    public void putSql(final String name, final DatabaseQueryConnectionSettings conn, final CredentialsProvider cp,
        final Collection<String> jars) throws Exception {
        final Type[] types = new Type[]{Type.STRING, Type.STRING, Type.STRING, Type.STRING, Type.STRING, Type.STRING,
            Type.INTEGER, Type.BOOLEAN, Type.STRING, Type.STRING_LIST};
        final String[] columnNames = new String[]{"driver", "jdbcurl", "username", "password", "query", "dbidentifier",
            "connectiontimeout", "autocommit", "timezone", "jars"};
        final RowImpl row = new RowImpl("0", 10);
        row.setCell(new CellImpl(conn.getDriver()), 0);
        row.setCell(new CellImpl(conn.getJDBCUrl()), 1);
        row.setCell(new CellImpl(conn.getUserName(cp)), 2);
        row.setCell(new CellImpl(conn.getPassword(cp)), 3);
        row.setCell(new CellImpl(conn.getQuery()), 4);
        row.setCell(new CellImpl(conn.getDatabaseIdentifier()), 5);
        row.setCell(new CellImpl(DatabaseConnectionSettings.getDatabaseTimeout()), 6);
        row.setCell(new CellImpl(false), 7);
        row.setCell(new CellImpl(conn.getTimezone()), 8);
        row.setCell(new CellImpl(jars.toArray(new String[jars.size()]), false), 9);
        final TableSpec spec = new TableSpecImpl(types, columnNames, new HashMap<String, String>());
        final TableIterator tableIterator = new KeyValueTableIterator(spec, row);
        final byte[] bytes = m_serializer.tableToBytes(tableIterator, m_kernelOptions.getSerializationOptions());
        try {
            m_commands.putSql(name, bytes);
        } catch (final EOFException ex) {
            throw new Exception("An exception occured while running the python kernel.");
        }
    }

    /**
     * Gets a SQL query from the python workspace.
     *
     * @param name the name of the DBUtil variable in the python workspace
     * @return a SQL query string
     * @throws Exception
     */
    public String getSql(final String name) throws Exception {
        try {
            return m_commands.getSql(name);
        } catch (final EOFException ex) {
            throw new Exception("An exception occured while running the python kernel.");
        }
    }

    /**
     * Get the id of the configured serialization library.
     *
     * @return a serialization library id
     */
    private String getSerializerId() {
        if (m_kernelOptions.getOverrulePreferencePage()) {
            return m_kernelOptions.getSerializerId();
        }
        return PythonPreferencePage.getSerializerId();
    }
}
