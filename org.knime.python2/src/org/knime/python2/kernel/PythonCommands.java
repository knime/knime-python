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
 * ------------------------------------------------------------------------
 */

package org.knime.python2.kernel;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;

import org.knime.core.node.NodeLogger;
import org.knime.core.util.ThreadUtils;
import org.knime.python2.kernel.messaging.AbstractTaskHandler;
import org.knime.python2.kernel.messaging.DefaultMessage;
import org.knime.python2.kernel.messaging.DefaultMessage.PayloadDecoder;
import org.knime.python2.kernel.messaging.DefaultMessage.PayloadEncoder;
import org.knime.python2.kernel.messaging.DefaultTaskFactory;
import org.knime.python2.kernel.messaging.DefaultTaskFactory.DefaultTask;
import org.knime.python2.kernel.messaging.Message;
import org.knime.python2.kernel.messaging.MessageHandler;
import org.knime.python2.kernel.messaging.MessageHandlerCollection;
import org.knime.python2.kernel.messaging.PythonMessaging;
import org.knime.python2.kernel.messaging.TaskHandler;
import org.knime.python2.util.PythonUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Used for communicating with the Python kernel via commands.
 *
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class PythonCommands implements AutoCloseable {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonCommands.class);

    private static final String PAYLOAD_NAME = "payload_name";

    private final PythonMessaging m_messaging;

    private final PythonExecutionMonitor m_monitor;

    private final ExecutorService m_executor;

    /**
     * @param outToPython output stream used for communication with Python
     * @param inFromPython input stream used for communication with Python
     */
    public PythonCommands(final OutputStream outToPython, final InputStream inFromPython,
        final PythonExecutionMonitor monitor) {
        m_messaging = new PythonMessaging(outToPython, inFromPython, monitor);
        m_monitor = monitor;
        m_executor = ThreadUtils.executorServiceWithContext(
            Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("python-task-%d").build()));
    }

    public MessageHandlerCollection getMessageHandlers() {
        return m_messaging;
    }

    public Message createExecuteCommand(final String sourceCode) {
        final byte[] payload = new PayloadEncoder().putString(sourceCode).get();
        return new DefaultMessage(m_messaging.createNextMessageId(), "execute", payload, null);
    }

    public Message createExecuteAsyncCommand(final String sourceCode) {
        final byte[] payload = new PayloadEncoder().putString(sourceCode).get();
        return new DefaultMessage(m_messaging.createNextMessageId(), "execute_async", payload, null);
    }

    public <T> RunnableFuture<T> createTask(final TaskHandler<T> handler, final Message message) {
        return new DefaultTask<>(message, handler, m_messaging, m_messaging, m_messaging::createNextMessageId,
            m_executor, m_monitor);
    }

    public MessageHandler createTaskFactory(final TaskHandler<?> handler) {
        return new DefaultTaskFactory<>(handler, m_messaging, m_messaging, m_messaging::createNextMessageId, m_executor,
            m_monitor);
    }

    /**
     * @return a runnable future that returns the Python kernel's process id
     */
    public synchronized Future<Integer> getPid() {
        return createTask(new IntReturningTaskHandler(),
            new DefaultMessage(m_messaging.createNextMessageId(), "getpid", null, null));
    }

    /**
     * Creates a runnable future that puts some serialized flow variables into the Python workspace. The flow variables
     * should be serialized using the currently active serialization library.
     *
     * @param name the variable name of the flow variables dictionary in the Python workspace
     * @param variables the serialized variables table as byte array
     * @return a runnable future that puts the flow variables into the Python workspace
     */
    public synchronized Future<Void> putFlowVariables(final String name, final byte[] variables) {
        final byte[] payload = new PayloadEncoder().putBytes(variables).get();
        return createTask(new VoidReturningTaskHandler(), new DefaultMessage(m_messaging.createNextMessageId(),
            "putFlowVariables", payload, ImmutableMap.of(PAYLOAD_NAME, name)));
    }

    /**
     * Creates a runnable future that gets some serialized flow variables from the Python workspace.
     *
     * @param name the variable name of the flow variables dictionary in the Python workspace
     * @return a runnable future that returns the flow variables from the Python workspace
     */
    public synchronized Future<byte[]> getFlowVariables(final String name) {
        final byte[] payload = new PayloadEncoder().putString(name).get();
        return createTask(new ByteArrayReturningTaskHandler(),
            new DefaultMessage(m_messaging.createNextMessageId(), "getFlowVariables", payload, null));
    }

    /**
     * Creates a runnable future that puts a serialized KNIME table into the Python workspace (as pandas.DataFrame). The
     * table should be serialized using the currently active serialization library.
     *
     * @param name the variable name of the table in the Python workspace
     * @param table the serialized KNIME table as byte array
     * @return a runnable future that puts the table into the Python workspace
     */
    public synchronized Future<Void> putTable(final String name, final byte[] table) {
        // FIXME: Avoid array creation. We effectively double the memory requirements of "table".
        final byte[] payload = new PayloadEncoder().putBytes(table).get();
        return createTask(new VoidReturningTaskHandler(), new DefaultMessage(m_messaging.createNextMessageId(),
            "putTable", payload, ImmutableMap.of(PAYLOAD_NAME, name)));
    }

    /**
     * Creates a runnable future that appends a chunk of table rows to a table represented as pandas.DataFrame in the
     * Python workspace. The table chunk should be serialized using the currently active serialization library.
     *
     * @param name the variable name of the table in the Python workspace
     * @param table the serialized table chunk as byte array
     * @return a runnable future that appends the chunk of table rows to the table
     */
    public synchronized Future<Void> appendToTable(final String name, final byte[] table) {
        // FIXME: Avoid array creation. We effectively double the memory requirements of "table".
        final byte[] payload = new PayloadEncoder().putBytes(table).get();
        return createTask(new VoidReturningTaskHandler(), new DefaultMessage(m_messaging.createNextMessageId(),
            "appendToTable", payload, ImmutableMap.of(PAYLOAD_NAME, name)));
    }

    /**
     * Creates a runnable future that gets the size in bytes of a serialized table from the Python workspace.
     *
     * @param name the variable name of the table in the Python workspace
     * @return a runnable future that returns the table's size in bytes
     */
    public synchronized Future<Integer> getTableSize(final String name) {
        final byte[] payload = new PayloadEncoder().putString(name).get();
        return createTask(new IntReturningTaskHandler(),
            new DefaultMessage(m_messaging.createNextMessageId(), "getTableSize", payload, null));
    }

    /**
     * Creates a runnable future that gets a serialized KNIME table from the Python workspace.
     *
     * @param name the variable name of the table in the Python workspace
     * @return a runnable future that returns the serialized table as byte array
     */
    public synchronized Future<byte[]> getTable(final String name) {
        final byte[] payload = new PayloadEncoder().putString(name).get();
        return createTask(new ByteArrayReturningTaskHandler(),
            new DefaultMessage(m_messaging.createNextMessageId(), "getTable", payload, null));
    }

    /**
     * Creates a runnable future that gets a chunk of a serialized KNIME table from the Python workspace.
     *
     * @param name the variable name of the table in the Python workspace
     * @param start the first row of the chunk
     * @param end the last row of the chunk
     * @return a runnable future that returns the serialized table chunk as byte array
     */
    public synchronized Future<byte[]> getTableChunk(final String name, final int start, final int end) {
        final byte[] payload = new PayloadEncoder().putString(name).putInt(start).putInt(end).get();
        return createTask(new ByteArrayReturningTaskHandler(),
            new DefaultMessage(m_messaging.createNextMessageId(), "getTableChunk", payload, null));
    }

    /**
     * Creates a runnable future that puts a Python object into the Python workspace. The object consists of a pickled
     * representation, a type and a string representation.
     *
     * @param name the variable name of the object in the Python workspace
     * @param object the serialized Python object
     * @return a runnable future that puts the serialized Python object in the Python workspace
     */
    public synchronized Future<Void> putObject(final String name, final byte[] object) {
        final byte[] payload = new PayloadEncoder().putBytes(object).get();
        return createTask(new VoidReturningTaskHandler(), new DefaultMessage(m_messaging.createNextMessageId(),
            "putObject", payload, ImmutableMap.of(PAYLOAD_NAME, name)));
    }

    /**
     * Creates a runnable future that gets a Python object from the Python workspace. The object consists of a pickled
     * representation, a type and a string representation.
     *
     * @param name the variable name of the object in the Python workspace
     * @return a runnable future that returns the serialized Python object
     */
    public synchronized Future<byte[]> getObject(final String name) {
        final byte[] payload = new PayloadEncoder().putString(name).get();
        return createTask(new ByteArrayReturningTaskHandler(),
            new DefaultMessage(m_messaging.createNextMessageId(), "getObject", payload, null));
    }

    /**
     * Creates a runnable future that puts information on how to connect to a specific SQL database alongside a query in
     * the Python workspace.
     *
     * @param name the name of the variable in the Python workspace
     * @param sql the serialized table containing the the entries: <tt>driver, jdbcurl, username, password, jars, query,
     *            dbidentifier</tt>
     * @return a runnable future that puts the connection information and query in the Python workspace
     */
    public synchronized Future<Void> putSql(final String name, final byte[] sql) {
        final byte[] payload = new PayloadEncoder().putBytes(sql).get();
        return createTask(new VoidReturningTaskHandler(), new DefaultMessage(m_messaging.createNextMessageId(),
            "putSql", payload, ImmutableMap.of(PAYLOAD_NAME, name)));
    }

    /**
     * Creates a runnable future that gets a SQL query from the Python workspace.
     *
     * @param name the name of the variable in the Python workspace
     * @return a runnable future that returns the SQL query from the Python workspace
     */
    public synchronized Future<String> getSql(final String name) {
        final byte[] payload = new PayloadEncoder().putString(name).get();
        return createTask(new AbstractTaskHandler<String>() {

            @Override
            protected String handleSuccessMessage(final Message response) throws ExecutionException {
                return new PayloadDecoder(response.getPayload()).getNextString();
            }
        }, new DefaultMessage(m_messaging.createNextMessageId(), "getSql", payload, null));
    }

    /**
     * Creates a runnable future that gets an image from the Python workspace.
     *
     * @param name the variable name of the image in the Python workspace
     * @return a runnable future that returns the serialized image
     */
    public synchronized Future<byte[]> getImage(final String name) {
        final byte[] payload = new PayloadEncoder().putString(name).get();
        return createTask(new AbstractTaskHandler<byte[]>() {

            @Override
            protected byte[] handleSuccessMessage(final Message response) throws ExecutionException {
                return response.getPayload().length > 0 //
                    ? new PayloadDecoder(response.getPayload()).getNextBytes() //
                    : null;
            }
        }, new DefaultMessage(m_messaging.createNextMessageId(), "getImage", payload, null));
    }

    /**
     * Creates a runnable future that gets a list of the variable names in the Python workspace.
     *
     * @return a runnable future that returns the serialized list of variable names
     */
    public synchronized Future<byte[]> listVariables() {
        return createTask(new ByteArrayReturningTaskHandler(),
            new DefaultMessage(m_messaging.createNextMessageId(), "listVariables", null, null));
    }

    /**
     * Creates a runnable future that gets if Python supports auto-completion.
     *
     * @return a runnable future that returns if Python supports auto-completion
     */
    public synchronized Future<Boolean> hasAutoComplete() {
        return createTask(new AbstractTaskHandler<Boolean>() {

            @Override
            protected Boolean handleSuccessMessage(final Message response) throws ExecutionException {
                return new PayloadDecoder(response.getPayload()).getNextInt() > 0;
            }
        }, new DefaultMessage(m_messaging.createNextMessageId(), "hasAutoComplete", null, null));
    }

    /**
     * Creates a runnable future that gets a list of auto-completion suggestions for the given source code snippet.
     *
     * @param sourceCode the source code snippet for which the auto-completion should be done
     * @param line the line number in the snippet for which the auto-completion should be done
     * @param column the cursor position in the line
     * @return a runnable future that returns the serialized list of auto-completion suggestions
     */
    public synchronized Future<byte[]> autoComplete(final String sourceCode, final int line, final int column) {
        final byte[] payload = new PayloadEncoder().putString(sourceCode).putInt(line).putInt(column).get();
        return createTask(new ByteArrayReturningTaskHandler(),
            new DefaultMessage(m_messaging.createNextMessageId(), "autoComplete", payload, null));
    }

    /**
     * Creates a runnable future that adds a serializer for an extension type to the Python workspace.
     *
     * @param serializerId the serializer's extension id (in Java)
     * @param typeId the Python extension type identifier
     * @param path the path to the code file containing the serializer function
     * @return a runnable future that adds the serializer to the Python workspace
     */
    public synchronized Future<Void> addSerializer(final String serializerId, final String typeId, final String path) {
        final byte[] payload = new PayloadEncoder().putString(serializerId).putString(typeId).putString(path).get();
        return createTask(new VoidReturningTaskHandler(),
            new DefaultMessage(m_messaging.createNextMessageId(), "addSerializer", payload, null));
    }

    /**
     * Creates a runnable future that adds a deserializer for an extension type to the Python workspace.
     *
     * @param deserializerId the deserializer's extension id (in Java)
     * @param path the path to the code file containing the deserializer function
     * @return a runnable future that adds the deserializer to the Python workspace
     */
    public synchronized Future<Void> addDeserializer(final String deserializerId, final String path) {
        final byte[] payload = new PayloadEncoder().putString(deserializerId).putString(path).get();
        return createTask(new VoidReturningTaskHandler(),
            new DefaultMessage(m_messaging.createNextMessageId(), "addDeserializer", payload, null));
    }

    /**
     * Creates a runnable future that transmits the paths to all custom module directories and make them available via
     * the <code>PYTHONPATH</code>.
     *
     * @param paths a semicolon-separated list of directories
     * @return a runnable future that adds the paths to the <code>PYTHONPATH</code>
     */
    public synchronized Future<Void> addToPythonPath(final String paths) {
        final byte[] payload = new PayloadEncoder().putString(paths).get();
        return createTask(new VoidReturningTaskHandler(),
            new DefaultMessage(m_messaging.createNextMessageId(), "setCustomModulePaths", payload, null));
    }

    /**
     * Creates a runnable future that executes a source code snippet in Python.
     *
     * @param sourceCode the snippet to execute
     * @return a runnable future that executes the snippet and returns output and error/warning messages that were
     *         emitted during execution
     */
    public synchronized Future<String[]> execute(final String sourceCode) {
        return createTask(new AbstractTaskHandler<String[]>() {

            @Override
            protected String[] handleSuccessMessage(final Message response) throws ExecutionException {
                final PayloadDecoder decoder = new PayloadDecoder(response.getPayload());
                final String[] outputs = new String[2];
                outputs[0] = decoder.getNextString();
                outputs[1] = decoder.getNextString();
                return outputs;
            }
        }, createExecuteCommand(sourceCode));
    }

    public synchronized Future<String[]> executeAsync(final String sourceCode) {
        return createTask(new AbstractTaskHandler<String[]>() {

            @Override
            protected String[] handleSuccessMessage(final Message response) throws ExecutionException {
                final PayloadDecoder decoder = new PayloadDecoder(response.getPayload());
                final String[] outputs = new String[2];
                outputs[0] = decoder.getNextString();
                outputs[1] = decoder.getNextString();
                return outputs;
            }
        }, createExecuteAsyncCommand(sourceCode));
    }

    /**
     * Creates a runnable future that resets the Python workspace by emptying all variable definitions.
     *
     * @return a runnable future that resets the Python workspace
     */
    public synchronized Future<Void> reset() {
        return createTask(new VoidReturningTaskHandler(),
            new DefaultMessage(m_messaging.createNextMessageId(), "reset", null, null));
    }

    /**
     * Creates a runnable future that cleans up all registered external resources (e.g., database connections) on Python
     * side.
     *
     * @return a runnable future that cleans up all registered external resources on Python side
     */
    public synchronized RunnableFuture<Void> cleanUp() {
        return createTask(new VoidReturningTaskHandler(),
            new DefaultMessage(m_messaging.createNextMessageId(), "cleanup", null, null));
    }

    public synchronized void start() {
        m_messaging.start();
    }

    /**
     * <b>Implementation note:</b> Please note that this method relies on the fact that all other methods of this class
     * are short running (i.e. simply return runnable futures that perform the actual computations). Changing this
     * behavior without adapting this method may lead to a delayed shutdown when trying to close an instance of this
     * class.
     * <P>
     * Inherited documentation: {@inheritDoc}
     */
    @Override
    public synchronized void close() {
        // Order is intended.
        PythonUtils.Misc.invokeSafely(LOGGER::debug, ExecutorService::shutdownNow, m_executor);
        PythonUtils.Misc.closeSafely(LOGGER::debug, m_messaging);
    }

    PythonMessaging getMessaging() {
        return m_messaging;
    }

    private static class ByteArrayReturningTaskHandler extends AbstractTaskHandler<byte[]> {

        @Override
        protected byte[] handleSuccessMessage(final Message response) throws ExecutionException {
            return new PayloadDecoder(response.getPayload()).getNextBytes();
        }
    }

    private static class IntReturningTaskHandler extends AbstractTaskHandler<Integer> {

        @Override
        protected Integer handleSuccessMessage(final Message response) throws ExecutionException {
            return new PayloadDecoder(response.getPayload()).getNextInt();
        }
    }

    private static class VoidReturningTaskHandler extends AbstractTaskHandler<Void> {

        @Override
        protected Void handleSuccessMessage(final Message response) throws ExecutionException {
            return null;
        }
    }
}
