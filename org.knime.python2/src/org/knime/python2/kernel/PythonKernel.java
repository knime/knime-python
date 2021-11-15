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
 *
 * History
 *   Sep 25, 2014 (Patrick Winter): created
 */
package org.knime.python2.kernel;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.python2.PythonCommand;
import org.knime.python2.PythonKernelTester;
import org.knime.python2.PythonKernelTester.PythonKernelTestResult;
import org.knime.python2.PythonModuleSpec;
import org.knime.python2.PythonVersion;
import org.knime.python2.generic.ImageContainer;
import org.knime.python2.port.PickledObject;
import org.knime.python2.port.PickledObjectFile;

/**
 * Provides operations on a Python kernel running in another process.
 *
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public class PythonKernel implements AutoCloseable {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonKernel.class);

    /**
     * Tests if Python can be started using the given Python command and if all given required custom modules are
     * implemented in the Python installation corresponding to the command.
     *
     * @param command The command to test.
     * @param additionalRequiredModules Additional custom modules that must exist in the Python installation in order
     *            for the caller to work properly, must not be {@code null} but may be empty.
     * @throws PythonInstallationTestException The results of the installation test.
     */
    public static void testInstallation(final PythonCommand command,
        final Collection<PythonModuleSpec> additionalRequiredModules) throws PythonInstallationTestException {
        final PythonKernelTestResult testResult = command.getPythonVersion() == PythonVersion.PYTHON3
            ? PythonKernelTester.testPython3Installation(command, additionalRequiredModules, false)
            : PythonKernelTester.testPython2Installation(command, additionalRequiredModules, false);
        if (testResult.hasError()) {
            throw new PythonInstallationTestException(
                "Could not start Python kernel. Error during Python installation test: " + testResult.getErrorLog(),
                testResult);
        }
    }

    private final PythonKernelBackend m_backend;

    private final PythonOutputLogger m_defaultStdoutListener;

    private final PythonOutputLogger m_defaultStderrListener;

    private boolean m_optionsInitialized = false;

    /**
     * Creates a new Python kernel by starting a Python process and connecting to it.
     * <P>
     * Important: Call the {@link #close()} method when this kernel is no longer needed to shut down the Python process
     * in the background.
     *
     * @param backend The back end which provides the actual functionality of the Python kernel.
     */
    public PythonKernel(final PythonKernelBackend backend) {
        m_backend = backend;
        m_defaultStdoutListener = new PythonOutputLogger(LOGGER);
        addStdoutListener(m_defaultStdoutListener);
        m_defaultStderrListener = new PythonOutputLogger(LOGGER);
        addStderrorListener(m_defaultStderrListener);
    }

    /**
     * Creates a new Python kernel by starting a Python process and connecting to it. The instantiated kernel uses the
     * {@link Python2KernelBackend legacy kernel back end}.
     * <P>
     * Important: Call the {@link #close()} method when this kernel is no longer needed to shut down the Python process
     * in the background.
     *
     * @param command The {@link PythonCommand} that is used to launch the Python process.
     * @throws PythonInstallationTestException If the Python environment represented by the given {@link PythonCommand}
     *             is not capable of running the Python kernel (e.g., because it misses essential Python modules or
     *             there are version mismatches).
     * @throws PythonIOException If the kernel could not be set up for any reason. This includes the
     *             {@link PythonInstallationTestException} described above which subclasses {@link PythonIOException}.
     *             Other possible cases include: process creation problems, socket connection problems, exceptions on
     *             Python side during setup, communication errors between the Java and the Python side.
     */
    @SuppressWarnings("resource") // Created back end will be closed with this instance.
    public PythonKernel(final PythonCommand command) throws PythonIOException {
        this(createOldBackend(command));
    }

    private static PythonKernelBackend createOldBackend(final PythonCommand command) throws PythonIOException {
        try {
            return PythonKernelBackendRegistry.getBackend(PythonKernelBackendType.PYTHON2).createBackend(command);
        } catch (final PythonIOException ex) {
            throw ex;
        } catch (final IOException ex) {
            throw new PythonIOException(ex);
        }
    }

    /**
     * Creates a new Python kernel by starting a Python process and connecting to it. The instantiated kernel uses the
     * {@link Python2KernelBackend legacy kernel back end}.
     * <P>
     * Important: Call the {@link #close()} method when this kernel is no longer needed to shut down the Python process
     * in the background.
     *
     * @param kernelOptions The {@link PythonKernelOptions} according to which this kernel instance is configured.
     * @throws PythonInstallationTestException See {@link #PythonKernel(PythonCommand)} and
     *             {@link #setOptions(PythonKernelOptions)}.
     * @throws PythonIOException See {@link #PythonKernel(PythonCommand)} and {@link #setOptions(PythonKernelOptions)}.
     * @deprecated Use {@link #PythonKernel(PythonCommand)} followed by {@link #setOptions(PythonKernelOptions)}
     *             instead. The latter ignores the deprecated Python version and command entries of
     *             {@link PythonKernelOptions}
     */
    @Deprecated
    public PythonKernel(final PythonKernelOptions kernelOptions) throws PythonIOException {
        this(kernelOptions.getUsePython3() //
            ? kernelOptions.getPython3Command() //
            : kernelOptions.getPython2Command());
        setOptions(kernelOptions);
    }

    /**
     * Grants access to the Python kernel's back end.
     *
     * @return The command mechanism.
     * @noreference This method is only exposed for legacy support and testing purposes. It is not intended to be used
     *              by third-party code.
     */
    public PythonKernelBackend getBackend() {
        return m_backend;
    }

    /**
     * @return The {@link PythonCommand} that was used to construct this instance.
     */
    public PythonCommand getPythonCommand() {
        return m_backend.getPythonCommand();
    }

    /**
     * @return The {@link PythonKernelOptions} that have been set via {@link #setOptions(PythonKernelOptions)} or
     *         {@code null} if none have been set.
     */
    public PythonKernelOptions getOptions() {
        return m_backend.getOptions();
    }

    /**
     * Configures this Python kernel instance according to the given options. Note that this method must be called
     * <em>exactly once</em> before this instance can be used. (If using the deprecated constructor
     * {@link PythonKernel#PythonKernel(PythonKernelOptions)}, it must not be called at all.)
     * <P>
     * This method ignores the deprecated Python version and command entries of the given options. If using the new
     * kernel back end, it also ignores the serialization options entry.
     *
     * @param options The {@link PythonKernelOptions} according to which this kernel instance is configured.
     * @throws PythonInstallationTestException If the Python environment represented by the {@link PythonCommand} that
     *             was used to construct this instance does not support the new configuration (e.g., because it lacks
     *             {@link PythonKernelOptions#getAdditionalRequiredModules() required modules}).
     * @throws PythonIOException If the kernel could not be configured for any reason. This includes the
     *             {@link PythonInstallationTestException} described above which subclasses {@link PythonIOException}.
     *             Other possible cases include: if configuring the kernel caused an exception on Python side, or if an
     *             error occurred while communicating with the Python side.
     * @throws IllegalStateException If this method is called although options have already been set.
     */
    public final void setOptions(final PythonKernelOptions options) throws PythonIOException {
        if (m_optionsInitialized) {
            throw new IllegalStateException(
                "Options have already been initialized. Calling this method again is an implementation error.");
        }
        m_backend.setOptions(options);
        m_optionsInitialized = true;
    }

    /**
     * Put the given flow variables into the workspace.
     *
     * The given flow variables will be available as a dict with the given name
     *
     * @param name The name of the dict
     * @param flowVariables The flow variables to put
     * @throws PythonIOException If an error occurred while communicating with the python kernel or while executing the
     *             task
     */
    public void putFlowVariables(final String name, final Collection<FlowVariable> flowVariables)
        throws PythonIOException {
        m_backend.putFlowVariables(name, flowVariables);
    }

    /**
     * Returns the list of defined flow variables
     *
     * @param name Variable name of the flow variable dict in Python
     * @return Collection of flow variables
     * @throws PythonIOException If an error occurred while communicating with the python kernel or while executing the
     *             task
     */
    public Collection<FlowVariable> getFlowVariables(final String name) throws PythonIOException {
        return m_backend.getFlowVariables(name);
    }

    /**
     * Put the given {@link BufferedDataTable} into the workspace while still checking whether the execution has been
     * canceled.
     *
     * The table will be available as a pandas.DataFrame.
     *
     * @param name The name of the table
     * @param table The table
     * @param executionMonitor The monitor that will be updated about progress
     * @param rowLimit The amount of rows that will be transfered
     * @throws PythonIOException If an error occurred while communicating with the python kernel or while executing the
     *             task
     * @throws CanceledExecutionException if canceled. This instance must not be used after a cancellation occurred and
     *             must be {@link #close() closed}.
     */
    public void putDataTable(final String name, final BufferedDataTable table, final ExecutionMonitor executionMonitor,
        final int rowLimit) throws PythonIOException, CanceledExecutionException {
        m_backend.putDataTable(name, table, executionMonitor, rowLimit);
    }

    /**
     * Put the given {@link BufferedDataTable} into the workspace while still checking whether the execution has been
     * canceled.
     *
     * The table will be available as a pandas.DataFrame.
     *
     * @param name The name of the table
     * @param table The table
     * @param executionMonitor The monitor that will be updated about progress
     * @throws PythonIOException If an error occurred while communicating with the python kernel or while executing the
     *             task
     * @throws CanceledExecutionException if canceled. This instance must not be used after a cancellation occurred and
     *             must be {@link #close() closed}.
     */
    public void putDataTable(final String name, final BufferedDataTable table, final ExecutionMonitor executionMonitor)
        throws PythonIOException, CanceledExecutionException {
        m_backend.putDataTable(name, table, executionMonitor);
    }

    /**
     * Get a {@link BufferedDataTable} from the workspace while still checking whether the execution has been canceled.
     *
     * @param name The name of the table to get
     * @param exec The calling node's execution context
     * @return The table
     * @param executionMonitor The monitor that will be updated about progress
     * @throws PythonIOException If an error occurred while communicating with the python kernel or while executing the
     *             task
     * @throws CanceledExecutionException if canceled. This instance must not be used after a cancellation occurred and
     *             must be {@link #close() closed}.
     *
     */
    public BufferedDataTable getDataTable(final String name, final ExecutionContext exec,
        final ExecutionMonitor executionMonitor) throws PythonIOException, CanceledExecutionException {
        return m_backend.getDataTable(name, exec, executionMonitor);
    }

    /**
     * Put a {@link PickledObject} into the python workspace.
     *
     * @param name the name of the variable in the python workspace
     * @param object the {@link PickledObject}
     * @throws PythonIOException If an error occurred while communicating with the python kernel or while executing the
     *             task
     */
    public void putObject(final String name, final PickledObjectFile object) throws PythonIOException {
        m_backend.putObject(name, object);
    }

    /**
     * Put a {@link PickledObject} into the python workspace while still checking whether the execution has been
     * canceled.
     *
     * @param name the name of the variable in the python workspace
     * @param object the {@link PickledObject}
     * @param executionMonitor the {@link ExecutionMonitor} of the calling node
     * @throws PythonIOException If an error occurred while communicating with the python kernel or while executing the
     *             task
     * @throws CanceledExecutionException if canceled. This instance must not be used after a cancellation occurred and
     *             must be {@link #close() closed}.
     */
    public void putObject(final String name, final PickledObjectFile object, final ExecutionMonitor executionMonitor)
        throws PythonIOException, CanceledExecutionException {
        m_backend.putObject(name, object, executionMonitor);
    }

    /**
     * Get a {@link PickledObject} from the python workspace while still checking whether the execution has been
     * canceled.
     *
     * @param name the name of the variable in the python workspace
     * @param executionMonitor the {@link ExecutionMonitor} of the calling KNIME node
     * @return a {@link PickledObject} containing the pickled object representation, the objects type and a string
     *         representation of the object
     * @throws PythonIOException If an error occurred while communicating with the python kernel or while executing the
     *             task
     * @throws CanceledExecutionException if canceled. This instance must not be used after a cancellation occurred and
     *             must be {@link #close() closed}.
     */
    public PickledObjectFile getObject(final String name, final File file, final ExecutionMonitor executionMonitor)
        throws PythonIOException, CanceledExecutionException {
        return m_backend.getObject(name, file, executionMonitor);
    }

    /**
     * Get an image from the workspace.
     *
     * The variable on the python site has to hold a byte string representing an image.
     *
     * @param name The name of the image
     * @return the image
     * @throws PythonIOException If an error occurred while communicating with the python kernel or while executing the
     *             task
     */
    public ImageContainer getImage(final String name) throws PythonIOException {
        return m_backend.getImage(name);
    }

    /**
     * Get an image from the workspace while still checking whether the execution has been canceled.
     *
     * The variable on the Python side has to hold a byte string representing an image.
     *
     * @param name the name of the image
     * @param executionMonitor the monitor that is used to check for cancellation
     * @return the image
     * @throws PythonIOException if an error occurred while communicating with the Python kernel or while executing the
     *             task
     * @throws CanceledExecutionException if canceled. This instance must not be used after a cancellation occurred and
     *             must be {@link #close() closed}.
     */
    public ImageContainer getImage(final String name, final ExecutionMonitor executionMonitor)
        throws PythonIOException, CanceledExecutionException {
        return m_backend.getImage(name, executionMonitor);
    }

    /**
     * Returns the list of all defined variables, functions, classes and loaded modules.
     *
     * Each variable map contains the fields 'name', 'type' and 'value'.
     *
     * @return List of variables currently defined in the workspace
     * @throws PythonIOException If an error occurred while communicating with the python kernel or while executing the
     *             task
     */
    public List<Map<String, String>> listVariables() throws PythonIOException {
        return m_backend.listVariables();
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
     * @throws PythonIOException If an error occurred while communicating with the python kernel or while executing the
     *             task
     */
    public List<Map<String, String>> autoComplete(final String sourceCode, final int line, final int column)
        throws PythonIOException {
        return m_backend.autoComplete(sourceCode, line, column);
    }

    /**
     * Execute the given source code on Python's main thread.
     *
     * @param sourceCode The source code to execute
     * @return Standard console output
     * @throws PythonIOException If an error occurred while communicating with the Python kernel
     */
    public String[] execute(final String sourceCode) throws PythonIOException {
        return m_backend.execute(sourceCode);
    }

    /**
     * Execute the given source code on Python's main thread while still checking whether the execution has been
     * canceled.
     *
     * @param sourceCode The source code to execute
     * @param cancelable The cancelable to check if execution has been canceled
     * @return Standard console output
     * @throws PythonIOException If an error occurred while communicating with the Python kernel or while executing the
     *             task
     * @throws CanceledExecutionException if canceled. This instance must not be used after a cancellation occurred and
     *             must be {@link #close() closed}.
     */
    public String[] execute(final String sourceCode, final PythonCancelable cancelable)
        throws PythonIOException, CanceledExecutionException {
        return m_backend.execute(sourceCode, cancelable);
    }

    /**
     * Execute the given source code concurrently to Python's main thread.
     *
     * @param sourceCode The source code to execute
     * @return Standard console output
     * @throws PythonIOException If an error occurred while communicating with the Python kernel
     */
    public String[] executeAsync(final String sourceCode) throws PythonIOException {
        return m_backend.executeAsync(sourceCode);
    }

    /**
     * Execute the given source code concurrently to Python's main thread while still checking whether the execution has
     * been canceled. Cancellation of an asynchronous should normally not occur but may be required if submitting the
     * task to Python takes some time (e.g., due to load).
     *
     * @param sourceCode The source code to execute
     * @param cancelable The cancelable to check if execution has been canceled
     * @return Standard console output
     * @throws PythonIOException If an error occurred while communicating with the Python kernel or while executing the
     *             task
     * @throws CanceledExecutionException if canceled. This instance must not be used after a cancellation occurred and
     *             must be {@link #close() closed}.
     */
    public String[] executeAsync(final String sourceCode, final PythonCancelable cancelable)
        throws PythonIOException, CanceledExecutionException {
        return m_backend.executeAsync(sourceCode, cancelable);
    }

    /**
     * Shuts down the Python kernel.
     *
     * This shuts down the Python background process and closes the sockets used for communication.
     *
     * @throws PythonKernelCleanupException if an error occurs while cleaning up external resources (e.g., closing
     *             database connections), contains an error message that is suitable to be shown to the user
     */
    @Override
    public void close() throws PythonKernelCleanupException {
        m_backend.close();
    }

    /**
     * Add a listener receiving live messages from Python's stdout stream.
     *
     * @param listener The listener to add.
     */
    public final void addStdoutListener(final PythonOutputListener listener) {
        @SuppressWarnings("resource") // Closed with back end.
        final PythonOutputListeners listeners = m_backend.getOutputListeners();
        listeners.addStdoutListener(listener);
    }

    /**
     * Add a listener receiving live messages from Python's stderror stream.
     *
     * @param listener The listener to add.
     */
    public final void addStderrorListener(final PythonOutputListener listener) {
        @SuppressWarnings("resource") // Closed with back end.
        final PythonOutputListeners listeners = m_backend.getOutputListeners();
        listeners.addStderrorListener(listener);
    }

    /**
     * Remove a listener receiving live messages from Python's stdout stream.
     *
     * @param listener The listener to remove.
     */
    public void removeStdoutListener(final PythonOutputListener listener) {
        @SuppressWarnings("resource") // Closed with back end.
        final PythonOutputListeners listeners = m_backend.getOutputListeners();
        listeners.removeStdoutListener(listener);
    }

    /**
     * Remove a listener receiving live messages from Python's stderror stream.
     *
     * @param listener The listener to remove.
     */
    public void removeStderrorListener(final PythonOutputListener listener) {
        @SuppressWarnings("resource") // Closed with back end.
        final PythonOutputListeners listeners = m_backend.getOutputListeners();
        listeners.removeStderrorListener(listener);
    }

    /**
     * @return the default stdout listener which logs to the info log by default
     */
    public PythonOutputListener getDefaultStdoutListener() {
        return m_defaultStdoutListener;
    }

    /**
     * @return the default stderr listener which logs to the info log by default
     */
    public PythonOutputListener getDefaultStderrListener() {
        return m_defaultStderrListener;
    }
}
