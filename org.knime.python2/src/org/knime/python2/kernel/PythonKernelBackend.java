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
 *   Jul 22, 2021 (marcel): created
 */
package org.knime.python2.kernel;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.python2.PythonCommand;
import org.knime.python2.generic.ImageContainer;
import org.knime.python2.port.PickledObject;
import org.knime.python2.port.PickledObjectFile;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public interface PythonKernelBackend extends AutoCloseable {

    /**
     * @return The {@link PythonCommand} that was used to construct this instance.
     */
    PythonCommand getPythonCommand();

    /**
     * @return The collection of listeners to which the back end forwards outputs of the Python process's standard
     *         output and standard error. Clients can add their own listeners to the collection to be notified about any
     *         process outputs.
     */
    PythonOutputListeners getOutputListeners();

    /**
     * @return The {@link PythonKernelOptions} that have been set via {@link #setOptions(PythonKernelOptions)} or
     *         {@code null} if none have been set.
     */
    PythonKernelOptions getOptions();

    /**
     * (Re-)Configures this Python kernel instance according to the given options. Note that this method must be called
     * at least once before this instance can be used. (When using the deprecated constructor
     * {@link PythonKernel#PythonKernel(PythonKernelOptions)}, this does not need to be done.)
     * <P>
     * This method ignores the deprecated Python version and command entries of the given options.
     *
     * @param options The {@link PythonKernelOptions} according to which this kernel instance is (re-)configured.
     * @throws PythonInstallationTestException If the Python environment represented by the {@link PythonCommand} that
     *             was used to construct this instance does not support the new configuration (e.g., because it lacks
     *             {@link PythonKernelOptions#getAdditionalRequiredModules() required modules}).
     * @throws PythonIOException If the kernel could not be configured for any reason. This includes the
     *             {@link PythonInstallationTestException} described above which subclasses {@link PythonIOException}.
     *             Other possible cases include: if configuring the kernel caused an exception on Python side, or if an
     *             error occurred while communicating with the Python side.
     */
    void setOptions(PythonKernelOptions options) throws PythonIOException;

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
    void putFlowVariables(String name, Collection<FlowVariable> flowVariables) throws PythonIOException;

    /**
     * Returns the list of defined flow variables
     *
     * @param name Variable name of the flow variable dict in Python
     * @return Collection of flow variables
     * @throws PythonIOException If an error occurred while communicating with the python kernel or while executing the
     *             task
     */
    Collection<FlowVariable> getFlowVariables(String name) throws PythonIOException;

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
    void putDataTable(String name, BufferedDataTable table, ExecutionMonitor executionMonitor, int rowLimit)
        throws PythonIOException, CanceledExecutionException;

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
    void putDataTable(String name, BufferedDataTable table, ExecutionMonitor executionMonitor)
        throws PythonIOException, CanceledExecutionException;

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
    BufferedDataTable getDataTable(String name, ExecutionContext exec, ExecutionMonitor executionMonitor)
        throws PythonIOException, CanceledExecutionException;

    /**
     * Puts a pickled object into the Python workspace.
     *
     * @param name The name of the variable in the Python workspace to which to assign the unpickled object
     * @param object contains the file the object is pickled to as well as some meta data
     * @throws PythonIOException If an error occurred in Python or while communicating with Python
     */
    void putObject(String name, PickledObjectFile object) throws PythonIOException;

    /**
     * Puts a pickled object into the Python workspace while still checking whether the execution has been canceled.
     *
     * @param name The name of the variable in the Python workspace to which to assign the unpickled object
     * @param pickledObjectFile contains the file that holds the object as well as some meta information
     * @param executionMonitor The {@link ExecutionMonitor} of the calling node
     * @throws PythonIOException If an error occurred in Python or while communicating with Python
     * @throws CanceledExecutionException If canceled. This instance must not be used after a cancellation occurred and
     *             must be {@link #close() closed}.
     */
    void putObject(String name, PickledObjectFile pickledObjectFile, ExecutionMonitor executionMonitor)
        throws PythonIOException, CanceledExecutionException;

    /**
     * Gets a pickled object from the Python workspace while still checking whether the execution has been canceled.
     *
     * @param name The name of the variable in the Python workspace that holds the object
     * @param file The file to which to pickle the object
     * @param executionMonitor The {@link ExecutionMonitor} of the calling node
     * @return A {@link PickledObject} containing a pointer to the given file to which the object was pickled, the
     *         object's Python type, and a string representation of the object
     * @throws PythonIOException If an error occurred in Python or while communicating with Python
     * @throws CanceledExecutionException If canceled. This instance must not be used after a cancellation occurred and
     *             must be {@link #close() closed}.
     */
    PickledObjectFile getObject(String name, File file, ExecutionMonitor executionMonitor)
        throws PythonIOException, CanceledExecutionException;

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
    ImageContainer getImage(String name) throws PythonIOException;

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
    ImageContainer getImage(String name, ExecutionMonitor executionMonitor)
        throws PythonIOException, CanceledExecutionException;

    /**
     * Returns the list of all defined variables, functions, classes and loaded modules.
     *
     * Each variable map contains the fields 'name', 'type' and 'value'.
     *
     * @return List of variables currently defined in the workspace
     * @throws PythonIOException If an error occurred while communicating with the python kernel or while executing the
     *             task
     */
    List<Map<String, String>> listVariables() throws PythonIOException;

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
    List<Map<String, String>> autoComplete(String sourceCode, int line, int column) throws PythonIOException;

    /**
     * Execute the given source code on Python's main thread.
     *
     * @param sourceCode The source code to execute
     * @return Standard console output
     * @throws PythonIOException If an error occurred while communicating with the Python kernel
     */
    String[] execute(String sourceCode) throws PythonIOException;

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
    String[] execute(String sourceCode, PythonCancelable cancelable)
        throws PythonIOException, CanceledExecutionException;

    /**
     * Execute the given source code concurrently to Python's main thread.
     *
     * @param sourceCode The source code to execute
     * @return Standard console output
     * @throws PythonIOException If an error occurred while communicating with the Python kernel
     */
    String[] executeAsync(String sourceCode) throws PythonIOException;

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
    String[] executeAsync(String sourceCode, PythonCancelable cancelable)
        throws PythonIOException, CanceledExecutionException;

    /**
     * Shuts down the Python kernel.
     *
     * This shuts down the Python background process and closes the sockets used for communication.
     *
     * @throws PythonKernelCleanupException if an error occurs while cleaning up external resources (e.g., closing
     *             database connections), contains an error message that is suitable to be shown to the user
     */
    @Override
    void close() throws PythonKernelCleanupException;
}