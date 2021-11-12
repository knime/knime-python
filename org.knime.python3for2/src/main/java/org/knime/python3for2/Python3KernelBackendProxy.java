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
package org.knime.python3for2;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionMonitor;
import org.knime.python2.extensions.serializationlibrary.SentinelOption;
import org.knime.python2.kernel.PythonKernelOptions;
import org.knime.python3.PythonDataSource;
import org.knime.python3.PythonEntryPoint;
import org.knime.python3.arrow.PythonArrowDataSink;

/**
 * Proxy interface delegating to the Python implementation of the kernel back end.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public interface Python3KernelBackendProxy extends PythonEntryPoint {

    /**
     * Initializes the Python kernel's Java callback that provides it with Java-backed functionality (e.g. resolving
     * KNIME URLs to local file paths).
     *
     * @param callback The kernel's Java callback.
     */
    void initializeJavaCallback(Callback callback);

    /**
     * Implements the functionality required by the part of {@link Python3KernelBackend#setOptions(PythonKernelOptions)}
     * that deals with {@link PythonKernelOptions#getExternalCustomPath()}.
     *
     * @param externalCustomPath The additional path to include in Python's module path.
     */
    void initializeExternalCustomPath(String externalCustomPath);

    /**
     * Implements the functionality required by the part of {@link Python3KernelBackend#setOptions(PythonKernelOptions)}
     * that is responsible for setting Python's working directory to the workflow directory.
     *
     * @param workingDirectoryPath
     */
    void initializeCurrentWorkingDirectory(String workingDirectoryPath);

    /**
     * Implements the functionality required by
     * {@link Python3KernelBackend#putFlowVariables(String, java.util.Collection)}.
     *
     * @param variableName The variable name of the dictionary containing the flow variables in Python.
     * @param flowVariablesMap The flow variables dictionary.
     */
    void putFlowVariablesIntoWorkspace(String variableName, Map<String, Object> flowVariablesMap);

    /**
     * Implements the functionality required by {@link Python3KernelBackend#getFlowVariables(String)}.
     *
     * @param name The variable name of the dictionary containing the flow variables in Python.
     * @return The flow variables dictionary.
     */
    Map<String, Object> getFlowVariablesFromWorkspace(String name);

    /**
     * Implements the functionality required by
     * {@link Python3KernelBackend#putDataTable(String, BufferedDataTable, ExecutionMonitor)} and
     * {@link Python3KernelBackend#putDataTable(String, BufferedDataTable, ExecutionMonitor, int)}.
     *
     * @param variableName The variable name of the table in Python.
     * @param tableDataSource The source providing the table's data.
     * @param numRows The number of rows starting at the beginning of the table that will be taken from
     *            {@code tableDataSource} and made available to Python.
     */
    void putTableIntoWorkspace(String variableName, PythonDataSource tableDataSource, long numRows);

    /**
     * Implements the functionality required by
     * {@link Python3KernelBackend#putDataTable(String, BufferedDataTable, ExecutionMonitor)} and
     * {@link Python3KernelBackend#putDataTable(String, BufferedDataTable, ExecutionMonitor, int)}.
     *
     * @param variableName The variable name of the table in Python.
     * @param tableDataSource The source providing the table's data.
     * @param numRows The number of rows starting at the beginning of the table that will be taken from
     *            {@code tableDataSource} and made available to Python.
     * @param sentinelStrategy Either "min" (corresponds to {@link SentinelOption#MIN_VAL}) or "max"
     *            ({@link SentinelOption#MAX_VAL}) sentinel strategy.
     */
    void putTableIntoWorkspace(String variableName, PythonDataSource tableDataSource, long numRows,
        String sentinelStrategy);

    /**
     * Implements the functionality required by
     * {@link Python3KernelBackend#putDataTable(String, BufferedDataTable, ExecutionMonitor)} and
     * {@link Python3KernelBackend#putDataTable(String, BufferedDataTable, ExecutionMonitor, int)}.
     *
     * @param variableName The variable name of the table in Python.
     * @param tableDataSource The source providing the table's data.
     * @param numRows The number of rows starting at the beginning of the table that will be taken from
     *            {@code tableDataSource} and made available to Python.
     * @param sentinelValue A fixed integer sentinel value (corresponds to {@link SentinelOption#CUSTOM}).
     */
    void putTableIntoWorkspace(String variableName, PythonDataSource tableDataSource, long numRows, int sentinelValue);

    /**
     * Implements the functionality required by
     * {@link Python3KernelBackend#getDataTable(String, org.knime.core.node.ExecutionContext, org.knime.core.node.ExecutionMonitor)}.
     *
     * @param variableName The varaible name of the table in Python.
     * @return The {@link PythonArrowDataSink} that this table was written to.
     */
    PythonArrowDataSink getTableFromWorkspace(String variableName);

    /**
     * Writes the image with the provided name to the provided path.
     *
     * @param imageName name of the variable that holds the image
     * @param path to write the image to
     */
    void writeImageFromWorkspaceToPath(String imageName, String path);

    /**
     * Pickles the object with the provided name into the file at the provided path
     *
     * @param objectName name of the object in the workspace
     * @param path to pickle the object to
     */
    void pickleObjectToFile(final String objectName, final String path);

    /**
     * Returns the type of the object with the provided name.
     *
     * @param objectName name of the object in the workspace
     * @return the type of the object with the provided name
     */
    String getObjectType(final String objectName);

    /**
     * Returns a string representation of the object with the provided name.
     *
     * @param objectName name of the object in the workspace
     * @return the string representation of the object with the provided name
     */
    String getObjectStringRepresentation(final String objectName);

    /**
     * Unpickles the object stored at path into the workflow under the provided name.
     *
     * @param objectName name the object should have in the workspace
     * @param path to unpickle from
     */
    void loadPickledObjectIntoWorkspace(final String objectName, final String path);

    /**
     * Implements the functionality required by {@link Python3KernelBackend#listVariables()}.
     *
     * @return The list of variables. Each variable is represented as a dictionary containing the fields "name", "type",
     *         and "value".
     */
    List<Map<String, String>> listVariablesInWorkspace();

    /**
     * Implements the functionality required by {@link Python3KernelBackend#autoComplete(String, int, int)}.
     *
     * @param sourceCode The source code.
     * @param line The cursor position (line) at which to apply word completion.
     * @param column The cursor position (cursor) at which to apply word completion.
     * @return A list of completion suggestions. Each suggestion is represented as a dictionary containing the fields
     *         "name", "type", and "doc".
     */
    List<Map<String, String>> autoComplete(String sourceCode, int line, int column);

    /**
     * Implements the functionality required by {@link Python3KernelBackend#execute(String)} and
     * {@link Python3KernelBackend#execute(String, org.knime.python2.kernel.PythonCancelable)}.
     *
     * @param sourceCode The Python code to execute.
     * @param sinkCreator
     * @return A list containing the output that the Python process has written to stdout (first element) and stderr
     *         (second element) while executing the given code.
     */
    List<String> executeOnMainThread(String sourceCode);

    /**
     * Implements the functionality required by {@link Python3KernelBackend#executeAsync(String)} and
     * {@link Python3KernelBackend#executeAsync(String, org.knime.python2.kernel.PythonCancelable)}.
     *
     * @param sourceCode The Python code to execute.
     * @param sinkCreator
     * @return A list containing the output that the Python process has written to stdout (first element) and stderr
     *         (second element) while executing the given code.
     */
    List<String> executeOnCurrentThread(String sourceCode);

    /**
     * Provides Java-backed functionality to the Python side.
     * <P>
     * Sonar: the methods of this interface are intended to be called from Python only, so they follow Python's naming
     * conventions. Sonar issues caused by this are suppressed.
     */
    public interface Callback {

        /**
         * Resolves the given KNIME URL to a local path, potentially involving copying a remote file to a local
         * temporary file.
         *
         * @param knimeUrl The {@code knime://} URL to resolve to a local path.
         * @return The resolved local path.
         * @throws IllegalStateException If resolving the URL failed. Wrapped in a {@code Py4JJavaError} on Python side.
         */
        String resolve_knime_url(String knimeUrl); // NOSONAR

        /**
         * @return a new {@link PythonArrowDataSink} that writes to a temporary file
         * @throws IOException if the temporary file for the sink could not be created
         */
        PythonArrowDataSink create_sink() throws IOException; //NOSONAR
    }
}
