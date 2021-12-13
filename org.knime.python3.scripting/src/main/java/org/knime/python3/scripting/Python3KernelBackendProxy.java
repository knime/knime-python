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
package org.knime.python3.scripting;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.knime.core.data.v2.ValueFactory;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.python2.kernel.PythonKernelOptions;
import org.knime.python2.port.PickledObjectFile;
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
     * Implements the functionality required by {@link Python3KernelBackend#putFlowVariables(String, Collection)}.
     *
     * @param flowVariables The flow variables dictionary.
     */
    void setFlowVariables(Map<String, Object> flowVariables);

    /**
     * Implements the functionality required by {@link Python3KernelBackend#getFlowVariables(String)}.
     *
     * @return The flow variables dictionary.
     */
    Map<String, Object> getFlowVariables();

    /**
     * Implements the functionality required by
     * {@link Python3KernelBackend#putDataTable(String, BufferedDataTable, ExecutionMonitor)} and
     * {@link Python3KernelBackend#putDataTable(String, BufferedDataTable, ExecutionMonitor, int)}.
     *
     * @param tableIndex The index of the table in the "input tables" group.
     * @param tableDataSource The source providing the table's data. May be {@code null} in which case the corresponding
     *            table on Python side will be {@code None}.
     */
    void setInputTable(int tableIndex, PythonDataSource tableDataSource);

    /**
     * Release the input tables on Python side i.e. close any open file handles, so that the underlying files can
     * be removed from Java if necessary (otherwise Windows won't allow to delete the files).
     */
    void releaseInputTables();

    void setNumExpectedOutputTables(int numOutputTables);

    /**
     * Implements the functionality required by
     * {@link Python3KernelBackend#getDataTable(String, ExecutionContext, ExecutionMonitor)}.
     *
     * @param tableIndex The index of the table in the "output tables" group.
     * @return The {@link PythonArrowDataSink} that this table was written to.
     */
    PythonArrowDataSink getOutputTable(int tableIndex);

    /**
     * Implements the functionality required by {@link Python3KernelBackend#putObject(String, PickledObjectFile)}.
     *
     * @param objectIndex The index of the object in the "input objects" group.
     * @param path The path from which to unpickle the object. May be {@code null} in which case the corresponding
     *            object on Python side will be {@code None}.
     */
    void setInputObject(int objectIndex, String path);

    void setNumExpectedOutputObjects(int numOutputObjects);

    /**
     * Implements parts of the functionality required by
     * {@link Python3KernelBackend#getObject(String, File, ExecutionMonitor)}.
     *
     * @param objectIndex The index of the object in the "output objects" group.
     * @param path The path to which to pickle the object.
     */
    void getOutputObject(int objectIndex, String path);

    /**
     * Implements parts of the functionality required by
     * {@link Python3KernelBackend#getObject(String, File, ExecutionMonitor)}.
     *
     * @param objectIndex The index of the object in the "output objects" group.
     * @return The type of the object.
     */
    String getOutputObjectType(int objectIndex);

    /**
     * Implements parts of the functionality required by
     * {@link Python3KernelBackend#getObject(String, File, ExecutionMonitor)}.
     *
     * @param objectIndex The index of the object in the "output objects" group.
     * @return The string representation of the object.
     */
    String getOutputObjectStringRepresentation(int objectIndex);

    void setNumExpectedOutputImages(int numOutputImages);

    /**
     * Implements the functionality required by {@link Python3KernelBackend#getImage(String)}.
     *
     * @param imageIndex The index of the image in the "output images" group.
     * @param path The path to which to write the image.
     */
    void getOutputImage(int imageIndex, String path);

    /**
     * Implements the functionality required by {@link Python3KernelBackend#listVariables()}.
     *
     * @return The list of variables. Each variable is represented as a dictionary containing the fields "name", "type",
     *         and "value".
     */
    List<Map<String, String>> getVariablesInWorkspace();

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
     * Implements the functionality required by {@link Python3KernelBackend#execute(String, boolean)} and
     * {@link Python3KernelBackend#execute(String, org.knime.python2.kernel.PythonCancelable)}.
     *
     * @param sourceCode The Python code to execute.
     * @param checkOutputs Check whether the sourceCode populates all output ports properly
     * @param sinkCreator
     * @return A list containing the output that the Python process has written to stdout (first element) and stderr
     *         (second element) while executing the given code.
     */
    List<String> executeOnMainThread(String sourceCode, boolean checkOutputs);

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
     * Register a combination of a ValueFactory of KNIME with its equivalent PythonValueFactory.
     *
     * @param pythonModule The module in which the PythonValueFactory is defined
     * @param pythonValueFactoryName The name of the PythonValueFactory
     * @param dataSpec String representation of the {@link DataSpec} created by the {@link ValueFactory}
     * @param dataTraits String representation of the {@link DataTraits} created by the {@link ValueFactory}
     */
    void registerPythonValueFactory(final String pythonModule, final String pythonValueFactoryName,
        final String dataSpec, final String dataTraits);

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
