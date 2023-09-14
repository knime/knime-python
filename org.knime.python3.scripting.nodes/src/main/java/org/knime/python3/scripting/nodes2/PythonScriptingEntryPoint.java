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
 *   Jul 26, 2022 (benjamin): created
 */
package org.knime.python3.scripting.nodes2;

import java.io.IOException;
import java.util.Map;

import org.knime.python3.PythonDataSource;
import org.knime.python3.PythonEntryPoint;
import org.knime.python3.arrow.PythonArrowDataSink;
import org.knime.python3.arrow.PythonArrowDataSource;

/**
 * The {@link PythonScriptingEntryPoint} defines methods that are implemented in a Python class to run scripts in
 * Python.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public interface PythonScriptingEntryPoint extends PythonEntryPoint {

    /**
     * Set the current working directory to the given directory and insert it at the first index of the path.
     *
     * @param workingDir the current working directory to use
     */
    void setCurrentWorkingDirectory(String workingDir);

    /**
     * Setup input and output variables in knime_io.
     *
     * @param sources the sources for the inputs. Either {@link PythonArrowDataSource} or
     *            {@link PickledObjectDataSource}.
     * @param flowVarSources
     * @param numOutTables the number of output tables of the node
     * @param numOutImages the number of output images of the node
     * @param numOutObjects the number of output objects of the node
     * @param callback a callback for accessing Java functionality
     */
    void setupIO(PythonDataSource[] sources, Map<String, Object> flowVarSources, int numOutTables, int numOutImages,
        int numOutObjects, Callback callback);

    /**
     * Execute the given script in Python.
     *
     * @param script the Python script
     * @param checkOutputs false when subset of lines are executed true when whole script is run
     * @return the Python workspace after the Script was executed
     */
    String execute(String script, boolean checkOutputs);

    /**
     * @return the flow variables defined currently
     */
    Map<String, Object> getFlowVariables();

    /**
     * Close the outputs. After calling this, the output tables cannot be modified anymore.
     *
     * @param checkOutputs if the validity of the outputs should be checked
     */
    void closeOutputs(boolean checkOutputs);

    /**
     * Get the sink of the table at the given output index.
     *
     * @param idx the index of the output table
     * @return the data sink of the table from which the table can be created again
     */
    PythonArrowDataSink getOutputTable(int idx);

    /**
     * Write the output image to the given file.
     *
     * @param idx the index of the image to write to the file
     * @param path the path to write the file to
     */
    void writeOutputImage(int idx, String path);

    /**
     * Write the output object to the given file using pickle.
     *
     * @param idx the index of the object to write to the file
     * @param path the path to write the file to
     */
    void writeOutputObject(int idx, String path);

    /**
     * Get the type of the output object at the given index.
     *
     * @param idx the index of the output object
     * @return the name of the type of the object
     */
    String getOutputObjectType(int idx);

    /**
     * Get the string representation of the output object at the given index.
     *
     * @param idx the index of the output object
     * @return the string representation of the object
     */
    String getOutputObjectStringRepr(int idx);

    /** A callback to call Java functions in Python code */
    interface Callback {

        /**
         * @return a new {@link PythonArrowDataSink} that writes to a temporary file
         * @throws IOException if the temporary file for the sink could not be created
         */
        PythonArrowDataSink create_sink() throws IOException; //NOSONAR

        /**
         * Report that the given text was added to the standard output.
         *
         * @param text the text
         */
        void add_stdout(String text); //NOSONAR

        /**
         * Report that the given text was added to the standard error.
         *
         * @param text the text
         */
        void add_stderr(String text); //NOSONAR

        /**
         * Resolves the given KNIME URL to a local path, potentially involving copying a remote file to a local
         * temporary file.
         *
         * @param knimeUrl The {@code knime://} URL to resolve to a local path.
         * @return The resolved local path.
         * @throws IOException If resolving the URL failed. Wrapped in a {@code Py4JJavaError} on Python side.
         */
        String resolve_knime_url(String knimeUrl) throws IOException; // NOSONAR

        /**
         * @return The temporary directory associated with this workflow
         */
        String get_workflow_temp_dir(); // NOSONAR

        /**
         * @return The local absolute path to the current workflow on disk
         */
        String get_workflow_dir(); // NOSONAR

        /**
         * @param fileStoreKey The string representation of a file store key
         * @return the absolute path of the file on disk
         */
        String file_store_key_to_absolute_path(String fileStoreKey); // NOSONAR

        /**
         * @return A tuple of two strings: the absolute path of the file on disk and the file store key
         * @throws IOException if the file store could not be created
         */
        String[] create_file_store() throws IOException; // NOSONAR
    }

}
