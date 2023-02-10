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
package org.knime.python3.js.scripting;

import java.io.IOException;
import java.util.Map;

import org.knime.python3.PythonDataSource;
import org.knime.python3.PythonEntryPoint;
import org.knime.python3.arrow.PythonArrowDataSink;
import org.knime.python3.arrow.PythonArrowDataSource;

/**
 * The {@link PythonJsScriptingEntryPoint} defines methods that are implemented in a Python class to run scripts in
 * Python.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public interface PythonJsScriptingEntryPoint extends PythonEntryPoint {

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
    void setupIO(PythonDataSource[] sources, Map<String, Object> flowVarSources,int numOutTables, int numOutImages, int numOutObjects, Callback callback);

    /**
     * Execute the given script in Python.
     *
     * @param script the Python script
     * @return the Python workspace after the Script was executed
     */
    String execute(String script);

    /**
     * @return
     *     Collection<FlowVariable> getFlowVariable();
     */
    Map<String, Object> getFlowVariable();

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
    public interface Callback {

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
    }

}
