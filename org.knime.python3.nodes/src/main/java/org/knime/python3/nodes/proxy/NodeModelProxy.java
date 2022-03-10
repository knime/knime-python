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
 *   Feb 15, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes.proxy;

import java.io.IOException;
import java.util.List;

import org.knime.python3.arrow.PythonArrowDataSink;
import org.knime.python3.arrow.PythonArrowDataSource;

/**
 * Proxy for a node implemented in Python. This interface is implemented on the Python side.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public interface NodeModelProxy {

    // TODO separate methods with long runtime (i.e. execute) from methods with short runtimes (all other methods)


    /**
     * Sets the given parameters.
     *
     * @param parameters as JSON string
     * @param version of KNIME with which the parameters were created (used for backwards compatibility)
     */
    void setParameters(final String parameters, String version);

    /**
     * Validates the given parameters.
     *
     * @param parameters as JSON string
     * @param version of KNIME with which the parameters were created (used for backwards compatibility)
     * @return the validation error or null if there are no errors
     */
    String validateParameters(final String parameters, String version);

    /**
     * Performs the node execution.
     *
     * @param outputObjectPaths
     * @param inputObjectPaths
     * @param sources
     * @return The data sinks populated with data on the Python side
     */
    List<PythonArrowDataSink> execute(PythonArrowDataSource[] sources, String[] inputObjectPaths,
        String[] outputObjectPaths, PythonExecutionContext ctx);

    /**
     * Performs the node configuration. Given the input table schemas, provide the schemas of the resulting tables.
     *
     * @param serializedInSchemas JSON to String serialized version of the input schemas
     * @return The JSON to String serialized version of the output schemas
     */
    List<String> configure(String[] serializedInSchemas);

    /**
     * Initializes the Python node's Java callback that provides it with Java-backed functionality (e.g. resolving
     * KNIME URLs to local file paths).
     *
     * @param callback The node's Java callback.
     */
    void initializeJavaCallback(Callback callback);

    /**
     * Provides Java-backed functionality to the Python side.
     * DUPLICATED FROM Python3KernelBackendProxy
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

    public interface PythonExecutionContext {
        void set_progress(double progress);

        boolean is_cancelled();
    }
}
