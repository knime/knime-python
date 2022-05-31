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
import java.util.Map;

import org.knime.core.data.filestore.FileStore;
import org.knime.python3.arrow.PythonArrowDataSink;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObjectSpec;
import org.knime.python3.views.PythonNodeViewSink;

/**
 * Proxy for a node implemented in Python. This interface is implemented on the Python side.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public interface PythonNodeModelProxy {

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
     * @param inputs
     * @param context The {@link PythonExecutionContext} valid for this execution
     * @return The outputs
     */
    List<PythonPortObject> execute(PythonPortObject[] inputs, PythonExecutionContext context);

    /**
     * Performs the node configuration. Given the input table schemas, provide the schemas of the resulting tables.
     *
     * @param inputs input specs
     * @param context The {@link PythonConfigurationContext} used during configure
     * @return The output specs
     */
    List<PythonPortObjectSpec> configure(PythonPortObjectSpec[] inputs, PythonConfigurationContext context);

    /**
     * Initializes the Python node's Java callback that provides it with Java-backed functionality (e.g. resolving KNIME
     * URLs to local file paths).
     *
     * @param callback The node's Java callback.
     */
    void initializeJavaCallback(Callback callback);

    /**
     * Get a file path with a key, where the key is used to identify the file
     * in a list of {@link FileStore}s generated during node execution via the {@link Callback}.
     *
     * Sonar: Method names follow Python naming convention.
     */
    public static class FileStoreBasedFile {
        private final String m_filePath;

        private final String m_key;

        /**
         * Create a {@link FileStoreBasedFile}
         * @param filePath
         * @param key
         */
        public FileStoreBasedFile(final String filePath, final String key) {
            m_filePath = filePath;
            m_key = key;
        }

        /**
         * @return the key used to identify the file later on
         */
        public String get_key() { // NOSONAR
            return m_key;
        }

        /**
         * @return Path to the file on disk
         */
        public String get_file_path() { // NOSONAR
            return m_filePath;
        }
    }

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

        /**
         * @return a new {@link FileStore} backed file
         * @throws IOException if the {@link FileStore} file could not be created
         */
        FileStoreBasedFile create_filestore_file() throws IOException; // NOSONAR

        /**
         * @return a {@link PythonNodeViewSink} that provides a temporary HTML file
         * @throws IOException if the temporary folder could not be created
         */
        PythonNodeViewSink create_view_sink() throws IOException; // NOSONAR

        /**
         * Pipe Python logging to KNIME's log facilities
         * @param msg The log message
         */
        void log(String msg);

        /**
         * Used to send flow variables from KNIME to Python
         * @return A map of name -> object pairs for the flow variables;
         */
        Map<String, Object> get_flow_variables(); // NOSONAR

        /**
         * Used to send flow variables from Python to KNIME
         * @param flowVariables A map of name -> object pairs for the flow variables;
         */
        void set_flow_variables(Map<String, Object> flowVariables); // NOSONAR

        /**
         * Set a failure of user code.
         *
         * @param message a short message that is shown to the node user
         * @param details a detailed message containing the traceback that is logged in the console
         * @param invalidSettings if the failure occurred because of invalid settings
         */
        void set_failure(String message, String details, boolean invalidSettings);
    }

    /**
     * Execution context provided to Python while executing a node. This can be used to perform progress reporting
     * and/or cancellation checks.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public interface PythonExecutionContext extends PythonConfigurationContext {
        /**
         * Set the current node execution progress
         *
         * @param progress between 0 and 1
         */
        void set_progress(double progress);

        /**
         * Set the current node execution progress with a message
         *
         * @param progress between 0 and 1
         * @param message The message to be shown in the progress monitor
         */
        void set_progress(final double progress, final String message);

        /**
         * @return True if the node has been cancelled. Then the Python code should return as soon as possible
         */
        boolean is_canceled();


    }

    /**
     * Context provided to Python while configuring a node. This can e.g. provide flow variables.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public interface PythonConfigurationContext {

        /**
         * Sets a warning message on the node.
         *
         * @param message warning message
         */
        void set_warning(final String message);//NOSONAR

    }
}
