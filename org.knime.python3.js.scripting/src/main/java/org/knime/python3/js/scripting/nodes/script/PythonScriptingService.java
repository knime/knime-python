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
 *   Jul 25, 2022 (benjamin): created
 */
package org.knime.python3.js.scripting.nodes.script;

import java.util.ArrayList;
import java.util.List;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.python3.CondaPythonCommand;
import org.knime.python3.PythonCommand;
import org.knime.scripting.editor.ScriptingService;

/**
 * A special {@link ScriptingService} for the Python scripting node.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
final class PythonScriptingService extends ScriptingService {

    // TODO(AP-19357) close the session when the dialog is closed
    // TODO(AP-19332) should the Python session be started immediately? (or whenever the frontend requests it?)
    private PythonScriptingSession m_interactiveSession;

    /** Create a new {@link PythonScriptingService}. */
    @SuppressWarnings("resource") // TODO(AP-19357) fix this
    PythonScriptingService() {
        super(PythonLanguageServer.instance().connect());
        // TODO(AP-19357) stop the language server when the dialog is closed
        // TODO(AP-19338) make language server configurable
    }

    @Override
    public PythonJsonRpcService getJsonRpcService() {
        return new PythonJsonRpcService();
    }

    /**
     * An extension of the {@link org.knime.scripting.editor.ScriptingService.JsonRpcService} that provides additional
     * methods to the frontend of the Python scripting node.
     *
     * NB: Must be public for the JSON-RPC server
     */
    public final class PythonJsonRpcService extends JsonRpcService {

        /**
         * Start the interactive Python session.
         *
         * @throws Exception
         */
        public void startInteractive() throws Exception {
            // TODO(AP-19332) Error handling
            if (m_interactiveSession != null) {
                m_interactiveSession.close();
                m_interactiveSession = null;
            }

            // Start the interactive Python session and setup the IO
            final var workflowControl = getWorkflowControl();

            // TODO(AP-19331) use the command from the configuration
            final PythonCommand pythonCommand = new CondaPythonCommand("/home/benjamin/miniconda3",
                "/home/benjamin/miniconda3/envs/knime-python-scripting-pa7");

            // TODO do we need to do more with the NotInWorkflowWriteFileStoreHandler?
            // TODO report the progress of converting the tables using the ExecutionMonitor?
            final var fileStoreHandler = NotInWorkflowWriteFileStoreHandler.create();
            m_interactiveSession = new PythonScriptingSession(pythonCommand,
                PythonScriptingService.this::addConsoleOutputEvent, fileStoreHandler);
            m_interactiveSession.setupIO(workflowControl.getInputData(), workflowControl.getNC().getNrOutPorts(),
                new ExecutionMonitor());
        }

        /**
         * Run the given script in the running interactive session.
         *
         * @param script the Python script
         * @return the workspace serialized as JSON
         */
        public String runInteractive(final String script) {
            if (m_interactiveSession != null) {
                // TODO(AP-19333) Error handling
                return m_interactiveSession.execute(script);
            } else {
                // TODO(AP-19332)
                throw new IllegalStateException("Please start the session before using it");
            }
        }

        /**
         * List the inputs that are available to the script.
         *
         * @return the list of input object
         */
        public String[][] getInputObjects() {
            // TODO(AP-19337) better javadoc
            // TODO(AP-19337) support other input objects
            // TODO(AP-19337) move to general scripting service?
            final PortObjectSpec[] inputSpec = getWorkflowControl().getInputSpec();
            final List<String[]> columnNames = new ArrayList<>();
            for (var s : inputSpec) {
                if (s instanceof DataTableSpec) {
                    columnNames.add(((DataTableSpec)s).getColumnNames());
                }
            }
            return columnNames.toArray(String[][]::new);
        }
    }
}
