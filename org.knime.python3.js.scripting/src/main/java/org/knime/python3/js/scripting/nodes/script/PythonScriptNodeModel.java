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
 *   Jun 30, 2022 (benjamin): created
 */
package org.knime.python3.js.scripting.nodes.script;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

import org.knime.core.data.filestore.internal.IFileStoreHandler;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContext;
import org.knime.python3.PythonCommand;
import org.knime.python3.js.scripting.nodes.script.ConsoleOutputUtils.ConsoleOutputStorage;
import org.knime.scripting.editor.ScriptingService.ConsoleText;

/**
 * The node model of a Python scripting node.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
final class PythonScriptNodeModel extends NodeModel {

    private static final String SEND_LAST_OUTPUT_PREFIX =
        "------------ START - Console output of the last execution ------------\n\n";

    private static final String SEND_LAST_OUTPUT_SUFFIX =
        "\n\n------------- END - Console output of the last execution -------------\n\n";

    private final PythonScriptNodeSettings m_settings;

    private final PythonScriptPortsConfiguration m_ports;

    private ConsoleOutputStorage m_consoleOutputStorage;

    PythonScriptNodeModel(final PortsConfiguration portsConfiguration) {
        super(portsConfiguration.getInputPorts(), portsConfiguration.getOutputPorts());
        m_settings = new PythonScriptNodeSettings();
        m_ports = PythonScriptPortsConfiguration.fromPortsConfiguration(portsConfiguration);
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return null; // NOSONAR
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final PythonCommand pythonCommand =
            ExecutableSelectionUtils.getPythonCommand(m_settings.getExecutableSelection());
        m_consoleOutputStorage = null;
        final var consoleConsumer = ConsoleOutputUtils.createConsoleConsumer();
        try (final var session =
            new PythonScriptingSession(pythonCommand, consoleConsumer, getWriteFileStoreHandler())) {
            exec.setMessage("Setting up inputs...");
            session.setupIO(inObjects, m_ports.getNumOutTables(), m_ports.getNumOutImages(), m_ports.getNumOutObjects(),
                exec.createSubProgress(0.3));
            exec.setProgress(0.3, "Running script...");
            session.execute(m_settings.getScript());
            exec.setProgress(0.7, "Processing output...");
            return session.getOutputs(exec.createSubExecutionContext(0.3));
        } finally {
            m_consoleOutputStorage = consoleConsumer.finish();
        }

        // TODO we probably need something like the kernelShutdownTracker in AbstractPythonScriptingNode
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        // Nothing to validate
        // Add a validate method to the m_settings object for validation
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadValidatedSettingsFrom(settings);
    }

    @Override
    protected void reset() {
        if (m_consoleOutputStorage == null) {
            return; // Nothing to do
        }

        m_consoleOutputStorage.close();
        m_consoleOutputStorage = null;
    }

    void sendLastConsoleOutputs(final Consumer<ConsoleText> consumer) throws IOException {
        if (m_consoleOutputStorage != null) {
            consumer.accept(new ConsoleText(SEND_LAST_OUTPUT_PREFIX, false));
            ConsoleOutputUtils.sendConsoleOutputs(m_consoleOutputStorage, consumer);
            consumer.accept(new ConsoleText(SEND_LAST_OUTPUT_SUFFIX, false));
        }
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        m_consoleOutputStorage = ConsoleOutputUtils.openConsoleOutput(nodeInternDir.toPath());
    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        if (m_consoleOutputStorage != null) {
            final var nodeInternPath = nodeInternDir.toPath();
            ConsoleOutputUtils.saveConsoleOutput(m_consoleOutputStorage, nodeInternPath);
            m_consoleOutputStorage = ConsoleOutputUtils.openConsoleOutput(nodeInternPath);
        }
    }

    private static IWriteFileStoreHandler getWriteFileStoreHandler() {
        final IFileStoreHandler nodeFsHandler = getFileStoreHandler();
        IWriteFileStoreHandler fsHandler = null;
        if (nodeFsHandler instanceof IWriteFileStoreHandler) {
            fsHandler = (IWriteFileStoreHandler)nodeFsHandler;
        } else {
            // This cannot happen
            throw new IllegalStateException(
                "A NodeContext should be available during execution of the Python scrpting node.");
        }
        return fsHandler;
    }

    private static IFileStoreHandler getFileStoreHandler() {
        return ((NativeNodeContainer)NodeContext.getContext().getNodeContainer()).getNode().getFileStoreHandler();
    }
}
