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
 *   Nov 18, 2021 (marcel): created
 */
package org.knime.python3.scripting.nodes;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.swing.JButton;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.ui.PlatformUI;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.DataAwareNodeDialogPane;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.util.PathUtils;
import org.knime.python2.config.PythonExecutableSelectionPanel;
import org.knime.python2.config.PythonFixedVersionExecutableSelectionPanel;
import org.knime.python2.config.PythonSourceCodeConfig;
import org.knime.python2.config.PythonSourceCodeOptionsPanel;
import org.knime.python2.config.PythonSourceCodePanel;
import org.knime.python2.generic.VariableNames;
import org.knime.python2.generic.templates.SourceCodeTemplatesPanel;
import org.knime.python2.kernel.PythonIOException;
import org.knime.python2.kernel.PythonKernelBackendRegistry.PythonKernelBackendType;
import org.knime.python2.port.PickledObjectFile;
import org.knime.python2.ports.DataTableInputPort;
import org.knime.python2.ports.InputPort;
import org.knime.python2.ports.PickledObjectInputPort;
import org.knime.python3.scripting.Python3KernelBackend;
import org.knime.python3.scripting.nodes.prefs.Python3ScriptingPreferences;

import com.equo.chromium.swt.Browser;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public class AbstractPythonScriptingNodeDialog extends DataAwareNodeDialogPane {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(AbstractPythonScriptingNodeDialog.class);

    private final InputPort[] m_inPorts;

    private final PythonSourceCodePanel m_scriptPanel;

    private final PythonFixedVersionExecutableSelectionPanel m_executablePanel;

    public AbstractPythonScriptingNodeDialog(final InputPort[] inPorts, final boolean hasView,
        final VariableNames variableNames, final String templateRepositoryId) {
        m_inPorts = inPorts;
        m_executablePanel =
            new PythonFixedVersionExecutableSelectionPanel(this, AbstractPythonScriptingNodeModel.createCommandConfig(),
                () -> Python3ScriptingPreferences.getEnvironmentTypePreference().getName());
        m_scriptPanel = new PythonSourceCodePanel(this, PythonKernelBackendType.PYTHON3, variableNames,
            new PythonSourceCodeOptionsPanel(), m_executablePanel);

        addTab("Script", m_scriptPanel, false);
        addTab(PythonExecutableSelectionPanel.DEFAULT_TAB_NAME, m_executablePanel);
        addTab("Templates", new SourceCodeTemplatesPanel(m_scriptPanel, templateRepositoryId));

        if (hasView) {
            // Tell the kernel that an output view is expected
            m_scriptPanel.registerWorkspacePreparer(
                kernel -> AbstractPythonScriptingNodeModel.setExpectedOutputView(kernel, true));

            // Add the "Preview View" button
            var openViewButton = new JButton("Preview View");
            openViewButton.addActionListener(a -> openView());
            m_scriptPanel.addWorkspaceButton(openViewButton);
        }
    }

    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        loadPanelSettingsAndSetFlowVariables(settings, specs);

        final List<DataTableSpec> inTableSpecs = new ArrayList<>();
        int numPickledObjects = 0;
        for (int i = 0; i < m_inPorts.length; i++) {
            final InputPort inPort = m_inPorts[i];
            final PortObjectSpec inSpec = specs[i];
            if (inPort instanceof DataTableInputPort) {
                inTableSpecs.add((DataTableSpec)inSpec);
            } else if (inPort instanceof PickledObjectInputPort) {
                numPickledObjects++;
            }
        }
        m_scriptPanel.updateData(inTableSpecs.toArray(DataTableSpec[]::new), new BufferedDataTable[inTableSpecs.size()],
            new PickledObjectFile[numPickledObjects]);
    }

    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings, final PortObject[] input)
        throws NotConfigurableException {
        final PortObjectSpec[] specs = Arrays.stream(input) //
            .map(po -> po != null ? po.getSpec() : null) //
            .toArray(PortObjectSpec[]::new);
        loadPanelSettingsAndSetFlowVariables(settings, specs);

        final List<DataTableSpec> inTableSpecs = new ArrayList<>();
        final List<BufferedDataTable> inTables = new ArrayList<>();
        final List<PickledObjectFile> inPickledObjects = new ArrayList<>();
        for (int i = 0; i < m_inPorts.length; i++) {
            final InputPort inPort = m_inPorts[i];
            final PortObject inObject = input[i];
            if (inObject != null) {
                if (inPort instanceof DataTableInputPort) {
                    final BufferedDataTable table = DataTableInputPort.extractWorkspaceObject(inObject);
                    inTableSpecs.add(table.getDataTableSpec());
                    inTables.add(table);
                } else if (inPort instanceof PickledObjectInputPort) {
                    try {
                        inPickledObjects.add(PickledObjectInputPort.extractWorkspaceObject(inObject));
                    } catch (IOException ex) {
                        throw new NotConfigurableException(ex.getMessage(), ex);
                    }
                }
            } else if (inPort instanceof DataTableInputPort) {
                inTableSpecs.add(new DataTableSpec());
                inTables.add(null);
            } else if (inPort instanceof PickledObjectInputPort) {
                inPickledObjects.add(null);
            }
        }
        m_scriptPanel.updateData(inTableSpecs.toArray(DataTableSpec[]::new), inTables.toArray(BufferedDataTable[]::new),
            inPickledObjects.toArray(PickledObjectFile[]::new));
    }

    private void loadPanelSettingsAndSetFlowVariables(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        m_executablePanel.loadSettingsFrom(settings);
        final String script;
        try {
            script = AbstractPythonScriptingNodeModel.loadScriptFrom(settings);
        } catch (final InvalidSettingsException ex) {
            throw new NotConfigurableException(ex.getMessage(), ex);
        }
        final var config = new PythonSourceCodeConfig();
        config.setSourceCode(script);
        m_scriptPanel.loadSettingsFrom(config, specs);

        final Collection<FlowVariable> inFlowVariables =
            getAvailableFlowVariables(Python3KernelBackend.getCompatibleFlowVariableTypes()).values();
        m_scriptPanel.updateFlowVariables(inFlowVariables.toArray(FlowVariable[]::new));
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        final var config = new PythonSourceCodeConfig();
        m_scriptPanel.saveSettingsTo(config);
        AbstractPythonScriptingNodeModel.saveScriptTo(config.getSourceCode(), settings);
        m_executablePanel.saveSettingsTo(settings);
    }

    @Override
    public boolean closeOnESC() {
        return false;
    }

    @Override
    public void onOpen() {
        m_scriptPanel.open();
    }

    @Override
    public void onClose() {
        m_scriptPanel.close();
    }

    @SuppressWarnings("resource") // The kernel and the kernel manager are handled by the scripting editor
    private void openView() {
        final Path html;
        try {
            html = PathUtils.createTempFile("", ".html");
        } catch (IOException ex) {
            var message =
                "The preview could not be opened because creating a temporary file to save the view into failed:\n"
                    + ex.getMessage();
            m_scriptPanel.errorToConsole(message);
            LOGGER.error(message, ex);
            return;
        }
        var kernelManager = m_scriptPanel.getKernelManager();
        if (kernelManager == null) {
            var message = "The preview could not be opened. No view exists because the kernel is not running.";
            m_scriptPanel.errorToConsole(message);
            LOGGER.error(message);
            return;
        }
        try {
            final var kernel = kernelManager.getKernel();

            // Check the outputs first to fail with a helpful error message if the output view is not set
            // NOTE: This will also fail if the node has other outputs that are not set correctly. However, this is the
            // easiest way to get a good error message in the console
            kernel.executeAndCheckOutputs("");

            // Save the view to the temporary file
            AbstractPythonScriptingNodeModel.getPython3Backend(kernel).getOutputView(html, new ExecutionMonitor());
        } catch (PythonIOException ex) {
            var message = "The preview could not be opened:\n" + ex.getShortMessage().orElse(ex.getMessage());
            m_scriptPanel.errorToConsole(message);
            LOGGER.error(message, ex);
            return;
        } catch (CanceledExecutionException ex) {
            // Cannot happen: The temporary execution monitor cannot be canceled
            var message = "The preview could not be opened:\n" + ex.getMessage();
            m_scriptPanel.errorToConsole(message);
            LOGGER.error(message, ex);
            return;
        }

        Display.getDefault().syncExec(() -> openBrowser(html));
    }

    /**
     * Opens the given path in a new browser window for previewing a node view. The file is deleted when the view is
     * closed.
     */
    private static void openBrowser(final Path html) {
        var shell = new Shell(Display.getDefault(), SWT.SHELL_TRIM);
        shell.setText("Preview View");

        // Set the layout for only the browser
        var layout = new GridLayout();
        layout.numColumns = 1;
        shell.setLayout(layout);

        // Open the browser
        var browser = new Browser(shell, SWT.NONE);
        browser.setLayoutData(new GridData(GridData.FILL, GridData.FILL, true, true));
        browser.setMenu(new Menu(browser.getShell()));

        var mainWindowBounds = PlatformUI.getWorkbench().getActiveWorkbenchWindow().getShell().getBounds();
        shell.setSize(1024, 768);
        shell.setLocation( //
            mainWindowBounds.x + (mainWindowBounds.width - shell.getSize().x) / 2, //
            mainWindowBounds.y + (mainWindowBounds.height - shell.getSize().y) / 2 //
        );

        shell.addDisposeListener(e -> {
            shell.dispose();
            browser.dispose();
        });

        shell.open();

        shell.addShellListener(new ShellAdapter() {
            @Override
            public void shellClosed(final ShellEvent e) {
                try {
                    PathUtils.deleteFileIfExists(html);
                } catch (final IOException ex) {
                    LOGGER.warn("Deleting the temporary view preview failed: " + ex.getMessage(), ex);
                }
            }
        });

        browser.setUrl(html.toUri().toString());
    }

}
