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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.DataAwareNodeDialogPane;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.VariableTypeRegistry;
import org.knime.python2.config.PythonExecutableSelectionPanel;
import org.knime.python2.config.PythonFixedVersionExecutableSelectionPanel;
import org.knime.python2.config.PythonSourceCodeConfig;
import org.knime.python2.config.PythonSourceCodeOptionsPanel;
import org.knime.python2.config.PythonSourceCodePanel;
import org.knime.python2.generic.VariableNames;
import org.knime.python2.generic.templates.SourceCodeTemplatesPanel;
import org.knime.python2.kernel.PythonKernelBackendRegistry.PythonKernelBackendType;
import org.knime.python2.port.PickledObjectFile;
import org.knime.python2.ports.DataTableInputPort;
import org.knime.python2.ports.InputPort;
import org.knime.python2.ports.PickledObjectInputPort;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public class AbstractPythonScriptingNodeDialog extends DataAwareNodeDialogPane {

    private final InputPort[] m_inPorts;

    private final PythonSourceCodePanel m_scriptPanel;

    private final PythonFixedVersionExecutableSelectionPanel m_executablePanel;

    public AbstractPythonScriptingNodeDialog(final InputPort[] inPorts, final VariableNames variableNames,
        final String templateRepositoryId) {
        m_inPorts = inPorts;
        m_executablePanel = new PythonFixedVersionExecutableSelectionPanel(this,
            AbstractPythonScriptingNodeModel.createCommandConfig());
        m_scriptPanel = new PythonSourceCodePanel(this, PythonKernelBackendType.PYTHON3, variableNames,
            new PythonSourceCodeOptionsPanel(), m_executablePanel);
        addTab("Script", m_scriptPanel, false);
        addTab(PythonExecutableSelectionPanel.DEFAULT_TAB_NAME, m_executablePanel);
        addTab("Templates", new SourceCodeTemplatesPanel(m_scriptPanel, templateRepositoryId));
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
            getAvailableFlowVariables(VariableTypeRegistry.getInstance().getAllTypes()).values();
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
}
