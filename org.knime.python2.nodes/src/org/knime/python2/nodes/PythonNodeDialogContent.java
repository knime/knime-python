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
 *   Nov 16, 2020 (marcel): created
 */
package org.knime.python2.nodes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.NodeContext;
import org.knime.python2.PythonModuleSpec;
import org.knime.python2.config.PythonCommandFlowVariableModel;
import org.knime.python2.config.PythonSourceCodeConfig;
import org.knime.python2.config.PythonSourceCodeOptionsPanel;
import org.knime.python2.config.PythonSourceCodePanel;
import org.knime.python2.config.WorkspacePreparer;
import org.knime.python2.generic.VariableNames;
import org.knime.python2.generic.templates.SourceCodeTemplate;
import org.knime.python2.generic.templates.SourceCodeTemplateRepository;
import org.knime.python2.generic.templates.SourceCodeTemplatesPanel;
import org.knime.python2.port.PickledObject;
import org.knime.python2.ports.DataTableInputPort;
import org.knime.python2.ports.DatabasePort;
import org.knime.python2.ports.InputPort;
import org.knime.python2.ports.PickledObjectInputPort;

/**
 * Works around multi-inheritance limitations of {@link PythonDataAwareNodeDialog} vs.
 * {@link PythonDataUnawareNodeDialog} by using composition.
 * <P>
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class PythonNodeDialogContent {

    /**
     * Creates a {@link PythonNodeDialogContent} that features the default panels used by the dialogs of the standard
     * Python scripting node.
     *
     * @param dialog The parent dialog.
     * @param inPorts The input ports of the node.
     * @param config The configuration object of the node.
     * @param variableNames The input and output variables in the workspace on Python side.
     * @param templateRepositoryId The unique name of the {@link SourceCodeTemplateRepository repository} containing the
     *            {@link SourceCodeTemplate script templates} of the node.
     * @return The created dialog content.
     */
    public static PythonNodeDialogContent createWithDefaultPanels(final NodeDialogPane dialog,
        final InputPort[] inPorts, final PythonSourceCodeConfig config, final VariableNames variableNames,
        final String templateRepositoryId) {
        final PythonSourceCodePanel scriptPanel = new PythonSourceCodePanel(dialog, variableNames);
        final PythonSourceCodeOptionsPanel optionsPanel = new PythonSourceCodeOptionsPanel(scriptPanel);
        return new PythonNodeDialogContent(dialog, inPorts, config, scriptPanel, optionsPanel, templateRepositoryId);
    }

    private final NodeDialogPane m_dialog;

    private final InputPort[] m_inPorts;

    private final PythonSourceCodeConfig m_config;

    private final PythonCommandFlowVariableModel m_python2CommandFlowVar;

    private final PythonCommandFlowVariableModel m_python3CommandFlowVar;

    private final PythonSourceCodePanel m_scriptPanel;

    private final PythonSourceCodeOptionsPanel m_optionsPanel;

    private final SourceCodeTemplatesPanel m_templatesPanel;

    private final Map<InputPort, WorkspacePreparer> m_dataUnawarePreparers = new HashMap<>(1);

    private final Map<InputPort, WorkspacePreparer> m_dataAwarePreparers = new HashMap<>(1);

    /**
     * @param dialog The parent dialog.
     * @param inPorts The input ports of the node.
     * @param config The configuration object of the node.
     * @param scriptPanel The script panel.
     * @param optionsPanel The options panel.
     * @param templateRepositoryId The unique name of the {@link SourceCodeTemplateRepository repository} containing the
     *            {@link SourceCodeTemplate script templates} of the node.
     */
    public PythonNodeDialogContent(final NodeDialogPane dialog, final InputPort[] inPorts,
        final PythonSourceCodeConfig config, final PythonSourceCodePanel scriptPanel,
        final PythonSourceCodeOptionsPanel optionsPanel, final String templateRepositoryId) {
        m_dialog = dialog;
        m_inPorts = inPorts;
        m_config = config;
        m_python2CommandFlowVar = new PythonCommandFlowVariableModel(dialog, config.getPython2CommandConfig());
        m_python3CommandFlowVar = new PythonCommandFlowVariableModel(dialog, config.getPython3CommandConfig());
        m_scriptPanel = scriptPanel;
        m_optionsPanel = optionsPanel;
        // TODO: ideally, we should filter the offered templates based upon the current port configuration.
        m_templatesPanel = new SourceCodeTemplatesPanel(m_scriptPanel, templateRepositoryId);

        m_python2CommandFlowVar.addCommandChangeListener(c -> m_optionsPanel.updatePython2Command(c.orElse(null)));
        m_python3CommandFlowVar.addCommandChangeListener(c -> m_optionsPanel.updatePython3Command(c.orElse(null)));
    }

    /**
     * @return The script panel.
     */
    public PythonSourceCodePanel getScriptPanel() {
        return m_scriptPanel;
    }

    /**
     * @return The options panel.
     */
    public PythonSourceCodeOptionsPanel getOptionsPanel() {
        return m_optionsPanel;
    }

    /**
     * @return The templates panel.
     */
    public SourceCodeTemplatesPanel getTemplatesPanel() {
        return m_templatesPanel;
    }

    /**
     * Data-unaware settings loading procedure.
     * <P>
     * Loads the settings of the dialog content.
     *
     * @param settings The settings to load into the dialog content.
     * @param specs The input port object specs of the node.
     * @param credentials The credentials provider of the parent dialog.
     * @throws NotConfigurableException If the dialog content cannot be configured.
     */
    public void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs,
        final CredentialsProvider credentials) throws NotConfigurableException {
        m_python2CommandFlowVar.loadSettingsFrom(settings);
        m_python3CommandFlowVar.loadSettingsFrom(settings);
        m_config.loadFromInDialog(settings);
        m_scriptPanel.loadSettingsFrom(m_config, specs);
        m_optionsPanel.loadSettingsFrom(m_config);

        @SuppressWarnings("deprecation")
        final Map<String, FlowVariable> inFlowVariables = m_dialog.getAvailableFlowVariables();
        m_scriptPanel.updateFlowVariables(inFlowVariables.values().toArray(new FlowVariable[inFlowVariables.size()]));

        int numTables = 0;
        int numPickledObjects = 0;
        for (int i = 0; i < m_inPorts.length; i++) {
            final InputPort inPort = m_inPorts[i];
            for (final PythonModuleSpec module : inPort.getRequiredModules()) {
                m_scriptPanel.addAdditionalRequiredModule(module.getName());
            }
            if (inPort instanceof DataTableInputPort) {
                numTables++;
            } else if (inPort instanceof PickledObjectInputPort) {
                numPickledObjects++;
            } else if (inPort instanceof DatabasePort) {
                ((DatabasePort)inPort).setCredentialsProvider(credentials);
            }
            final WorkspacePreparer preparer = inPort.prepareInDialog(specs[i]);
            updatePreparers(inPort, preparer, false);
        }
        m_scriptPanel.updateData(new BufferedDataTable[numTables], new PickledObject[numPickledObjects]);
    }

    /**
     * Data-aware settings loading procedure.
     * <P>
     * Loads the settings of the dialog content.
     *
     * @param settings The settings to load into the dialog content.
     * @param input The input port objects of the node.
     * @param credentials The credentials provider of the parent dialog.
     * @throws NotConfigurableException If the dialog content cannot be configured.
     */
    public void loadSettingsFrom(final NodeSettingsRO settings, final PortObject[] input,
        final CredentialsProvider credentials) throws NotConfigurableException {
        final PortObjectSpec[] inSpecs = new PortObjectSpec[input.length];
        final List<BufferedDataTable> inTables = new ArrayList<>();
        final List<PickledObject> inPickledObjects = new ArrayList<>();
        for (int i = 0; i < m_inPorts.length; i++) {
            final InputPort inPort = m_inPorts[i];
            final PortObject inObject = input[i];
            if (inObject != null) {
                inSpecs[i] = inObject.getSpec();
            }
            if (inPort instanceof DataTableInputPort) {
                inTables.add(DataTableInputPort.extractWorkspaceObject(inObject));
            } else if (inPort instanceof PickledObjectInputPort) {
                try {
                    inPickledObjects.add(PickledObjectInputPort.extractWorkspaceObject(inObject));
                } catch (IOException ex) {
                    throw new NotConfigurableException(ex.getMessage(), ex);
                }
            }
            final WorkspacePreparer preparer = inPort.prepareInDialog(input[i]);
            updatePreparers(inPort, preparer, true);
        }
        loadSettingsFrom(settings, inSpecs, credentials);
        m_scriptPanel.updateData(inTables.toArray(new BufferedDataTable[0]),
            inPickledObjects.toArray(new PickledObject[0]));
    }

    private void updatePreparers(final InputPort inPort, final WorkspacePreparer preparer, final boolean dataAware) {
        final Map<InputPort, WorkspacePreparer> preparers = dataAware //
            ? m_dataAwarePreparers //
            : m_dataUnawarePreparers;
        final WorkspacePreparer oldPreparer = preparers.get(inPort);
        if (oldPreparer != null) {
            m_scriptPanel.unregisterWorkspacePreparer(oldPreparer);
        }
        if (preparer != null) {
            final NodeContext nodeContext = NodeContext.getContext();
            final WorkspacePreparer fullPreparer = kernel -> {
                NodeContext.pushContext(nodeContext);
                try {
                    preparer.prepareWorkspace(kernel);
                    m_scriptPanel.updateVariables();
                } catch (final Exception ex) {
                    m_scriptPanel.errorToConsole(ex.getMessage());
                } finally {
                    NodeContext.removeLastContext();
                }
            };
            preparers.put(inPort, fullPreparer);
            m_scriptPanel.registerWorkspacePreparer(fullPreparer);
        }
    }

    /**
     * Save the settings of the dialog content.
     *
     * @param settings The store to which to save the settings.
     * @throws InvalidSettingsException If writing the settings failed.
     */
    public void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_scriptPanel.saveSettingsTo(m_config);
        m_optionsPanel.saveSettingsTo(m_config);
        m_config.saveTo(settings);
        m_python2CommandFlowVar.saveSettingsTo(settings);
        m_python3CommandFlowVar.saveSettingsTo(settings);
    }

    /**
     * @return Whether the parent dialog should close upon pressing of the escape key.
     */
    public static boolean closeDialogOnESC() {
        return false;
    }

    /**
     * Notifies this instance that the parent content is opening.
     */
    public void onDialogOpen() {
        m_python2CommandFlowVar.onDialogOpen();
        m_python3CommandFlowVar.onDialogOpen();
        m_scriptPanel.open();
    }

    /**
     * Notifies this instance that the parent content is closing.
     */
    public void onDialogClose() {
        m_scriptPanel.close();
    }
}
