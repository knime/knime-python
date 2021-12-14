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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.knime.base.node.util.exttool.ExtToolOutputNodeModel;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.VariableType;
import org.knime.core.node.workflow.VariableTypeRegistry;
import org.knime.python2.PythonModuleSpec;
import org.knime.python2.PythonVersion;
import org.knime.python2.config.PythonCommandConfig;
import org.knime.python2.kernel.PythonCancelable;
import org.knime.python2.kernel.PythonCanceledExecutionException;
import org.knime.python2.kernel.PythonExecutionMonitorCancelable;
import org.knime.python2.kernel.PythonIOException;
import org.knime.python2.kernel.PythonKernel;
import org.knime.python2.kernel.PythonKernelBackendRegistry.PythonKernelBackendType;
import org.knime.python2.kernel.PythonKernelOptions;
import org.knime.python2.kernel.PythonKernelQueue;
import org.knime.python2.ports.DataTableOutputPort;
import org.knime.python2.ports.ImageOutputPort;
import org.knime.python2.ports.InputPort;
import org.knime.python2.ports.OutputPort;
import org.knime.python2.ports.PickledObjectOutputPort;
import org.knime.python2.ports.Port;
import org.knime.python2.prefs.PythonPreferences;
import org.knime.python3.scripting.Python3KernelBackend;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public abstract class AbstractPythonScriptingNodeModel extends ExtToolOutputNodeModel {

    private static final String CFG_KEY_SCRIPT = "script";

    static PythonCommandConfig createCommandConfig() {
        return new PythonCommandConfig(PythonVersion.PYTHON3, PythonPreferences::getCondaInstallationPath,
            PythonPreferences::getPython3CommandPreference);
    }

    static void saveScriptTo(final String script, final NodeSettingsWO settings) {
        settings.addString(CFG_KEY_SCRIPT, script);
    }

    static String loadScriptFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        return settings.getString(CFG_KEY_SCRIPT);
    }

    private final InputPort[] m_inPorts;

    private final OutputPort[] m_outPorts;

    private String m_script;

    private final PythonCommandConfig m_command = createCommandConfig();

    protected AbstractPythonScriptingNodeModel(final InputPort[] inPorts, final OutputPort[] outPorts,
        final String defaultScript) {
        super(toPortTypes(inPorts), toPortTypes(outPorts));
        m_inPorts = inPorts;
        m_outPorts = outPorts;
        m_script = defaultScript;
    }

    private static final PortType[] toPortTypes(final Port[] ports) {
        return Arrays.stream(ports).map(Port::getPortType).toArray(PortType[]::new);
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        saveScriptTo(m_script, settings);
        m_command.saveSettingsTo(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        loadScriptFrom(settings);
        m_command.loadSettingsFrom(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_script = loadScriptFrom(settings);
        m_command.loadSettingsFrom(settings);
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        for (int i = 0; i < m_inPorts.length; i++) {
            m_inPorts[i].configure(inSpecs[i]);
        }
        return null; // NOSONAR Conforms to KNIME API.
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        double inWeight = 0d;
        final Set<PythonModuleSpec> requiredAdditionalModules = new HashSet<>();
        for (int i = 0; i < m_inPorts.length; i++) {
            final InputPort inPort = m_inPorts[i];
            inWeight += inPort.getExecuteProgressWeight();
            requiredAdditionalModules.addAll(inPort.getRequiredModules());
        }
        double inRelativeWeight = inWeight / m_inPorts.length;

        final var cancelable = new PythonExecutionMonitorCancelable(exec);
        try (final PythonKernel kernel =
            getNextKernelFromQueue(requiredAdditionalModules, Collections.emptySet(), cancelable)) {
            final Collection<FlowVariable> inFlowVariables =
                getAvailableFlowVariables(Python3KernelBackend.getCompatibleFlowVariableTypes()).values();
            kernel.putFlowVariables(null, inFlowVariables);

            ExecutionMonitor inMonitor = exec;
            if (m_inPorts.length > 0) {
                inMonitor = exec.createSubProgress(inRelativeWeight);
                inMonitor.setMessage("Transferring input data to Python...");
            }
            for (int i = 0; i < m_inPorts.length; i++) {
                final InputPort inPort = m_inPorts[i];
                final ExecutionMonitor inPortMonitor = inWeight > 0d //
                    ? inMonitor.createSubProgress(inPort.getExecuteProgressWeight() / inWeight) //
                    : inMonitor;
                inPort.execute(inObjects[i], kernel, inPortMonitor);
            }
            final List<String> outputTableNames = new ArrayList<>(2);
            final List<String> outputImageNames = new ArrayList<>(2);
            final List<String> outputObjectNames = new ArrayList<>(2);
            double outWeight = 0d;
            for (final var outPort : m_outPorts) {
                final String variableName = outPort.getVariableName();
                if (outPort instanceof DataTableOutputPort) {
                    outputTableNames.add(variableName);
                } else if (outPort instanceof ImageOutputPort) {
                    outputImageNames.add(variableName);
                } else if (outPort instanceof PickledObjectOutputPort) {
                    outputObjectNames.add(variableName);
                }
                outWeight += outPort.getExecuteProgressWeight();
            }
            kernel.setExpectedOutputTables(outputTableNames.toArray(String[]::new));
            kernel.setExpectedOutputImages(outputImageNames.toArray(String[]::new));
            kernel.setExpectedOutputObjects(outputObjectNames.toArray(String[]::new));
            final double outRelativeWeight = outWeight / m_outPorts.length;

            final var scriptExecutionMonitor = exec.createSubProgress(1d - inRelativeWeight - outRelativeWeight);
            scriptExecutionMonitor.setMessage("Executing Python script...");
            final String[] output = kernel.executeAndCheckOutputs(m_script, cancelable);
            setExternalOutput(new LinkedList<>(Arrays.asList(output[0].split("\n"))));
            setExternalErrorOutput(new LinkedList<>(Arrays.asList(output[1].split("\n"))));
            scriptExecutionMonitor.setProgress(1);

            final Collection<FlowVariable> outFlowVariables = kernel.getFlowVariables(null);
            addNewFlowVariables(outFlowVariables);

            ExecutionContext outExec = exec;
            if (m_outPorts.length > 0) {
                outExec = exec.createSubExecutionContext(outRelativeWeight);
                outExec.setMessage("Retrieving output data from Python...");
            }
            final var outObjects = new PortObject[m_outPorts.length];
            for (int i = 0; i < m_outPorts.length; i++) {
                final OutputPort outPort = m_outPorts[i];
                final ExecutionContext outPortExec = outWeight > 0d //
                    ? outExec.createSubExecutionContext(outPort.getExecuteProgressWeight() / outWeight) //
                    : outExec;
                outObjects[i] = outPort.execute(kernel, outPortExec);
            }
            return outObjects;
        }
    }

    protected PythonKernel getNextKernelFromQueue(final Set<PythonModuleSpec> requiredAdditionalModules,
        final Set<PythonModuleSpec> optionalAdditionalModules, final PythonCancelable cancelable)
        throws PythonCanceledExecutionException, PythonIOException {
        return PythonKernelQueue.getNextKernel(m_command.getCommand(), PythonKernelBackendType.PYTHON3,
            requiredAdditionalModules, optionalAdditionalModules, new PythonKernelOptions(), cancelable);
    }

    protected void addNewFlowVariables(final Collection<FlowVariable> newVariables) {
        final Map<String, FlowVariable> oldVariables =
            getAvailableFlowVariables(VariableTypeRegistry.getInstance().getAllTypes());
        for (final FlowVariable variable : newVariables) {
            if (!Objects.equals(oldVariables.get(variable.getName()), variable)) {
                pushNewFlowVariable(variable);
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void pushNewFlowVariable(final FlowVariable variable) {
        pushFlowVariable(variable.getName(), (VariableType)variable.getVariableType(),
            variable.getValue(variable.getVariableType()));
    }
}
