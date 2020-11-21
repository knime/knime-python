/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 *
 * History
 *   Sep 25, 2014 (Patrick Winter): created
 */
package org.knime.python2.nodes.script2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.python2.PythonModuleSpec;
import org.knime.python2.kernel.PythonExecutionMonitorCancelable;
import org.knime.python2.kernel.PythonKernel;
import org.knime.python2.kernel.PythonKernelCleanupException;
import org.knime.python2.nodes.PythonNodeModel;
import org.knime.python2.ports.DatabasePort;
import org.knime.python2.ports.InputPort;
import org.knime.python2.ports.OutputPort;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
final class PythonScriptNodeModel2 extends PythonNodeModel<PythonScriptNodeConfig2> {

    private final InputPort[] m_inPorts;

    private final OutputPort[] m_outPorts;

    public PythonScriptNodeModel2(final PortType[] inPortTypes, final InputPort[] inPorts,
        final PortType[] outPortTypes, final OutputPort[] outPorts) {
        super(inPortTypes, outPortTypes);
        m_inPorts = inPorts;
        m_outPorts = outPorts;
        getConfig().setSourceCode(PythonScriptNodeConfig2.getDefaultSourceCode(inPorts, outPorts));
    }

    @Override
    protected PythonScriptNodeConfig2 createConfig() {
        return new PythonScriptNodeConfig2();
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        for (int i = 0; i < m_inPorts.length; i++) {
            m_inPorts[i].configure(inSpecs[i]);
        }
        return null;
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

        final PythonExecutionMonitorCancelable cancelable = new PythonExecutionMonitorCancelable(exec);
        try (final PythonKernel kernel = getNextKernelFromQueue(requiredAdditionalModules, cancelable)) {
            @SuppressWarnings("deprecation")
            final Collection<FlowVariable> inFlowVariables = getAvailableFlowVariables().values();
            kernel.putFlowVariables(PythonScriptNodeConfig2.FLOW_VARIABLES_NAME, inFlowVariables);

            ExecutionMonitor inMonitor = exec;
            if (m_inPorts.length > 0) {
                inMonitor = exec.createSubProgress(inRelativeWeight);
                inMonitor.setMessage("Transferring input data to Python...");
            }
            for (int i = 0; i < m_inPorts.length; i++) {
                final InputPort inPort = m_inPorts[i];
                if (inPort instanceof DatabasePort) {
                    ((DatabasePort)inPort).setCredentialsProvider(getCredentialsProvider());
                }
                final ExecutionMonitor inPortMonitor = inWeight > 0d //
                    ? inMonitor.createSubProgress(inPort.getExecuteProgressWeight() / inWeight) //
                    : inMonitor;
                inPort.execute(inObjects[i], kernel, inPortMonitor);
            }

            double outWeight = 0d;
            for (int i = 0; i < m_outPorts.length; i++) {
                outWeight += m_outPorts[i].getExecuteProgressWeight();
            }
            final double outRelativeWeight = outWeight / m_outPorts.length;

            final ExecutionMonitor scriptExecutionMonitor =
                exec.createSubProgress(1d - inRelativeWeight - outRelativeWeight);
            scriptExecutionMonitor.setMessage("Executing Python script...");
            final String[] output = kernel.execute(getConfig().getSourceCode(), cancelable);
            setExternalOutput(new LinkedList<String>(Arrays.asList(output[0].split("\n"))));
            setExternalErrorOutput(new LinkedList<String>(Arrays.asList(output[1].split("\n"))));
            scriptExecutionMonitor.setProgress(1);

            final Collection<FlowVariable> outFlowVariables =
                kernel.getFlowVariables(PythonScriptNodeConfig2.FLOW_VARIABLES_NAME);
            addNewVariables(outFlowVariables);

            ExecutionContext outExec = exec;
            if (m_outPorts.length > 0) {
                outExec = exec.createSubExecutionContext(outRelativeWeight);
                outExec.setMessage("Retrieving output data from Python...");
            }
            final PortObject[] outObjects = new PortObject[m_outPorts.length];
            for (int i = 0; i < m_outPorts.length; i++) {
                final OutputPort outPort = m_outPorts[i];
                final ExecutionContext outPortExec = outWeight > 0d //
                    ? outExec.createSubExecutionContext(outPort.getExecuteProgressWeight() / outWeight) //
                    : outExec;
                outObjects[i] = outPort.execute(kernel, outPortExec);
            }
            return outObjects;
        } catch (final PythonKernelCleanupException ex) {
            if (Arrays.stream(m_inPorts).anyMatch(DatabasePort.class::isInstance)) {
                throw new PythonKernelCleanupException(
                    ex.getMessage() + "\nDatabase connections that were opened during "
                        + "the run of the Python script may not have been closed properly.",
                    ex);
            } else {
                throw ex;
            }
        }
    }
}
