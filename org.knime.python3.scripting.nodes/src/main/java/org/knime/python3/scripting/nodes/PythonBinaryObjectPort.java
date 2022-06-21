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
 *   Jun 20, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.scripting.nodes;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.util.CheckUtils;
import org.knime.python2.PythonModuleSpec;
import org.knime.python2.config.WorkspacePreparer;
import org.knime.python2.kernel.PythonKernel;
import org.knime.python2.ports.InputPort;
import org.knime.python2.ports.OutputPort;
import org.knime.python3.nodes.ports.PythonBinaryBlobFileStorePortObject;
import org.knime.python3.scripting.Python3KernelBackend;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class PythonBinaryObjectPort implements InputPort, OutputPort {

    private final String m_variableName;

    public PythonBinaryObjectPort(final String variableName) {
        m_variableName = variableName;
    }

    @Override
    public PortType getPortType() {
        return PythonBinaryBlobFileStorePortObject.TYPE;
    }

    @Override
    public String getVariableName() {
        return m_variableName;
    }

    @Override
    public double getExecuteProgressWeight() {
        return 0.1;
    }

    @Override
    public Collection<PythonModuleSpec> getRequiredModules() {
        // TODO return dependencies of extension once we can properly define portobjects in Python
        return List.of();
    }

    @Override
    public void configure(final PortObjectSpec inSpec) throws InvalidSettingsException {
        // nothing to configure
    }

    @Override
    public WorkspacePreparer prepareInDialog(final PortObjectSpec inSpec) throws NotConfigurableException {
        return null;
    }

    @Override
    public WorkspacePreparer prepareInDialog(final PortObject inObject) throws NotConfigurableException {
        return k -> castBackend(k).putObject(m_variableName, (PythonBinaryBlobFileStorePortObject)inObject);
    }

    @SuppressWarnings("resource")
    @Override
    public void execute(final PortObject inObject, final PythonKernel kernel, final ExecutionMonitor monitor)
        throws Exception {
        castBackend(kernel).putObject(m_variableName, (PythonBinaryBlobFileStorePortObject)inObject);
    }

    @SuppressWarnings("resource")
    @Override
    public PortObject execute(final PythonKernel kernel, final ExecutionContext exec) throws Exception {
        var fileStore = exec.createFileStore(UUID.randomUUID().toString());
        return castBackend(kernel).getObject(m_variableName, fileStore);
    }

    private static Python3KernelBackend castBackend(final PythonKernel kernel) {
        var backend = kernel.getBackend();
        CheckUtils.checkState(backend instanceof Python3KernelBackend,
            "Unexpected PythonKernelBackend '%s' encountered. Expected the backend to be of type '%s'",
            backend.getClass().getSimpleName(), Python3KernelBackend.class.getSimpleName());
        return (Python3KernelBackend)backend;
    }

}
