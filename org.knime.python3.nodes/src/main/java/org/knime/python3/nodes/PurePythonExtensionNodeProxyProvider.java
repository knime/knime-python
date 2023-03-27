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
 *   Feb 28, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes;

import java.io.IOException;
import java.util.OptionalInt;

import org.knime.core.node.port.PortObject;
import org.knime.python3.PythonGateway;
import org.knime.python3.nodes.PurePythonNodeSetFactory.ResolvedPythonExtension;
import org.knime.python3.nodes.ports.PythonTransientConnectionPortObject;
import org.knime.python3.nodes.proxy.CloseableNodeFactoryProxy;
import org.knime.python3.nodes.proxy.NodeDialogProxy;
import org.knime.python3.nodes.proxy.NodeProxyProvider;
import org.knime.python3.nodes.proxy.model.NodeConfigurationProxy;
import org.knime.python3.nodes.proxy.model.NodeExecutionProxy;

/**
 * {@link NodeProxyProvider} for a KNIME extension written purely in Python. There is always one
 * PurePythonExtensionNodeProxyProvider per node instance.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
class PurePythonExtensionNodeProxyProvider implements NodeProxyProvider {

    protected final CloseablePythonNodeProxyFactory m_proxyFactory;

    protected final ResolvedPythonExtension m_extension;

    /**
     * We hold on to the gateway here because the proxy provider is linked to a node, and if that node created a
     * {@link PythonTransientConnectionPortObject} we need to use this very same gateway for all downstream nodes using
     * this port object.
     *
     * When the node is reset, removed from the workflow, or the workflow is closed, it will call
     * {@link PurePythonExtensionNodeProxyProvider#cleanup()} to make sure the gateway gets closed.
     *
     * If this node does not create a connection port, closing the gateway will be handled in {@link DelegatingNodeModel}
     * and the reference here will point to a closed gateway, which doesn't cost much.
     */
    @SuppressWarnings("javadoc")
    protected PythonGateway<KnimeNodeBackend> m_cachedGateway;

    PurePythonExtensionNodeProxyProvider(final ResolvedPythonExtension extension, final String nodeId) {
        m_extension = extension;
        m_proxyFactory = new CloseablePythonNodeProxyFactory(extension, nodeId);
    }

    @Override
    public NodeConfigurationProxy getConfigurationProxy() {
        return createPythonNode();
    }

    @Override
    public NodeExecutionProxy getExecutionProxy(final PortObject[] inputPorts) {
        OptionalInt requiredPid = getRequiredPid(inputPorts);
        return createPythonNode(requiredPid);
    }

    private static OptionalInt getRequiredPid(final PortObject[] inputPorts) {
        var pid = OptionalInt.empty();
        for (var inputPort : inputPorts) {
            if (inputPort instanceof PythonTransientConnectionPortObject connectionPort) {
                if (pid.isPresent() && pid.getAsInt() != connectionPort.getPid()) {
                    throw new IllegalStateException(
                        "Node cannot depend on multiple connection input ports coming from different sources");
                }
                pid = OptionalInt.of(connectionPort.getPid());
            }
        }
        return pid;
    }

    @Override
    public CloseableNodeFactoryProxy getNodeFactoryProxy() {
        return createPythonNode();
    }

    @Override
    public NodeDialogProxy getNodeDialogProxy() {
        return createPythonNode();
    }

    private CloseablePythonNodeProxy createPythonNode() {
        return createPythonNode(OptionalInt.empty());
    }

    @SuppressWarnings("resource") // the gateway is managed by the returned object
    private CloseablePythonNodeProxy createPythonNode(final OptionalInt requiredPid) { // NOSONAR
        try {
            PythonGateway<KnimeNodeBackend> gateway = null;
            if (requiredPid.isPresent()) {
                gateway = m_extension.getGatewayByPid(requiredPid.getAsInt());

                if (gateway == null || gateway.getEntryPoint() == null) {
                    m_extension.releaseGateway(m_cachedGateway);
                    throw new IllegalStateException(
                        "No connection data found. Re-execute the upstream node to refresh the connection.");
                }
            }

            if (gateway == null) {
                gateway = m_extension.createGateway();
                // Holding on to the gateway is no problem here because it can be closed
                // even if we have a reference to it.
                m_cachedGateway = gateway;
            }
            return m_proxyFactory.createProxy(gateway);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to initialize Python gateway.", ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while creating Python gateway.", ex);
        }
    }

    @Override
    public void cleanup() {
        if (m_cachedGateway == null) {
            return;
        }

        try {
            m_extension.releaseGateway(m_cachedGateway);
            m_cachedGateway.close();
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to close Python Gateway", ex);
        }
    }

}
