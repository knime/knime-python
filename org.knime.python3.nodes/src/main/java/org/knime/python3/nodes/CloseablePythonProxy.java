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
 *   Jan 21, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.knime.python3.PythonGateway;
import org.knime.python3.arrow.PythonArrowDataSink;
import org.knime.python3.arrow.PythonArrowDataSource;
import org.knime.python3.nodes.proxy.CloseableNodeDialogProxy;
import org.knime.python3.nodes.proxy.CloseableNodeFactoryProxy;
import org.knime.python3.nodes.proxy.CloseableNodeModelProxy;
import org.knime.python3.nodes.proxy.NodeModelProxy;
import org.knime.python3.nodes.proxy.NodeProxy;

import py4j.Py4JException;

/**
 * Manages the lifecycle of a Python based NodeProxy and its associated process. Invoking {@link Closeable#close()}
 * shuts down the Python process and the proxy is no longer usable.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class CloseablePythonProxy
    implements CloseableNodeModelProxy, CloseableNodeFactoryProxy, CloseableNodeDialogProxy {

    private final NodeProxy m_proxy;

    private final PythonGateway<?> m_gateway;

    CloseablePythonProxy(final NodeProxy proxy, final PythonGateway<?> gateway) {
        m_proxy = proxy;
        m_gateway = gateway;
    }

    @Override
    public void close() {
        try {
            m_gateway.close();
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to shutdown Python gateway.", ex);
        }
    }

    @Override
    public void initializeJavaCallback(final Callback callback) {
        m_proxy.initializeJavaCallback(callback);
    }

    @Override
    public String getInitialParameters() {
        return m_proxy.getInitialParameters();
    }

    @Override
    public void setParameters(final String parameters, final String version) {
        try {
            m_proxy.setParameters(parameters, version);
        } catch (Py4JException ex) {
            throw new IllegalStateException("Failed to set settings after validating them.", ex);
        }
    }

    @Override
    public String validateParameters(final String parameters, final String version) {
        return m_proxy.validateParameters(parameters, version);
    }

    @Override
    public List<PythonArrowDataSink> execute(final PythonArrowDataSource[] sources, final String[] inputObjectPaths,
        final String[] outputObjectPaths, final NodeModelProxy.PythonExecutionContext ctx) {
        return m_proxy.execute(sources, inputObjectPaths, outputObjectPaths, ctx);
    }

    @Override
    public String getDialogRepresentation(final String parameters, final String version, final String[] specs) {
        return m_proxy.getDialogRepresentation(parameters, version, specs);
    }

    @Override
    public List<String> configure(final String[] serializedInSchemas) {
        return m_proxy.configure(serializedInSchemas);
    }
}
