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
 *   Mar 17, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes.pycentric;

import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import org.knime.core.node.extension.CategoryExtension;
import org.knime.python3.PythonGateway;
import org.knime.python3.nodes.KnimeNodeBackend;
import org.knime.python3.nodes.PyNodeExtension;
import org.knime.python3.nodes.PythonNode;
import org.knime.python3.nodes.PythonNodeGatewayFactory;
import org.knime.python3.nodes.extension.ExtensionNode;
import org.knime.python3.nodes.proxy.PythonNodeProxy;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class FluentPythonNodeExtension implements PyNodeExtension {

    private final List<CategoryExtension.Builder> m_categoryBuilders;

    private final Map<String, PythonNode> m_nodes;

    private final String m_id;

    private final String m_extensionModule;

    private final PythonNodeGatewayFactory m_gatewayFactory;

    private final String m_version;

    FluentPythonNodeExtension(
        final String id,
        final String extensionModule,
        final PythonNode[] nodes,
        final List<CategoryExtension.Builder> categoryBuilders,
        final PythonNodeGatewayFactory gatewayFactory,
        final String version) {
        m_id = id;
        m_extensionModule = extensionModule;
        m_nodes = Stream.of(nodes).collect(toMap(PythonNode::getId, Function.identity()));
        m_categoryBuilders = categoryBuilders;
        m_gatewayFactory = gatewayFactory;
        m_version = version;
    }

    @Override
    public String getId() {
        return m_id;
    }

    @Override
    public String getVersion() {
        return m_version;
    }

    @Override
    public Stream<CategoryExtension.Builder> getCategories() {
        return m_categoryBuilders.stream();
    }

    @Override
    public PythonNode getNode(final String id) {
        return m_nodes.get(id);
    }

    @Override
    public PythonNodeProxy createNodeProxy(final KnimeNodeBackend backend, final String nodeId) {
        return backend.createNodeFromExtension(m_extensionModule, nodeId);
    }

    @Override
    public Stream<ExtensionNode> getNodeStream() {
        return m_nodes.values().stream().map(Function.identity());
    }

    @Override
    public PythonGateway<KnimeNodeBackend> createGateway() throws IOException, InterruptedException {
        return m_gatewayFactory.create();
    }
}
