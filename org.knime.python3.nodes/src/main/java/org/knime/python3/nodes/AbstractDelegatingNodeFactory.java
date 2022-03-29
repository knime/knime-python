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
 *   Jan 20, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes;

import java.nio.file.Path;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeDialogFactory;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.core.webui.node.dialog.TextSettingsDataService;
import org.knime.python3.nodes.dialog.DelegatingTextSettingsDataService;
import org.knime.python3.nodes.dialog.JsonFormsNodeDialog;
import org.knime.python3.nodes.proxy.NodeProxyProvider;

/**
 * Abstract implementation of a {@link NodeFactory} which performs all tasks of a node by delegating to proxy objects.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public abstract class AbstractDelegatingNodeFactory extends NodeFactory<DelegatingNodeModel>
    implements NodeDialogFactory {

    private final JsonNodeSettings m_initialSettings;

    private final TextSettingsDataService m_settingsService;

    private final NodeProxyProvider m_proxyProvider;

    /**
     * Constructor.
     *
     * @param modulePath path to the module containing the node
     * @param nodeClass the class of the node in the module
     */
    protected AbstractDelegatingNodeFactory(final Path modulePath, final String nodeClass) {
        this(new FreshPythonProcessProxyProvider(new PythonNodeClass(modulePath, nodeClass)));

    }

    /**
     * @param proxyProvider provider for proxies to be used by the different aspects of a node
     */
    protected AbstractDelegatingNodeFactory(final NodeProxyProvider proxyProvider) {
        m_proxyProvider = proxyProvider;
        try (var proxy = m_proxyProvider.getNodeFactoryProxy()) {
            m_initialSettings = new JsonNodeSettings(proxy.getParameters());
            m_settingsService = new DelegatingTextSettingsDataService(m_proxyProvider::getNodeDialogProxy);
        }
    }

    @Override
    public DelegatingNodeModel createNodeModel() {
        return new DelegatingNodeModel(m_proxyProvider::getNodeModelProxy, m_initialSettings);
    }

    @Override
    protected int getNrNodeViews() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public NodeView<DelegatingNodeModel> createNodeView(final int viewIndex, final DelegatingNodeModel nodeModel) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected boolean hasDialog() {
        // TODO eventually determined by Python
        return true;
    }

    @Override
    public NodeDialog createNodeDialog() {
        // TODO use dialog service once implemented (UIEXT-161)
        return new JsonFormsNodeDialog(SettingsType.MODEL, m_settingsService);
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        // TODO throw an exception instead?
        return null;
    }

}
