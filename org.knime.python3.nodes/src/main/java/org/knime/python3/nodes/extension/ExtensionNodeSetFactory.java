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
package org.knime.python3.nodes.extension;

import static java.util.stream.Collectors.toMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.knime.core.node.DynamicNodeFactory;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDescription;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSetFactory;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeView;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.node.config.ConfigWO;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeDialogFactory;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.python3.nodes.DelegatingNodeModel;
import org.knime.python3.nodes.JsonNodeSettings;
import org.knime.python3.nodes.dialog.DelegatingTextSettingsDataService;
import org.knime.python3.nodes.dialog.JsonFormsNodeDialog;
import org.knime.python3.nodes.proxy.NodeProxyProvider;

/**
 * A {@link NodeSetFactory} for extensions that provide nodes whose settings are JSON and whose dialogs are JSON Forms
 * based.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public abstract class ExtensionNodeSetFactory implements NodeSetFactory {

    private static final Map<String, KnimeExtension> ALL_EXTENSIONS = new ConcurrentHashMap<>();

    private final Map<String, KnimeExtension> m_extensions;

    private final Map<NodeId, ExtensionNode> m_allNodes;

    /**
     * Constructor.
     *
     * @param extensionSupplier supplier for a {@link Stream} of {@link KnimeExtension} objects.
     */
    protected ExtensionNodeSetFactory(final Supplier<Stream<? extends KnimeExtension>> extensionSupplier) {
        m_extensions = extensionSupplier.get().collect(toMap(KnimeExtension::getId, Function.identity()));
        ALL_EXTENSIONS.putAll(m_extensions);
        m_allNodes = new HashMap<>();
        for (var extension : m_extensions.values()) {
            var extensionId = extension.getId();
            extension.getNodes().forEach(n -> m_allNodes.put(new NodeId(extensionId, n.getId()), n));
        }
    }

    @Override
    public final Collection<String> getNodeFactoryIds() {
        return m_allNodes.keySet().stream()//
            .map(NodeId::getCombinedId)//
            .collect(Collectors.toList());
    }

    @Override
    public final Class<? extends NodeFactory<? extends NodeModel>> getNodeFactory(final String id) {
        return DynamicExtensionNodeFactory.class;
    }

    @Override
    public final String getCategoryPath(final String id) {
        return m_allNodes.get(NodeId.fromCombined(id)).getCategoryPath();
    }

    @Override
    public final String getAfterID(final String id) {
        return m_allNodes.get(NodeId.fromCombined(id)).getAfterId();
    }

    @Override
    public final ConfigRO getAdditionalSettings(final String id) {
        var nodeId = NodeId.fromCombined(id);
        final var extensionId = nodeId.getExtensionId();
        var settings = new NodeSettings(id);
        settings.addString("extension_id", extensionId);
        settings.addString("node_id", nodeId.getNodeId());
        return settings;
    }

    /**
     * {@link DynamicNodeFactory} that is used in the context of {@link ExtensionNodeSetFactory}.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public static final class DynamicExtensionNodeFactory extends DynamicNodeFactory<DelegatingNodeModel>
        implements NodeDialogFactory {

        private NodeProxyProvider m_proxyProvider;

        private ConfigRO m_nodeFactoryConfig;

        private DelegatingTextSettingsDataService m_dialogSettingsService;

        private NodeDescription m_nodeDescription;

        @Override
        public void loadAdditionalFactorySettings(final ConfigRO config) throws InvalidSettingsException {
            var extensionId = config.getString("extension_id");
            var nodeId = config.getString("node_id");
            var extension = ALL_EXTENSIONS.get(extensionId);
            CheckUtils.checkSetting(extension != null, "Unknown extension id '%s' encountered.", extensionId);
            @SuppressWarnings("null") // checked in preceding line
            var node = extension.getNode(nodeId);
            m_nodeDescription = new ExtensionNodeDescription(node);
            m_nodeFactoryConfig = config;
            var proxyProvider = extension.createProxyProvider(nodeId);
            m_proxyProvider = proxyProvider;
            m_dialogSettingsService = new DelegatingTextSettingsDataService(m_proxyProvider::getNodeDialogProxy);
            super.loadAdditionalFactorySettings(config);
        }

        @Override
        public void saveAdditionalFactorySettings(final ConfigWO config) {
            m_nodeFactoryConfig.copyTo(config);
        }

        @Override
        public DelegatingNodeModel createNodeModel() {
            try (var proxy = m_proxyProvider.getNodeFactoryProxy()) {
                // happens here to speed up the population of the node repository
                var initialSettings = proxy.getParameters();
                return new DelegatingNodeModel(m_proxyProvider::getNodeModelProxy,
                    new JsonNodeSettings(initialSettings));
            }
        }

        @Override
        protected NodeDescription createNodeDescription() {
            // TODO retrieve from extension
            return m_nodeDescription;
        }

        @Override
        protected int getNrNodeViews() {
            // TODO support views. Talk with Benny how this can be realized
            return 0;
        }

        @Override
        public NodeView<DelegatingNodeModel> createNodeView(final int viewIndex, final DelegatingNodeModel nodeModel) {
            // TODO throw an exception? NodeViews are Java, hence the extension can't be pure Python and therefore nodes
            // can be registered the ordinary way.
            return null;
        }

        @Override
        protected boolean hasDialog() {
            // TODO let the extension decide
            return true;
        }

        @Override
        public NodeDialog createNodeDialog() {
            return new JsonFormsNodeDialog(SettingsType.MODEL, m_dialogSettingsService);
        }

        @Override
        protected NodeDialogPane createNodeDialogPane() {
            // TODO throw an exception? If a NodeDialogPane is used, then we don't have a pure Python extension and
            // and nodes could be registered the ordinary way
            return null;
        }

    }

}
