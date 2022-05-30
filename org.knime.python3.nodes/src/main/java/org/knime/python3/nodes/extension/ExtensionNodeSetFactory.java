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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.node.config.ConfigWO;
import org.knime.core.node.extension.CategoryExtension;
import org.knime.core.node.extension.CategorySetFactory;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.webui.data.ApplyDataService;
import org.knime.core.webui.data.DataService;
import org.knime.core.webui.data.InitialDataService;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeDialogFactory;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.core.webui.node.view.NodeView;
import org.knime.core.webui.node.view.NodeViewFactory;
import org.knime.core.webui.page.Page;
import org.knime.python3.nodes.DelegatingNodeModel;
import org.knime.python3.nodes.dialog.DelegatingTextSettingsDataService;
import org.knime.python3.nodes.dialog.JsonFormsNodeDialog;
import org.knime.python3.nodes.proxy.NodeProxyProvider;
import org.knime.python3.nodes.settings.JsonNodeSettings;
import org.knime.python3.nodes.views.HtmlFileNodeView;

/**
 * A {@link NodeSetFactory} for extensions that provide nodes whose settings are JSON and whose dialogs are JSON Forms
 * based.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public abstract class ExtensionNodeSetFactory implements NodeSetFactory, CategorySetFactory {

    private static final Map<String, KnimeExtension> ALL_EXTENSIONS = new ConcurrentHashMap<>();

    private final Map<String, KnimeExtension> m_extensions;

    private final Map<NodeId, ExtensionNode> m_allNodes;

    private final List<CategoryExtension> m_allCategories;

    /**
     * Constructor.
     *
     * @param extensionSupplier supplier for a {@link Stream} of {@link KnimeExtension} objects.
     */
    protected ExtensionNodeSetFactory(final Supplier<Stream<? extends KnimeExtension>> extensionSupplier) {
        m_extensions = extensionSupplier.get().collect(toMap(KnimeExtension::getId, Function.identity()));
        ALL_EXTENSIONS.putAll(m_extensions);
        m_allNodes = new HashMap<>();
        m_allCategories = new ArrayList<>();
        for (var extension : m_extensions.values()) {
            var extensionId = extension.getId();
            extension.getNodes().forEach(n -> m_allNodes.put(new NodeId(extensionId, n.getId()), n));
            extension.getCategories().forEach(c -> m_allCategories.add(c));
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

    @Override
    public Collection<CategoryExtension> getCategories() {
        return m_allCategories;
    }

    /**
     * {@link DynamicNodeFactory} that is used in the context of {@link ExtensionNodeSetFactory}.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public static final class DynamicExtensionNodeFactory extends DynamicNodeFactory<DelegatingNodeModel>
        implements NodeDialogFactory, NodeViewFactory<DelegatingNodeModel> {

        private NodeProxyProvider m_proxyProvider;

        private ConfigRO m_nodeFactoryConfig;

        private DelegatingTextSettingsDataService m_dialogSettingsService;

        private NodeDescription m_nodeDescription;

        private Optional<String> m_bundleName;

        private ExtensionNode m_node;

        private int m_numViews;

        @SuppressWarnings("null")
        @Override
        public void loadAdditionalFactorySettings(final ConfigRO config) throws InvalidSettingsException {
            var extensionId = config.getString("extension_id");
            var nodeId = config.getString("node_id");
            var extension = ALL_EXTENSIONS.get(extensionId);
            CheckUtils.checkSetting(extension != null, "Unknown extension id '%s' encountered.", extensionId);
            m_bundleName = extension.getBundleName();
            m_node = extension.getNode(nodeId);
            m_nodeDescription = m_node.getNodeDescription();
            m_nodeFactoryConfig = config;
            m_numViews = m_node.getNumViews();
            var proxyProvider = extension.createProxyProvider(nodeId);
            m_proxyProvider = proxyProvider;
            m_dialogSettingsService = new DelegatingTextSettingsDataService(m_proxyProvider::getNodeDialogProxy);
            super.loadAdditionalFactorySettings(config);
        }

        @Override
        protected Optional<String> getBundleName() {
            return m_bundleName;
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
                var schema = proxy.getSchema();
                return new DelegatingNodeModel(m_proxyProvider, m_node.getInputPortTypes(), m_node.getOutputPortTypes(),
                    new JsonNodeSettings(initialSettings, schema));
            }
        }

        @Override
        protected NodeDescription createNodeDescription() {
            return m_nodeDescription;
        }

        @Override
        protected int getNrNodeViews() {
            // We never have Java Views (see #hasView() for the JS view)
            return 0;
        }

        @Override
        public org.knime.core.node.NodeView<DelegatingNodeModel> createNodeView(final int viewIndex,
            final DelegatingNodeModel nodeModel) {
            // We never have Java Views but only a JS view (see #createNodeView(NodeModel))
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
            return createNodeDialog().createLegacyFlowVariableNodeDialog();
        }

        @Override
        public boolean hasNodeView() {
            // FIXME hack to make dialogs for nodes without views work until they work on their own.
            return true;
        }

        @Override
        public NodeView createNodeView(final DelegatingNodeModel nodeModel) {
            if (!hasNodeView()) {
                throw new IllegalStateException("The node has no view.");
            }
            final var pathToHtml = nodeModel.getPathToHtmlView();
            if (pathToHtml.isEmpty()) {
                // FIXME this is just a hack to make dialogs work for nodes without view
//                throw new IllegalStateException("The node did not return a view.");
                return new DummyNodeView();
            }
            return new HtmlFileNodeView(pathToHtml.get());
        }

        private static final class DummyNodeView implements NodeView {

            @Override
            public Optional<InitialDataService> createInitialDataService() {
                return Optional.empty();
            }

            @Override
            public Optional<DataService> createDataService() {
                return Optional.empty();
            }

            @Override
            public Optional<ApplyDataService> createApplyDataService() {
                return Optional.empty();
            }

            @Override
            public Page getPage() {
                return Page.builder(() -> "<!DOCTYPE html>", "index.html").build();
            }

            @Override
            public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
                // nothing to do
            }

            @Override
            public void loadValidatedSettingsFrom(final NodeSettingsRO settings) {
                // nothing to do
            }

        }
    }
}
