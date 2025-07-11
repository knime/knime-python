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
import java.util.Arrays;
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

import org.knime.core.node.BundleNameProvider;
import org.knime.core.node.ConfigurableNodeFactory;
import org.knime.core.node.DynamicNodeFactory;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDescription;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSetFactory;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.ParameterizedNodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.node.config.ConfigWO;
import org.knime.core.node.context.NodeCreationConfiguration;
import org.knime.core.node.extension.CategoryExtension;
import org.knime.core.node.extension.CategorySetFactory;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeDialogFactory;
import org.knime.core.webui.node.dialog.NodeDialogManager;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.core.webui.node.dialog.defaultdialog.DefaultNodeSettingsServiceWithVariables;
import org.knime.core.webui.node.dialog.kai.KaiNodeInterface;
import org.knime.core.webui.node.dialog.kai.KaiNodeInterfaceFactory;
import org.knime.core.webui.node.view.NodeView;
import org.knime.core.webui.node.view.NodeViewFactory;
import org.knime.python3.nodes.DelegatingNodeModel;
import org.knime.python3.nodes.dialog.DelegatingJsonSettingsDataService;
import org.knime.python3.nodes.dialog.JsonFormsNodeDialog;
import org.knime.python3.nodes.ports.PythonPortTypeRegistry;
import org.knime.python3.nodes.proxy.NodeProxyProvider;
import org.knime.python3.nodes.proxy.NodeViewProxy;
import org.knime.python3.nodes.proxy.NodeViewProxy.DataServiceProxy;
import org.knime.python3.views.HtmlFileNodeView;
import org.knime.python3.views.HtmlFileNodeView.JsonRpcRequestHandler;

/**
 * A {@link NodeSetFactory} for extensions that provide nodes whose settings are JSON and whose dialogs are JSON Forms
 * based.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction")
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
        return m_allNodes.entrySet().stream().filter(entry -> !entry.getValue().isHidden())
            .map(entry -> entry.getKey().getCombinedId()).collect(Collectors.toList());
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
    public static final class DynamicExtensionNodeFactory extends ConfigurableNodeFactory<DelegatingNodeModel>
        implements NodeDialogFactory, NodeViewFactory<DelegatingNodeModel>, BundleNameProvider,
        ParameterizedNodeFactory, KaiNodeInterfaceFactory {

        private NodeProxyProvider m_proxyProvider;

        private ConfigRO m_nodeFactoryConfig;

        private DelegatingJsonSettingsDataService m_dialogSettingsService;

        private NodeDescription m_nodeDescription;

        private String m_bundleName;

        private ExtensionNode m_node;

        private int m_numViews;

        private String m_extensionVersion;

        private String m_factoryIdUniquifier;

        private ExtensionKaiNodeInterface m_kaiNodeInterface;

        /**
         * We use the lazy constructor of the {@link ConfigurableNodeFactory}.
         */
        public DynamicExtensionNodeFactory() {
            super(true);
        }

        @SuppressWarnings("null")
        @Override
        public void loadAdditionalFactorySettings(final ConfigRO config) throws InvalidSettingsException {
            var extensionId = config.getString("extension_id");
            var nodeId = config.getString("node_id");
            var extension = ALL_EXTENSIONS.get(extensionId);
            CheckUtils.checkSetting(extension != null, "Unknown extension id '%s' encountered.", extensionId);
            m_bundleName = extensionId;
            m_node = extension.getNode(nodeId);
            m_nodeDescription = m_node.getNodeDescription();
            m_nodeFactoryConfig = config;
            m_numViews = m_node.getNumViews();
            m_extensionVersion = extension.getVersion();
            var proxyProvider = extension.createProxyProvider(nodeId);
            m_proxyProvider = proxyProvider;
            m_dialogSettingsService =
                new DelegatingJsonSettingsDataService(m_proxyProvider::getNodeDialogProxy, m_extensionVersion);
            m_kaiNodeInterface = new ExtensionKaiNodeInterface(m_proxyProvider::getNodeDialogProxy, m_extensionVersion);
            m_factoryIdUniquifier = new NodeId(extensionId, nodeId).getCombinedId();
            super.loadAdditionalFactorySettings(config);
        }

        /**
         * @return The name for the bundle
         */
        @Override
        public String getBundleName() {
            return m_bundleName;

        }

        @Override
        public void saveAdditionalFactorySettings(final ConfigWO config) {
            m_nodeFactoryConfig.copyTo(config);
        }

        @Override
        protected NodeDescription createNodeDescription() {
            return m_nodeDescription;
        }

        @Override
        protected int getNrNodeViews() {
            return m_numViews;
        }

        @Override
        public org.knime.core.node.NodeView<DelegatingNodeModel> createNodeView(final int viewIndex,
            final DelegatingNodeModel nodeModel) {
            // We never have Java Views but only a JS view (see #createNodeView(NodeModel))

            // AP-19877: The dummy view will only be opened by workflow tests
            // The user cannot open the view because it is not shown in the context menu
            return new DummyNodeView(nodeModel);
        }

        @Override
        protected boolean hasDialog() {
            // TODO let the extension decide
            return true;
        }

        @Override
        public NodeDialog createNodeDialog() {
            return new JsonFormsNodeDialog(SettingsType.MODEL,
                new DefaultNodeSettingsServiceWithVariables(m_dialogSettingsService), m_dialogSettingsService);
        }

        @Override
        public boolean hasNodeView() {
            return m_numViews > 0;
        }

        @Override
        public NodeView createNodeView(final DelegatingNodeModel nodeModel) {
            if (!hasNodeView()) {
                throw new IllegalStateException("The node has no view.");
            }

            var extensionNodeView = m_node.getExtensionNodeView()[0];
            var viewResources = extensionNodeView.getViewResources();

            if (extensionNodeView.indexHtmlName() == null) {
                // The view is is generated dynamically in execute
                return new HtmlFileNodeView( //
                    () -> nodeModel.getPathToHtmlView()
                        .orElseThrow(() -> new IllegalStateException("View is not present. This is a coding error.")),
                    viewResources //
                );
            } else {
                // The view is a static HTML file - we need a data service
                Supplier<JsonRpcRequestHandler> dataServiceSupplier = () -> {
                    return new JsonRpcRequestHandler() {

                        private NodeViewProxy m_nodeViewProxy;

                        private DataServiceProxy m_dataServiceProxy;

                        @Override
                        public String handleRequest(final String jsonRpcRequest) {
                            if (m_nodeViewProxy == null) {
                                m_nodeViewProxy = m_proxyProvider.getNodeViewProxy();
                                m_dataServiceProxy = m_nodeViewProxy.getDataServiceProxy(nodeModel.getSettings(),
                                    nodeModel.getInternalPortObjects(), nodeModel, nodeModel);
                            }
                            return m_dataServiceProxy.handleJsonRpcRequest(jsonRpcRequest);
                        }

                        @Override
                        public void close() throws Exception {
                            if (m_nodeViewProxy != null) {
                                m_nodeViewProxy.close();
                                m_nodeViewProxy = null;
                            }
                            if (m_dataServiceProxy != null) {
                                m_dataServiceProxy.close();
                                m_dataServiceProxy = null;
                            }
                        }

                    };
                };

                return new HtmlFileNodeView(extensionNodeView::getHtmlPath, extensionNodeView.getRelativeHtmlPath(),
                    viewResources, dataServiceSupplier);
            }
        }

        /** A dummy node view that does nothing and that will only be opened by workflow tests. */
        private static final class DummyNodeView extends org.knime.core.node.NodeView<DelegatingNodeModel> {

            protected DummyNodeView(final DelegatingNodeModel nodeModel) {
                super(nodeModel);
            }

            @Override
            protected void onClose() {
                // Dummy
            }

            @Override
            protected void onOpen() {
                // Dummy

            }

            @Override
            protected void modelChanged() {
                // Dummy
            }
        }

        @Override
        protected Optional<PortsConfigurationBuilder> createPortsConfigBuilder() {
            // Order matters here too
            final var b = new PortsConfigurationBuilder();

            PortSpecifier[] inputPorts = m_node.getInputPorts();
            for (int i = 0; i < inputPorts.length; i++) {

                PortSpecifier portSpecifier = inputPorts[i];
                var portType = PythonPortTypeRegistry.getPortTypeForIdentifier(portSpecifier.typeString());
                if (portSpecifier.isGroup()) {
                    b.addExtendableInputPortGroup(portSpecifier.name(), portType);
                } else if (portSpecifier.isOptional()) {
                    b.addOptionalInputPortGroup(portSpecifier.name(), portType);
                } else {
                    b.addFixedInputPortGroup(String.format("Input %s # %d", portSpecifier.name(), i), portType);
                }
            }

            PortSpecifier[] outputPorts = m_node.getOutputPorts();
            for (int i = 0; i < outputPorts.length; i++) {

                PortSpecifier portSpecifier = outputPorts[i];
                if (portSpecifier.isGroup()) {
                    b.addExtendableOutputPortGroup(portSpecifier.name(),
                        PythonPortTypeRegistry.getPortTypeForIdentifier(portSpecifier.typeString()));
                } else {
                    b.addFixedOutputPortGroup(String.format("Output %s # %d", portSpecifier.name(), i),
                        PythonPortTypeRegistry.getPortTypeForIdentifier(portSpecifier.typeString()));
                }
            }

            return Optional.of(b);
        }

        @Override
        protected DelegatingNodeModel createNodeModel(final NodeCreationConfiguration creationConfig) {
            final var config = creationConfig.getPortConfig().get(); // NOSONAR

            // Note: We hold the outputs if one of the views is not generated dynamically, indicating that they need to
            // load their data from a data service
            var shouldHoldOutputs = Arrays.stream(m_node.getExtensionNodeView()) //
                .anyMatch(v -> v.indexHtmlName() != null);

            // happens here to speed up the population of the node repository
            return new DelegatingNodeModel(m_proxyProvider, config.getInputPorts(), config.getOutputPorts(),
                m_extensionVersion, config.getInputPortLocation(), config.getOutputPortLocation(), shouldHoldOutputs);
        }

        @Override
        protected NodeDialogPane createNodeDialogPane(final NodeCreationConfiguration creationConfig) {
            return NodeDialogManager.createLegacyFlowVariableNodeDialog(createNodeDialog());
        }

        @Override
        public String getFactoryIdUniquifier() {
            return m_factoryIdUniquifier;
        }

        @Override
        public KaiNodeInterface createKaiNodeInterface() {
            return m_kaiNodeInterface;
        }

    }

    /**
     * Represents a specification for a port, including its name, type, description, and additional properties such as
     * group membership and default values.
     *
     * @param name The name of the port.
     * @param typeString The type of the port, represented as a string.
     * @param description A brief description of the port's purpose or usage.
     * @param isGroup Indicates whether the port is part of a group (true) or not (false).
     * @param defaults The default value for the port, typically used for initialization.
     * @param descriptionIndex the index where to insert in the description.
     */
    public record PortSpecifier(String name, String typeString, String description, boolean isGroup, int defaults,
        int descriptionIndex, boolean isOptional) {
    }
}
