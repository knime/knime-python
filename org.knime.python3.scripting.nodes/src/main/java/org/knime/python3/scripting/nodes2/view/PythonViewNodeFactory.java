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
 *   Sep 15, 2023 (benjamin): created
 */
package org.knime.python3.scripting.nodes2.view;

import static org.knime.python3.scripting.nodes2.PythonScriptPortsConfiguration.PORTGR_ID_INP_OBJECT;
import static org.knime.python3.scripting.nodes2.PythonScriptPortsConfiguration.PORTGR_ID_INP_TABLE;
import static org.knime.python3.scripting.nodes2.PythonScriptPortsConfiguration.PORTGR_ID_OUT_IMAGE;
import static org.knime.python3.scripting.nodes2.PythonScriptPortsConfiguration.PORTGR_ID_PYTHON_ENV;

import java.util.Optional;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ConfigurableNodeFactory;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.context.NodeCreationConfiguration;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.image.ImagePortObject;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeDialogFactory;
import org.knime.core.webui.node.dialog.NodeDialogManager;
import org.knime.core.webui.node.view.NodeView;
import org.knime.core.webui.node.view.NodeViewFactory;
import org.knime.pixi.port.PythonEnvironmentPortObject;
import org.knime.python2.port.PickledObjectFileStorePortObject;
import org.knime.python3.scripting.nodes2.PythonScriptNodeDialog;
import org.knime.python3.scripting.nodes2.PythonScriptNodeModel;
import org.knime.python3.views.HtmlFileNodeView;

/**
 * The factory for the Python View node.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction") // the UIExtension API is still restricted
public class PythonViewNodeFactory extends ConfigurableNodeFactory<PythonScriptNodeModel>
    implements NodeDialogFactory, NodeViewFactory<PythonScriptNodeModel> {

    @Override
    public NodeDialog createNodeDialog() {
        return new PythonScriptNodeDialog(true);
    }

    @Override
    public NodeView createNodeView(final PythonScriptNodeModel nodeModel) {
        return HtmlFileNodeView.builder() //
            .htmlSupplier(nodeModel::getPathToHtmlView) //
            .canBeUsedInReport(nodeModel::canViewBeUsedInReport) //
            .build();
    }

    @Override
    protected PythonScriptNodeModel createNodeModel(final NodeCreationConfiguration creationConfig) {
        final var portsConfig = creationConfig.getPortConfig().orElseThrow(
            () -> new IllegalStateException("Ports configuration missing. This is an implementation error"));
        return new PythonScriptNodeModel(portsConfig, true);
    }

    @Override
    protected int getNrNodeViews() {
        return 1;
    }

    @Override
    public org.knime.core.node.NodeView<PythonScriptNodeModel> createNodeView(final int viewIndex,
        final PythonScriptNodeModel nodeModel) {
        return new DummyNodeView(nodeModel);
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    @Override
    protected NodeDialogPane createNodeDialogPane(final NodeCreationConfiguration creationConfig) {
        // TODO(AP-19377) Display settings that are overwritten by a flow variable correctly
        return NodeDialogManager.createLegacyFlowVariableNodeDialog(createNodeDialog());
    }

    @Override
    protected Optional<PortsConfigurationBuilder> createPortsConfigBuilder() {
        final var b = new PortsConfigurationBuilder();
        b.addOptionalInputPortGroup(PORTGR_ID_PYTHON_ENV, PythonEnvironmentPortObject.TYPE);
        b.addExtendableInputPortGroup(PORTGR_ID_INP_OBJECT, PickledObjectFileStorePortObject.TYPE);
        b.addExtendableInputPortGroupWithDefault(PORTGR_ID_INP_TABLE, new PortType[0],
            new PortType[]{BufferedDataTable.TYPE}, BufferedDataTable.TYPE);

        b.addOptionalOutputPortGroup(PORTGR_ID_OUT_IMAGE, ImagePortObject.TYPE);
        return Optional.of(b);
    }

    /** A dummy node view that does nothing and that will only be opened by workflow tests. */
    private static final class DummyNodeView extends org.knime.core.node.NodeView<PythonScriptNodeModel> {

        protected DummyNodeView(final PythonScriptNodeModel nodeModel) {
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
}
