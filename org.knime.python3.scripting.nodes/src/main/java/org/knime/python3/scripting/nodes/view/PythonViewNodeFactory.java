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
 *   Nov 18, 2021 (marcel): created
 */
package org.knime.python3.scripting.nodes.view;

import static org.knime.python3.scripting.nodes.PortsConfigurationUtils.createInputPorts;

import java.util.Optional;

import org.knime.base.node.util.exttool.ExtToolStderrNodeView;
import org.knime.base.node.util.exttool.ExtToolStdoutNodeView;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ConfigurableNodeFactory;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;
import org.knime.core.node.context.NodeCreationConfiguration;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.image.ImagePortObject;
import org.knime.core.webui.node.view.NodeViewFactory;
import org.knime.python2.port.PickledObjectFileStorePortObject;
import org.knime.python2.ports.ImageOutputPort;
import org.knime.python2.ports.OutputPort;
import org.knime.python3.views.HtmlFileNodeView;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class PythonViewNodeFactory extends ConfigurableNodeFactory<PythonViewNodeModel>
    implements NodeViewFactory<PythonViewNodeModel> {

    @Override
    protected Optional<PortsConfigurationBuilder> createPortsConfigBuilder() {
        final var b = new PortsConfigurationBuilder();
        b.addExtendableInputPortGroup("Input object (pickled)", PickledObjectFileStorePortObject.TYPE);
        b.addExtendableInputPortGroupWithDefault("Input table", new PortType[0], new PortType[]{BufferedDataTable.TYPE},
            BufferedDataTable.TYPE);
        b.addOptionalOutputPortGroup("Output image", ImagePortObject.TYPE);
        return Optional.of(b);
    }

    @Override
    protected PythonViewNodeModel createNodeModel(final NodeCreationConfiguration creationConfig) {
        final var config = creationConfig.getPortConfig().get(); // NOSONAR
        return new PythonViewNodeModel(createInputPorts(config), createOutputPorts(config));
    }

    @Override
    protected int getNrNodeViews() {
        return 3;
    }

    @Override
    public NodeView<PythonViewNodeModel> createNodeView(final int viewIndex, final PythonViewNodeModel nodeModel) {
        if (viewIndex == 0) {
            // NB: The dummy view will only be opened by workflow tests
            // The user cannot open the view because it is not shown in the context menu
            return new DummyNodeView(nodeModel);
        } else if (viewIndex == 1) {
            return new ExtToolStdoutNodeView<>(nodeModel);
        } else if (viewIndex == 2) {
            return new ExtToolStderrNodeView<>(nodeModel);
        } else {
            return null;
        }
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    @Override
    protected NodeDialogPane createNodeDialogPane(final NodeCreationConfiguration creationConfig) {
        final var config = creationConfig.getPortConfig().get(); // NOSONAR
        return new PythonViewNodeDialog(createInputPorts(config), createOutputPorts(config));
    }

    @Override
    public org.knime.core.webui.node.view.NodeView createNodeView(final PythonViewNodeModel nodeModel) {
        return new HtmlFileNodeView(nodeModel::getPathToHtml);
    }

    /** Create either an empty list or a list with one image output port */
    private static OutputPort[] createOutputPorts(final PortsConfiguration config) {
        final PortType[] outputPorts = config.getOutputPorts();

        // No output port
        if (outputPorts.length == 0) {
            return new OutputPort[0];
        }

        // Check the configured output: This should never fail because we only allow 1 image output
        if (outputPorts.length > 1) {
            throw new IllegalStateException("Too many output ports: " + outputPorts.length);
        } else if (!ImagePortObject.TYPE.equals(outputPorts[0])) {
            throw new IllegalStateException("Unsupported output type: " + outputPorts[0].getName());
        }

        // One image output port
        return new OutputPort[]{new ImageOutputPort("knio.output_images[0]")};

    }

    /** A dummy node view that does nothing and that will only be opened by workflow tests. */
    private static final class DummyNodeView extends NodeView<PythonViewNodeModel> {

        protected DummyNodeView(final PythonViewNodeModel nodeModel) {
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
