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
import org.knime.core.webui.node.view.NodeViewFactory;
import org.knime.python2.port.PickledObjectFileStorePortObject;
import org.knime.python2.ports.DataTableInputPort;
import org.knime.python2.ports.InputPort;
import org.knime.python2.ports.PickledObjectInputPort;
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
        return Optional.of(b);
    }

    @Override
    protected PythonViewNodeModel createNodeModel(final NodeCreationConfiguration creationConfig) {
        return new PythonViewNodeModel(createPorts(creationConfig.getPortConfig().get())); // NOSONAR
    }

    @Override
    protected int getNrNodeViews() {
        return 3;
    }

    @Override
    public NodeView<PythonViewNodeModel> createNodeView(final int viewIndex, final PythonViewNodeModel nodeModel) {
        if (viewIndex == 0) {
            throw new IllegalStateException(
                "The view with the index 0 is a JS view and needs to be accessed via NodeViewFactory#createNodeView. "
                    + "This is an implementation error.");
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
        return new PythonViewNodeDialog(createPorts(creationConfig.getPortConfig().get())); // NOSONAR
    }

    private static InputPort[] createPorts(final PortsConfiguration config) {
        final PortType[] inTypes = config.getInputPorts();
        int inTableIndex = 0;
        int inObjectIndex = 0;
        final var inPorts = new InputPort[inTypes.length];
        for (int i = 0; i < inTypes.length; i++) {
            final PortType inType = inTypes[i];
            final InputPort inPort;
            if (BufferedDataTable.TYPE.equals(inType)) {
                inPort = new DataTableInputPort("knio.input_tables[" + inTableIndex++ + "]");
            } else if (PickledObjectFileStorePortObject.TYPE.equals(inType)) {
                inPort = new PickledObjectInputPort("knio.input_objects[" + inObjectIndex++ + "]");
            } else {
                throw new IllegalStateException("Unsupported input type: " + inType.getName());
            }
            inPorts[i] = inPort;
        }
        return inPorts;
    }

    @Override
    public org.knime.core.webui.node.view.NodeView createNodeView(final PythonViewNodeModel nodeModel) {
        return new HtmlFileNodeView(nodeModel::getPathToHtml);
    }
}
