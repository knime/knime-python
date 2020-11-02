/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 *
 * History
 *   Sep 25, 2014 (Patrick Winter): created
 */
package org.knime.python2.nodes.script2;

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
import org.knime.core.node.port.database.DatabasePortObject;
import org.knime.core.node.port.image.ImagePortObject;
import org.knime.core.util.Pair;
import org.knime.python2.port.PickledObjectFileStorePortObject;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class Python2ScriptNodeFactory2 extends ConfigurableNodeFactory<PythonScriptNodeModel2> {

    // TODO: add drag'n'drop support for .pkl files of old Python Object Reader node. This requires changes to the
    // dynamic-ports framework (automatically adding an "Output object (pickled)" port in case of drag'n'drop).

    @Override
    protected Optional<PortsConfigurationBuilder> createPortsConfigBuilder() {
        final PortsConfigurationBuilder b = new PortsConfigurationBuilder();
        b.addOptionalPortGroup("Database connection (legacy)", DatabasePortObject.TYPE);
        b.addExtendableInputPortGroup("Input object (pickled)", PickledObjectFileStorePortObject.TYPE);
        b.addExtendableInputPortGroupWithDefault("Input table", new PortType[0], new PortType[]{BufferedDataTable.TYPE},
            BufferedDataTable.TYPE);
        b.addExtendableOutputPortGroupWithDefault("Output table", new PortType[0],
            new PortType[]{BufferedDataTable.TYPE}, BufferedDataTable.TYPE);
        b.addExtendableOutputPortGroup("Output image", ImagePortObject.TYPE);
        b.addExtendableOutputPortGroup("Output object (pickled)", PickledObjectFileStorePortObject.TYPE);
        return Optional.of(b);
    }

    @Override
    protected PythonScriptNodeModel2 createNodeModel(final NodeCreationConfiguration creationConfig) {
        final PortsConfiguration portsConfig = creationConfig.getPortConfig().get();
        final Pair<InputPort[], OutputPort[]> ports = createPorts(portsConfig);
        return new PythonScriptNodeModel2(portsConfig.getInputPorts(), ports.getFirst(), portsConfig.getOutputPorts(),
            ports.getSecond());
    }

    private static Pair<InputPort[], OutputPort[]> createPorts(final PortsConfiguration config) {
        final PortType[] inTypes = config.getInputPorts();
        int inTableSuffix = 1;
        int inObjectSuffix = 1;
        final InputPort[] inPorts = new InputPort[inTypes.length];
        DatabasePort databasePort = null;
        for (int i = 0; i < inTypes.length; i++) {
            final PortType inType = inTypes[i];
            final InputPort inPort;
            if (BufferedDataTable.TYPE.equals(inType)) {
                inPort = new DataTableInputPort("input_table_" + inTableSuffix++);
            } else if (PickledObjectFileStorePortObject.TYPE.equals(inType)) {
                inPort = new PickledObjectInputPort("input_object_" + inObjectSuffix++);
            } else if (DatabasePortObject.TYPE.equals(inType)) {
                databasePort = new DatabasePort(PythonScriptNodeConfig2.DB_UTIL_NAME);
                inPort = databasePort;
            } else {
                throw new IllegalStateException("Unsupported input type: " + inType.getName());
            }
            inPorts[i] = inPort;
        }

        final PortType[] outTypes = config.getOutputPorts();
        int outTableSuffix = 1;
        int outImageSuffix = 1;
        int outObjectSuffix = 1;
        final OutputPort[] outPorts = new OutputPort[outTypes.length];
        for (int i = 0; i < outTypes.length; i++) {
            final PortType outType = outTypes[i];
            final OutputPort outPort;
            if (BufferedDataTable.TYPE.equals(outType)) {
                outPort = new DataTableOutputPort("output_table_" + outTableSuffix++);
            } else if (ImagePortObject.TYPE.equals(outType)) {
                outPort = new ImageOutputPort("output_image_" + outImageSuffix++);
            } else if (PickledObjectFileStorePortObject.TYPE.equals(outType)) {
                outPort = new PickledObjectOutputPort("output_object_" + outObjectSuffix++);
            } else if (DatabasePortObject.TYPE.equals(outType)) {
                outPort = databasePort;
            } else {
                throw new IllegalStateException("Unsupported output type: " + outType.getName());
            }
            outPorts[i] = outPort;
        }

        return new Pair<>(inPorts, outPorts);
    }

    @Override
    public boolean hasDialog() {
        return true;
    }

    @Override
    protected NodeDialogPane createNodeDialogPane(final NodeCreationConfiguration creationConfig) {
        final Pair<InputPort[], OutputPort[]> ports = createPorts(creationConfig.getPortConfig().get());
        return new PythonScriptNodeDialog2(ports.getFirst(), ports.getSecond());
    }

    // TODO: we'll need a PortsConfiguration here to determine the correct number of views (2 + x) for when there are x
    // image outputs.
    @Override
    public int getNrNodeViews() {
        return 2;
    }

    @Override
    public NodeView<PythonScriptNodeModel2> createNodeView(final int viewIndex,
        final PythonScriptNodeModel2 nodeModel) {
        if (viewIndex == 0) {
            return new ExtToolStdoutNodeView<>(nodeModel);
        } else if (viewIndex == 1) {
            return new ExtToolStderrNodeView<>(nodeModel);
        } else {
            return null;
        }
    }
}
