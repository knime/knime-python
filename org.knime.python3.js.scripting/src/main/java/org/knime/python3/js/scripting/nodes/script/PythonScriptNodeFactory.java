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
 *   Jun 30, 2022 (benjamin): created
 */
package org.knime.python3.js.scripting.nodes.script;

import static org.knime.python3.js.scripting.nodes.script.PythonScriptPortsConfiguration.PORTGR_ID_INP_OBJECT;
import static org.knime.python3.js.scripting.nodes.script.PythonScriptPortsConfiguration.PORTGR_ID_INP_TABLE;
import static org.knime.python3.js.scripting.nodes.script.PythonScriptPortsConfiguration.PORTGR_ID_OUT_IMAGE;
import static org.knime.python3.js.scripting.nodes.script.PythonScriptPortsConfiguration.PORTGR_ID_OUT_OBJECT;
import static org.knime.python3.js.scripting.nodes.script.PythonScriptPortsConfiguration.PORTGR_ID_OUT_TABLE;

import java.util.Optional;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ConfigurableNodeFactory;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;
import org.knime.core.node.context.NodeCreationConfiguration;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.image.ImagePortObject;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeDialogFactory;
import org.knime.python2.port.PickledObjectFileStorePortObject;

/**
 * The factory for the Python scripting node.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class PythonScriptNodeFactory extends ConfigurableNodeFactory<PythonScriptNodeModel>
    implements NodeDialogFactory {

    @Override
    public NodeDialog createNodeDialog() {
        return new PythonScriptNodeDialog();
    }

    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    @Override
    public NodeView<PythonScriptNodeModel> createNodeView(final int viewIndex, final PythonScriptNodeModel nodeModel) {
        return null;
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    @Override
    protected PythonScriptNodeModel createNodeModel(final NodeCreationConfiguration creationConfig) {
        final var portsConfig = creationConfig.getPortConfig().orElseThrow(
            () -> new IllegalStateException("Ports configuration missing. This is an implementation error"));
        return new PythonScriptNodeModel(portsConfig);
    }

    @Override
    protected NodeDialogPane createNodeDialogPane(final NodeCreationConfiguration creationConfig) {
        // TODO(AP-19377) Display settings that are overwritten by a flow variable correctly
        return createNodeDialog().createLegacyFlowVariableNodeDialog();
    }

    @Override
    protected Optional<PortsConfigurationBuilder> createPortsConfigBuilder() {
        final var b = new PortsConfigurationBuilder();
        b.addExtendableInputPortGroup(PORTGR_ID_INP_OBJECT, PickledObjectFileStorePortObject.TYPE);
        b.addExtendableInputPortGroupWithDefault(PORTGR_ID_INP_TABLE, new PortType[0],
            new PortType[]{BufferedDataTable.TYPE}, BufferedDataTable.TYPE);
        b.addExtendableOutputPortGroupWithDefault(PORTGR_ID_OUT_TABLE, new PortType[0],
            new PortType[]{BufferedDataTable.TYPE}, BufferedDataTable.TYPE);
        b.addExtendableOutputPortGroup(PORTGR_ID_OUT_IMAGE, ImagePortObject.TYPE);
        b.addExtendableOutputPortGroup(PORTGR_ID_OUT_OBJECT, PickledObjectFileStorePortObject.TYPE);
        return Optional.of(b);
    }
}
