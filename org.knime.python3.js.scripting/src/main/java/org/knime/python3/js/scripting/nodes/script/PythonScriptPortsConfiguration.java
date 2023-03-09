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
 *   Sep 21, 2022 (benjamin): created
 */
package org.knime.python3.js.scripting.nodes.script;

import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.port.image.ImagePortObject;
import org.knime.core.node.workflow.NodeContext;
import org.knime.python2.port.PickledObjectFileStorePortObject;

/**
 * Information about the configured ports of a Python scripting node.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
final class PythonScriptPortsConfiguration {

    static final String PORTGR_ID_INP_OBJECT = "Input object (pickled)";

    static final String PORTGR_ID_INP_TABLE = "Input table";

    static final String PORTGR_ID_OUT_TABLE = "Output table";

    static final String PORTGR_ID_OUT_IMAGE = "Output image";

    static final String PORTGR_ID_OUT_OBJECT = "Output object (pickled)";

    private final int m_numOutTables;

    private final int m_numOutImages;

    private final int m_numOutObjects;

    /**
     * Create a new {@link PythonScriptPortsConfiguration} from the given {@link PortsConfiguration}.
     *
     * @param portsConfig
     * @return a new {@link PythonScriptPortsConfiguration}
     */
    static PythonScriptPortsConfiguration fromPortsConfiguration(final PortsConfiguration portsConfig) {
        // Get the number of different output ports from the ports configuration (ArrayUtils#getLength handles null)
        final Map<String, int[]> outPortsLocation = portsConfig.getOutputPortLocation();
        final var numOutTables = ArrayUtils.getLength(outPortsLocation.get(PORTGR_ID_OUT_TABLE));
        final var numOutImages = ArrayUtils.getLength(outPortsLocation.get(PORTGR_ID_OUT_IMAGE));
        final var numOutObjects = ArrayUtils.getLength(outPortsLocation.get(PORTGR_ID_OUT_OBJECT));
        return new PythonScriptPortsConfiguration(numOutTables, numOutImages, numOutObjects);
    }

    /**
     * Create a new {@link PythonScriptPortsConfiguration} from the current {@link NodeContext}. Has to be called when a
     * node context is available.
     * <P>
     * NOTE: This is a hack because the {@link PortsConfiguration} is currently not available for a UI-Extension based
     * dialog
     *
     * @return the {@link PythonScriptPortsConfiguration} of the node that is associated with the current thread
     */
    static PythonScriptPortsConfiguration fromCurrentNodeContext() {
        // TODO(AP-19552) Remove this function. The framework should provide us with a PortsConfiguration

        // Get the node container from the NodeContext
        final var nodeContext = NodeContext.getContext();
        if (nodeContext == null) {
            throw new IllegalStateException("A node context must be available when creating a ports configuration. "
                + "This is an implementation error.");
        }
        final var nodeContainer = nodeContext.getNodeContainer();

        // Count the number of the different ports (skip the flow var port)
        var numOutTables = 0;
        var numOutImages = 0;
        var numOutObjects = 0;
        for (int i = 1; i < nodeContainer.getNrOutPorts(); i++) {
            var portType = nodeContainer.getOutPort(i).getPortType();
            if (BufferedDataTable.TYPE.equals(portType)) {
                numOutTables++;
            } else if (ImagePortObject.TYPE.equals(portType)) {
                numOutImages++;
            } else if (PickledObjectFileStorePortObject.TYPE.equals(portType)) {
                numOutObjects++;
            } else {
                throw new IllegalStateException("Unsupported output port configured. This is an implementation error.");
            }
        }
        return new PythonScriptPortsConfiguration(numOutTables, numOutImages, numOutObjects);
    }

    private PythonScriptPortsConfiguration(final int numOutTables, final int numOutImages, final int numOutObjects) {
        m_numOutTables = numOutTables;
        m_numOutImages = numOutImages;
        m_numOutObjects = numOutObjects;
    }

    /**
     * @return the number of output tables
     */
    public int getNumOutTables() {
        return m_numOutTables;
    }

    /**
     * @return the number of output images
     */
    public int getNumOutImages() {
        return m_numOutImages;
    }

    /**
     * @return the number of output objects
     */
    public int getNumOutObjects() {
        return m_numOutObjects;
    }
}
