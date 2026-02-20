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
package org.knime.python3.scripting.nodes2;

import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.image.ImagePortObject;
import org.knime.core.node.workflow.NodeContext;
import org.knime.pixi.port.PythonEnvironmentPortObject;
import org.knime.python2.port.PickledObjectFileStorePortObject;

/**
 * Information about the configured ports of a Python scripting node.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class PythonScriptPortsConfiguration {

    /** Name of the object input port */
    public static final String PORTGR_ID_INP_OBJECT = "Input object (pickled)";

    /** Name of the table input port */
    public static final String PORTGR_ID_INP_TABLE = "Input table";

    /** Name of the table output port */
    public static final String PORTGR_ID_OUT_TABLE = "Output table";

    /** Name of the image output port */
    public static final String PORTGR_ID_OUT_IMAGE = "Output image";

    /** Name of the object output port */
    public static final String PORTGR_ID_OUT_OBJECT = "Output object (pickled)";

    /** Name of the Python environment port (accepts PythonEnvironmentPortObject) */
    public static final String PORTGR_ID_PYTHON_ENV = "Python environment";

    private final int m_numInTables;

    private final int m_numInObjects;

    private final int m_numOutTables;

    private final int m_numOutImages;

    private final int m_numOutObjects;

    private final boolean m_hasView;

    private final boolean m_hasPythonEnvironmentPort;

    /**
     * Create a new {@link PythonScriptPortsConfiguration} from the given {@link PortsConfiguration}.
     *
     * @param portsConfig
     * @return a new {@link PythonScriptPortsConfiguration}
     */
    static PythonScriptPortsConfiguration fromPortsConfiguration(final PortsConfiguration portsConfig,
        final boolean hasView) {
        // Get the number of different output ports from the ports configuration (ArrayUtils#getLength handles null)

        final Map<String, int[]> inPortsLocation = portsConfig.getInputPortLocation();
        final var numInTables = ArrayUtils.getLength(inPortsLocation.get(PORTGR_ID_INP_TABLE));
        final var numInObjects = ArrayUtils.getLength(inPortsLocation.get(PORTGR_ID_INP_OBJECT));
        // Check for environment port (accepts PythonEnvironmentPortObject)
        final var hasEnvironmentPort = inPortsLocation.containsKey(PORTGR_ID_PYTHON_ENV)
            && ArrayUtils.getLength(inPortsLocation.get(PORTGR_ID_PYTHON_ENV)) > 0;

        final Map<String, int[]> outPortsLocation = portsConfig.getOutputPortLocation();
        final var numOutTables = ArrayUtils.getLength(outPortsLocation.get(PORTGR_ID_OUT_TABLE));
        final var numOutImages = ArrayUtils.getLength(outPortsLocation.get(PORTGR_ID_OUT_IMAGE));
        final var numOutObjects = ArrayUtils.getLength(outPortsLocation.get(PORTGR_ID_OUT_OBJECT));
        return new PythonScriptPortsConfiguration(numInTables, numInObjects, numOutTables, numOutImages, numOutObjects,
            hasView, hasEnvironmentPort);
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
        var numInTables = 0;
        var numInObjects = 0;
        var hasEnvironmentPort = false;
        for (int i = 1; i < nodeContainer.getNrInPorts(); i++) {
            var portType = nodeContainer.getInPort(i).getPortType();
            if (BufferedDataTable.TYPE.equals(portType)) {
                numInTables++;
            } else if (PickledObjectFileStorePortObject.TYPE.equals(portType)) {
                numInObjects++;
            } else if (isPythonEnvironmentPort(portType)) {
                hasEnvironmentPort = true;
            } else {
                throw new IllegalStateException("Unsupported input port configured. This is an implementation error.");
            }
        }

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

        var hasView = nodeContainer.getNrViews() > 0;
        return new PythonScriptPortsConfiguration(numInTables, numInObjects, numOutTables, numOutImages, numOutObjects,
            hasView, hasEnvironmentPort);
    }

    private PythonScriptPortsConfiguration(final int numInTables, final int numInObjects, final int numOutTables,
        final int numOutImages, final int numOutObjects, final boolean hasView,
        final boolean hasPythonEnvironmentPort) {
        m_numInTables = numInTables;
        m_numInObjects = numInObjects;
        m_numOutTables = numOutTables;
        m_numOutImages = numOutImages;
        m_numOutObjects = numOutObjects;
        m_hasView = hasView;
        m_hasPythonEnvironmentPort = hasPythonEnvironmentPort;
    }

    /**
     * @return the number of input tables
     */
    public int getNumInTables() {
        return m_numInTables;
    }

    /**
     * @return the number of input objects
     */
    public int getNumInObjects() {
        return m_numInObjects;
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

    /**
     * @return whether the node has a view
     */
    public boolean hasView() {
        return m_hasView;
    }

    /**
     * @return if the node has a Python environment port (accepts PythonEnvironmentPortObject)
     */
    public boolean hasPythonEnvironmentPort() {
        return m_hasPythonEnvironmentPort;
    }

    /**
     * Check if a port type is a Python environment port.
     *
     * @param inType the port type to check
     * @return true if the port type is a Python environment port
     */
    private static boolean isPythonEnvironmentPort(final PortType inType) {
        return inType.equals(PythonEnvironmentPortObject.TYPE);
    }

    /**
     * Extract the Python environment port object from the input port objects
     *
     * @param inObjects the input port objects
     * @return the Python environment port object
     * @throws IllegalArgumentException if a Python environment port is expected but not found
     */
    public static PythonEnvironmentPortObject extractPythonEnvironmentPort(final PortObject[] inObjects) {
        for (final PortObject inObject : inObjects) {
            if (inObject instanceof PythonEnvironmentPortObject envPort) {
                return envPort;
            }
        }
        throw new IllegalArgumentException(
            "Expected a Python environment port object in the input ports, but none was found.");
    }
}
