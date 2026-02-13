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
 *   Nov 3, 2022 (benjamin): created
 */
package org.knime.python3.scripting.nodes;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.image.ImagePortObject;
import org.knime.pixi.port.PythonEnvironmentPortObject;
import org.knime.python2.port.PickledObjectFileStorePortObject;
import org.knime.python2.ports.DataTableInputPort;
import org.knime.python2.ports.DataTableOutputPort;
import org.knime.python2.ports.ImageOutputPort;
import org.knime.python2.ports.InputPort;
import org.knime.python2.ports.OutputPort;
import org.knime.python2.ports.PickledObjectInputPort;
import org.knime.python2.ports.PickledObjectOutputPort;


/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class PortsConfigurationUtils {

    private PortsConfigurationUtils() {
        // Utility class
    }

    /**
     * Check if the ports configuration contains a Python environment port.
     *
     * @param config the ports configuration
     * @return true if a Python environment port is present
     */
    public static boolean hasPythonEnvironmentPort(final PortsConfiguration config) {
        final PortType[] inTypes = config.getInputPorts();
        try {
            // Check if any input port is a PythonEnvironmentPortObject
            for (final PortType inType : inTypes) {
                if (isPythonEnvironmentPort(inType)) {
                    return true;
                }
            }
        } catch (NoClassDefFoundError e) {
            // Python environment bundle is not available - this is fine since it's optional
        }
        return false;
    }

    /**
     * Extract the input ports from the given ports configuration.
     *
     * @param config the ports configuration
     * @return the input ports which can be used in the node model
     */
    public static InputPort[] createInputPorts(final PortsConfiguration config) {
        final PortType[] inTypes = config.getInputPorts();
        int inTableIndex = 0;
        int inObjectIndex = 0;
        // Count non-environment ports for the result array
        int numNonEnvironmentPorts = 0;
        for (final PortType inType : inTypes) {
            if (!isPythonEnvironmentPort(inType)) {
                numNonEnvironmentPorts++;
            }
        }
        final var inPorts = new InputPort[numNonEnvironmentPorts];
        int portIndex = 0;
        for (int i = 0; i < inTypes.length; i++) {
            final PortType inType = inTypes[i];
            // Skip Python environment ports - they are not InputPorts in the traditional sense
            if (isPythonEnvironmentPort(inType)) {
                continue;
            }
            final InputPort inPort;
            if (BufferedDataTable.TYPE.equals(inType)) {
                inPort = new DataTableInputPort("knio.input_tables[" + inTableIndex++ + "]");
            } else if (PickledObjectFileStorePortObject.TYPE.equals(inType)) {
                inPort = new PickledObjectInputPort("knio.input_objects[" + inObjectIndex++ + "]");
            } else {
                throw new IllegalStateException("Unsupported input type: " + inType.getName());
            }
            inPorts[portIndex++] = inPort;
        }
        return inPorts;
    }

    /**
     * Check if a port type is a Python environment port.
     *
     * @param inType the port type to check
     * @return true if the port type is a Python environment port
     */
    public static boolean isPythonEnvironmentPort(final PortType inType) {
        try {
            return inType.equals(PythonEnvironmentPortObject.TYPE) || inType.equals(PythonEnvironmentPortObject.TYPE_OPTIONAL);
        } catch (NoClassDefFoundError e) {
            // Python environment bundle is not available - this is fine since it's optional
            return false;
        }
    }

    /**
     * Extract the output ports from the given ports configuration.
     *
     * @param config the ports configuration
     * @return the output ports which can be used in the node model
     */
    public static OutputPort[] createOutputPorts(final PortsConfiguration config) {
        final PortType[] outTypes = config.getOutputPorts();
        int outTableSuffix = 0;
        int outImageSuffix = 0;
        int outObjectSuffix = 0;
        final var outPorts = new OutputPort[outTypes.length];
        for (int i = 0; i < outTypes.length; i++) {
            final PortType outType = outTypes[i];
            final OutputPort outPort;
            if (BufferedDataTable.TYPE.equals(outType)) {
                outPort = new DataTableOutputPort("knio.output_tables[" + outTableSuffix++ + "]");
            } else if (ImagePortObject.TYPE.equals(outType)) {
                outPort = new ImageOutputPort("knio.output_images[" + outImageSuffix++ + "]");
            } else if (PickledObjectFileStorePortObject.TYPE.equals(outType)) {
                outPort = createPickledObjectOutputPort(outObjectSuffix++);
            } else {
                throw new IllegalStateException("Unsupported output type: " + outType.getName());
            }
            outPorts[i] = outPort;
        }
        return outPorts;
    }

    /**
     * Creates the OutputPort for pickled objects.
     *
     * @param outObjectSuffix the index of the output object
     * @return an OutputPort for pickled objects.
     */
    public static OutputPort createPickledObjectOutputPort(final int outObjectSuffix) {
        return new PickledObjectOutputPort("knio.output_objects[" + outObjectSuffix + "]");
    }

    /**
     * Extract the Python environment port object from the input port objects, if present.
     *
     * @param inObjects the input port objects
     * @return the Python environment port object, or null if not present
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

    /*public static void installPythonEnvironmentIfPresent(final PortsConfiguration config, final PortObject[] inObjects,
        final ExecutionMonitor exec) throws IOException, CanceledExecutionException {
        final PythonEnvironmentPortObject envPort = extractPythonEnvironmentPort(config, inObjects);
        if (envPort != null) {
            PythonEnvironmentPortObject.installPythonEnvironmentWithProgress(config, inObjects, exec);
        }
    }*/

    /**
     * Filter out the Python environment port from the input port objects array.
     * The environment port is not a data port and should not be passed to the session.
     *
     * @param inObjects the input port objects
     * @return the filtered array without the Python environment port
     */
    public static PortObject[] filterEnvironmentPort(final PortObject[] inObjects) {
        // Find and exclude the environment port
        final PortObject[] filtered = new PortObject[inObjects.length - 1];
        int filteredIndex = 0;
        for (int i = 0; i < inObjects.length; i++) {
            if (!(inObjects[i] instanceof PythonEnvironmentPortObject)) {
                filtered[filteredIndex++] = inObjects[i];
            }
        }
        return filtered;
    }
}