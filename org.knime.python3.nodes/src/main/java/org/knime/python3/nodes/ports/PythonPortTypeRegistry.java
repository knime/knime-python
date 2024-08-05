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
 *   2 August 2024 (Ivan Prigarin): created
 */
package org.knime.python3.nodes.ports;

import java.util.HashMap;
import java.util.Map;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.image.ImagePortObject;
import org.knime.credentials.base.CredentialPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObject;
import org.knime.python3.nodes.ports.converters.PortObjectConversionContext;
import org.knime.python3.nodes.ports.converters.PortObjectConverterInterfaces.KnimeToPythonPortObjectConverter;
import org.knime.python3.nodes.ports.converters.PortObjectConverterInterfaces.PortObjectConverterMarker;
import org.knime.python3.nodes.ports.converters.PortObjectConverterInterfaces.PythonToKnimePortObjectConverter;
import org.knime.python3.nodes.ports.converters.PortObjectConverters;

/**
 *
 * TODO
 *
 */
public final class PythonPortTypeRegistry {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonPortObjectTypeRegistry.class);

    private static class InstanceHolder {
        private static final PythonPortTypeRegistry INSTANCE = new PythonPortTypeRegistry();
    }

    private final Map<String, PortObjectConverterMarker> m_portObjectConverterMap;

    private PythonPortTypeRegistry() {
        m_portObjectConverterMap = new HashMap<>();

        m_portObjectConverterMap.put(BufferedDataTable.class.getName(), new PortObjectConverters.TablePortObjectConverter());
        m_portObjectConverterMap.put(PythonBinaryBlobFileStorePortObject.class.getName(), new PortObjectConverters.PythonBinaryPortObjectConverter());
        m_portObjectConverterMap.put(ImagePortObject.class.getName(), new PortObjectConverters.ImagePortObjectConverter());
        m_portObjectConverterMap.put(PythonTransientConnectionPortObject.class.getName(), new PortObjectConverters.PythonConnectionPortObjectConverter());
        m_portObjectConverterMap.put(CredentialPortObject.class.getName(), new PortObjectConverters.PythonCredentialsPortObjectConverter());
    }


    /**
     * Converts the provided {@link PortObject} implementor to the corresponding {@link PythonPortObject} wrapper.
     *
     * @param portObject KNIME-native {@link PortObject}
     * @param context The conversion context providing objects needed during the conversion process
     * @return The Port Object wrapped in {@link PythonPortObject}, which can be provided to the Python proxy
     */
    public static PythonPortObject convertToPython(final PortObject portObject, final PortObjectConversionContext context) {
        if (portObject == null) {
            throw new IllegalStateException("Cannot convert `null` portObject from KNIME to Python");
        }
        var registry = InstanceHolder.INSTANCE;
        PortObjectConverterMarker converter = registry.m_portObjectConverterMap.get(portObject.getClass().getName());

        if (converter == null) {
            throw new IllegalStateException("No converter found for " + portObject.getClass().getName());
        }

        if (converter instanceof KnimeToPythonPortObjectConverter) {
            @SuppressWarnings("unchecked")
            KnimeToPythonPortObjectConverter<PortObject, PythonPortObject> knimeToPythonConverter =
                    (KnimeToPythonPortObjectConverter<PortObject, PythonPortObject>) converter;
            return knimeToPythonConverter.toPython(portObject, context);
        } else {
            throw new IllegalStateException("Registered converter for " + portObject.getClass().getName() +
                " does not implement KNIME to Python conversion.");
        }
    }

    /**
     * Converts the provided PurePythonPortObject-interfaced object received from Python to the corresponding KNIME-native
     * {@link PortObject}.
     *
     * @param purePythonPortObject The `PurePython` Port Object to be converted back to its KNIME-native {@link PortObject} counterpart
     * @param context The conversion context providing objects needed during the conversion process
     * @return The KNIME-native {@link PortObject} extracted from the `PurePython` object
     */
    public static PortObject convertFromPython(final PythonPortObject purePythonPortObject, final PortObjectConversionContext context) {
        if (purePythonPortObject == null) {
            throw new IllegalStateException("Cannot convert 'null' portObject from Python to KNIME");
        }

        var registry = InstanceHolder.INSTANCE;
        PortObjectConverterMarker converter = registry.m_portObjectConverterMap.get(purePythonPortObject.getJavaClassName());

        if (converter == null) {
            throw new IllegalStateException("No converter found for " + purePythonPortObject.getJavaClassName());
        }

        if (converter instanceof PythonToKnimePortObjectConverter) {
            @SuppressWarnings("unchecked")
            PythonToKnimePortObjectConverter<PythonPortObject, PortObject> pythonToKnimeConverter =
                    (PythonToKnimePortObjectConverter<PythonPortObject, PortObject>) converter;
            return pythonToKnimeConverter.fromPython(purePythonPortObject, context);
        } else {
            throw new IllegalStateException("Registered converter for " + purePythonPortObject.getJavaClassName() +
                " does not implement Python to KNIME conversion.");
        }
    }
}
