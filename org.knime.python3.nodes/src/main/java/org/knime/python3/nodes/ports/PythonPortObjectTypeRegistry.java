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
 *   12 May 2022 (chaubold): created
 */
package org.knime.python3.nodes.ports;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.python3.arrow.PythonArrowTableConverter;
import org.knime.python3.nodes.ports.PythonPortObjects.PortObjectProvider;
import org.knime.python3.nodes.ports.PythonPortObjects.PortObjectSpecProvider;
import org.knime.python3.nodes.ports.PythonPortObjects.PurePythonBinaryPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PurePythonTablePortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonBinaryPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonBinaryPortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonTablePortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonTablePortObjectSpec;

/**
 * A registry for {@link PythonPortObject}s that manages the types registered at the extension point.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class PythonPortObjectTypeRegistry {
    //    private static final String EXT_POINT_ID = "org.knime.python3.nodes.PythonPortObjectType";

    //    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonPortObjectTypeRegistry.class);

    // NOTE: The instance is initialized with the first access
    private static class InstanceHolder {
        private static final PythonPortObjectTypeRegistry INSTANCE = new PythonPortObjectTypeRegistry();
    }

    private final Map<String, Class<? extends PythonPortObject>> m_pythonPortObjectMap;

    private final Map<String, Class<? extends PythonPortObject>> m_pythonPortObjectInterfaceMap;

    private final Map<String, Class<? extends PythonPortObjectSpec>> m_pythonPortObjectSpecMap;

    private PythonPortObjectTypeRegistry() {
        // TODO: actually make this a registry. See https://knime-com.atlassian.net/browse/AP-18368
        m_pythonPortObjectMap = new HashMap<>();
        m_pythonPortObjectInterfaceMap = new HashMap<>();
        m_pythonPortObjectSpecMap = new HashMap<>();

        m_pythonPortObjectMap.put(BufferedDataTable.class.getName(), PythonTablePortObject.class);
        m_pythonPortObjectMap.put(PythonBinaryBlobFileStorePortObject.class.getName(), PythonBinaryPortObject.class);

        m_pythonPortObjectInterfaceMap.put(BufferedDataTable.class.getName(), PurePythonTablePortObject.class);
        m_pythonPortObjectInterfaceMap.put(PythonBinaryBlobFileStorePortObject.class.getName(),
            PurePythonBinaryPortObject.class);

        m_pythonPortObjectSpecMap.put(DataTableSpec.class.getName(), PythonTablePortObjectSpec.class);
        m_pythonPortObjectSpecMap.put(PythonBinaryBlobPortObjectSpec.class.getName(), PythonBinaryPortObjectSpec.class);
    }

    /**
     * Convert from a {@link PortObjectSpec} to a {@link PythonPortObjectSpec} using the registered conversion rules.
     *
     * @param spec The {@link PortObjectSpec}
     * @return The {@link PythonPortObjectSpec}
     */
    public static PythonPortObjectSpec convertToPythonPortObjectSpec(final PortObjectSpec spec) {
        if (spec == null) {
            throw new IllegalStateException("Cannot convert null spec from KNIME to Python");
        }
        var registry = InstanceHolder.INSTANCE;
        var clazz = registry.m_pythonPortObjectSpecMap.get(spec.getClass().getName());
        if (clazz == null) {
            throw new IllegalStateException("No PythonPortObjectSpec found for " + spec.getClass().getName());
        }
        try {
            return clazz.getConstructor(spec.getClass()).newInstance(spec);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException ex) {
            throw new IllegalStateException(
                "Could not find or create PythonPortObjectSpec for " + spec.getClass().getName(), ex);
        }
    }

    /**
     * Convert from a {@link PythonPortObjectSpec} to a {@link PortObjectSpec} using the registered matching port object
     * spec types.
     *
     * @param spec The {@link PythonPortObjectSpec}
     * @return The {@link PortObjectSpec}
     */
    public static PortObjectSpec convertFromPythonPortObjectSpec(final PythonPortObjectSpec spec) {
        if (spec == null) {
            throw new IllegalStateException("Cannot convert null spec from Python to KNIME");
        }
        var registry = InstanceHolder.INSTANCE;
        var clazz = registry.m_pythonPortObjectSpecMap.get(spec.getJavaClassName());
        if (clazz == null) {
            throw new IllegalStateException("No PortObjectSpec found for " + spec.getJavaClassName());
        }

        var payload = spec.toJsonString();
        Method factory;
        try {
            factory = clazz.getMethod("fromJsonString", String.class);
            final var object = factory.invoke(null, payload);
            return ((PortObjectSpecProvider)object).getPortObjectSpec();
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException ex) {
            throw new IllegalStateException(
                "Could not instantiate PortObjectSpec from JSON for " + spec.getJavaClassName(), ex);
        }
    }

    /**
     * Convert from a {@link PortObject} to a {@link PythonPortObject} using the registered conversion rules.
     *
     * @param portObject The {@link PortObject} to convert
     * @param tableConverter The {@link PythonArrowTableConverter} to use when converting {@link BufferedDataTable}s.
     * @return The {@link PythonPortObject}
     */
    public static PythonPortObject convertToPythonPortObject(final PortObject portObject,
        final PythonArrowTableConverter tableConverter) {
        if (portObject == null) {
            throw new IllegalStateException("Cannot convert null portObject from KNIME to Python");
        }
        var registry = InstanceHolder.INSTANCE;
        var clazz = registry.m_pythonPortObjectMap.get(portObject.getClass().getName());
        if (clazz == null) {
            throw new IllegalStateException("No PythonPortObject found for " + portObject.getClass().getName());
        }
        try {
            return clazz.getConstructor(portObject.getClass(), PythonArrowTableConverter.class).newInstance(portObject,
                tableConverter);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException ex) {
            throw new IllegalStateException(
                "Could not find or create PythonPortObject for " + portObject.getClass().getName(), ex);
        }
    }

    /**
     * Convert from a {@link PythonPortObject} to a {@link PortObject} using the registered conversion rules.
     *
     * @param pythonPortObject The {@link PythonPortObject}
     * @param tableConverter The {@link PythonArrowTableConverter} to use when converting {@link BufferedDataTable}s
     * @param execContext The {@link ExecutionContext} to use when accessing table internals
     * @return The {@link PortObject}
     */
    public static PortObject convertFromPythonPortObject(final PythonPortObject pythonPortObject,
        final PythonArrowTableConverter tableConverter, final ExecutionContext execContext) {
        if (pythonPortObject == null) {
            throw new IllegalStateException("Cannot convert 'null' port object from Python to KNIME");
        }

        if (pythonPortObject instanceof PortObjectProvider) {
            return ((PortObjectProvider)pythonPortObject).getPortObject();
        }

        var registry = InstanceHolder.INSTANCE;
        var clazz = registry.m_pythonPortObjectMap.get(pythonPortObject.getJavaClassName());
        if (clazz == null) {
            throw new IllegalStateException("No PortObject found for " + pythonPortObject.getJavaClassName());
        }
        var interfazze = registry.m_pythonPortObjectInterfaceMap.get(pythonPortObject.getJavaClassName());
        if (interfazze == null) {
            throw new IllegalStateException("No dedicated interface for the Python return value found for " //
                + pythonPortObject.getJavaClassName());
        }

        Method factory;
        try {
            factory =
                clazz.getMethod("fromPurePython", interfazze, PythonArrowTableConverter.class, ExecutionContext.class);
            final var object = factory.invoke(null, pythonPortObject, tableConverter, execContext);
            return ((PortObjectProvider)object).getPortObject();
        } catch (NoSuchElementException | SecurityException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException ex) {
            throw new IllegalStateException("Could not instantiate PortObject from Python representation for "
                + pythonPortObject.getJavaClassName(), ex);
        }
    }
}
