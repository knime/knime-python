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
import org.knime.core.data.filestore.FileStore;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.image.ImagePortObject;
import org.knime.core.node.port.image.ImagePortObjectSpec;
import org.knime.credentials.base.CredentialPortObject;
import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.python3.arrow.PythonArrowTableConverter;
import org.knime.python3.nodes.ports.PythonPortObjects.PortObjectProvider;
import org.knime.python3.nodes.ports.PythonPortObjects.PortObjectSpecProvider;
import org.knime.python3.nodes.ports.PythonPortObjects.PurePythonBinaryPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PurePythonConnectionPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PurePythonCredentialPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PurePythonImagePortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PurePythonTablePortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonBinaryPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonBinaryPortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonConnectionPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonConnectionPortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonCredentialPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonCredentialPortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonImagePortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonImagePortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonTablePortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonTablePortObjectSpec;

/**
 * A registry for {@link PythonPortObject}s that manages the types registered at the extension point.
 *
 * Interface contracts used via reflection:
 *
 * {@link PythonPortObjectSpec}s are expected to have a constructor that take an instance of the corresponding
 * {@link PortObjectSpec} as input, as well as a "public static fromJsonString(String)" factory method to be created
 * from a JSON encoded string representation.
 *
 * {@link PythonPortObject}s are expected to have a constructor that takes an instance of the corresponding
 * {@link PortObject} as well as a {@link PythonArrowTableConverter}. And they should offer a factory method
 * "fromPurePython" with arguments: 1. Pure Python interface (as registered in the m_pythonPortObjectInterfaceMap), 2. a
 * map of {@link String} keys to {@link FileStore}s which may contain additional data, 3. the
 * {@link PythonArrowTableConverter} and 4. the current {@link ExecutionContext}.
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

    // Port object types going from KNIME to Python
    private final Map<String, Class<? extends PythonPortObject>> m_pythonPortObjectMap;

    // Port object interfaces that are implemented in Python and passed to KNIME
    private final Map<String, Class<? extends PythonPortObject>> m_pythonPortObjectInterfaceMap;

    // Port object specs
    private final Map<String, Class<? extends PythonPortObjectSpec>> m_pythonPortObjectSpecMap;

    private PythonPortObjectTypeRegistry() {
        // TODO: PortGroup <?>
        // TODO: actually make this a registry. See https://knime-com.atlassian.net/browse/AP-18368
        m_pythonPortObjectMap = new HashMap<>();
        m_pythonPortObjectInterfaceMap = new HashMap<>();
        m_pythonPortObjectSpecMap = new HashMap<>();

        m_pythonPortObjectMap.put(BufferedDataTable.class.getName(), PythonTablePortObject.class);
        m_pythonPortObjectMap.put(PythonBinaryBlobFileStorePortObject.class.getName(), PythonBinaryPortObject.class);
        m_pythonPortObjectMap.put(PythonTransientConnectionPortObject.class.getName(),
            PythonConnectionPortObject.class);
        m_pythonPortObjectMap.put(ImagePortObject.class.getName(), PythonImagePortObject.class);
        m_pythonPortObjectMap.put(CredentialPortObject.class.getName(), PythonCredentialPortObject.class);

        m_pythonPortObjectInterfaceMap.put(BufferedDataTable.class.getName(), PurePythonTablePortObject.class);
        m_pythonPortObjectInterfaceMap.put(PythonBinaryBlobFileStorePortObject.class.getName(),
            PurePythonBinaryPortObject.class);
        m_pythonPortObjectInterfaceMap.put(PythonTransientConnectionPortObject.class.getName(),
            PurePythonConnectionPortObject.class);
        m_pythonPortObjectInterfaceMap.put(ImagePortObject.class.getName(), PurePythonImagePortObject.class);
        m_pythonPortObjectInterfaceMap.put(CredentialPortObject.class.getName(), PurePythonCredentialPortObject.class);

        m_pythonPortObjectSpecMap.put(DataTableSpec.class.getName(), PythonTablePortObjectSpec.class);
        m_pythonPortObjectSpecMap.put(PythonBinaryBlobPortObjectSpec.class.getName(), PythonBinaryPortObjectSpec.class);
        m_pythonPortObjectSpecMap.put(PythonTransientConnectionPortObjectSpec.class.getName(),
            PythonConnectionPortObjectSpec.class);
        m_pythonPortObjectSpecMap.put(ImagePortObjectSpec.class.getName(), PythonImagePortObjectSpec.class);
        m_pythonPortObjectSpecMap.put(CredentialPortObjectSpec.class.getName(), PythonCredentialPortObjectSpec.class);
    }

    /**
     * Convert from a {@link PortObjectSpec} to a {@link PythonPortObjectSpec} using the registered conversion rules.
     *
     * @param spec The {@link PortObjectSpec}
     * @return The {@link PythonPortObjectSpec} {@code null} if spec is {@code null}
     */
    public static PythonPortObjectSpec convertToPythonPortObjectSpec(final PortObjectSpec spec) {
        if (spec == null) {
            return null;
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
            return null;
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
     * When a {@link PythonPortObject} is converted to a KNIME {@link PortObject}, it can be either a PythonPortObject
     * or a PurePythonPortObject, where the latter is actually implemented on the Python side. For those, we need to
     * have a mapping registered from PortObject fully qualified classname to PurePythonPortObject interface so we can
     * find the {@link PythonPortObject} factory method with the correct inputs via reflection. The created
     * {@link PythonPortObject} is then also one that implements {@link PortObjectProvider} and used to finally return
     * the {@link PortObject}.
     *
     * @param pythonPortObject The {@link PythonPortObject}
     * @param fileStoresByKey A map of {@link FileStore}s that could have been created during node execution
     * @param tableConverter The {@link PythonArrowTableConverter} to use when converting {@link BufferedDataTable}s
     * @param execContext The {@link ExecutionContext} to use when accessing table internals
     * @return The {@link PortObject}
     */
    public static PortObject convertFromPythonPortObject(final PythonPortObject pythonPortObject,
        final Map<String, FileStore> fileStoresByKey, final PythonArrowTableConverter tableConverter,
        final ExecutionContext execContext) {
        if (pythonPortObject == null) {
            throw new IllegalStateException("Cannot convert 'null' port object from Python to KNIME");
        }

        if (pythonPortObject instanceof PortObjectProvider portObjectProvider) {
            return portObjectProvider.getPortObject();
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
            factory = clazz.getMethod("fromPurePython", interfazze, Map.class, PythonArrowTableConverter.class,
                ExecutionContext.class);
            final var object = factory.invoke(null, pythonPortObject, fileStoresByKey, tableConverter, execContext);
            return ((PortObjectProvider)object).getPortObject();
        } catch (InvocationTargetException ex) {
            // If #fromPurePython threw an exception we just use the message of this exception
            throw new IllegalStateException(ex.getCause().getMessage(), ex);
        } catch (NoSuchElementException | SecurityException | IllegalAccessException | IllegalArgumentException
                | NoSuchMethodException ex) {
            throw new IllegalStateException("Could not instantiate PortObject from Python representation for "
                + pythonPortObject.getJavaClassName(), ex);
        }
    }
}
