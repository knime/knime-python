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

import static org.knime.python3.types.port.PortObjectConverterExtensionPoint.getKnimeToPyConverters;
import static org.knime.python3.types.port.PortObjectConverterExtensionPoint.getPyToKnimeConverters;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.image.ImagePortObject;
import org.knime.core.node.port.image.ImagePortObjectSpec;
import org.knime.core.node.workflow.capture.WorkflowPortObject;
import org.knime.core.node.workflow.capture.WorkflowPortObjectSpec;
import org.knime.credentials.base.CredentialPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObjectSpec;
import org.knime.python3.nodes.ports.converters.PortObjectConversionContext;
import org.knime.python3.nodes.ports.converters.PortObjectConverterInterfaces.KnimeToPythonPortObjectConverter;
import org.knime.python3.nodes.ports.converters.PortObjectConverterInterfaces.PythonPortObjectConverter;
import org.knime.python3.nodes.ports.converters.PortObjectConverterInterfaces.PythonToKnimePortObjectConverter;
import org.knime.python3.nodes.ports.converters.PortObjectConverters;
import org.knime.python3.nodes.ports.converters.PortObjectSpecConverterInterfaces.KnimeToPythonPortObjectSpecConverter;
import org.knime.python3.nodes.ports.converters.PortObjectSpecConverterInterfaces.PythonPortObjectSpecConverter;
import org.knime.python3.nodes.ports.converters.PortObjectSpecConverterInterfaces.PythonToKnimePortObjectSpecConverter;
import org.knime.python3.nodes.ports.converters.PortObjectSpecConverters;
import org.knime.python3.nodes.ports.extension.ExtensionPortObjectConverterRegistry;
import org.knime.python3.nodes.ports.extension.ExtensionPortObjectConverters;
import org.knime.python3.nodes.ports.extension.ExtensionPortObjectConverters.NoConverterFoundException;
import org.knime.python3.nodes.ports.extension.ExtensionPortObjectConverters.PythonExtensionPortObject;
import org.knime.python3.nodes.ports.extension.ExtensionPortObjectConverters.PythonExtensionPortObjectSpec;
import org.knime.python3.types.port.PythonPortObjectConverterExtension;
import org.knime.python3.types.port.converter.PortObjectSpecConversionContext;
import org.knime.workflowservices.connection.AbstractHubAuthenticationPortObject;

/**
 * A registry that manages conversion of {@link PortObject}s and {@link PortObjectSpec}s from KNIME to Python and back.
 * This is done by using {@link PortObjectConverters} and {@link PortObjectSpecConverters} respectively.
 *
 * The registry maintains two maps with converters for Port Objects and Port Object Specs, as well as a map linking
 * fully qualified Java class names of KNIME-native Port Objects and Port Object Specs to their actual {@link Class}es.
 * The latter is needed since we get the string of the class name from objects returned from Python, and we need the
 * corresponding class for inheritance resolution in the converter maps.
 *
 ** @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 ** @author Ivan Prigarin, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction")
public final class PythonPortTypeRegistry {

    // Lazy-loaded singleton initialised on first access
    private static class InstanceHolder {
        private static final PythonPortTypeRegistry INSTANCE = new PythonPortTypeRegistry();
    }

    private final ExtensionPortObjectConverters m_extensionConverters;

    private final ExtensionPortObjectConverterRegistry m_extensionConverterRegistry;

    private final Map<Class<?>, PythonPortObjectConverter> m_builtinPortObjectConverterMap;

    private final Map<Class<?>, PythonPortObjectSpecConverter> m_builtinPortObjectSpecConverterMap;

    private final Map<String, Class<?>> m_classNameToClassMap;

    private PythonPortTypeRegistry() {
        m_classNameToClassMap = new HashMap<>();
        m_extensionConverterRegistry = new ExtensionPortObjectConverterRegistry(
            getKnimeToPyConverters().stream().map(PythonPortObjectConverterExtension::converter),
            getPyToKnimeConverters().stream().map(PythonPortObjectConverterExtension::converter));
        m_extensionConverters = new ExtensionPortObjectConverters(m_extensionConverterRegistry);

        m_builtinPortObjectConverterMap = new HashMap<>();
        m_builtinPortObjectSpecConverterMap = new HashMap<>();
        registerBuiltinPortTypeConverters();
    }

    /**
     * Register converters for Port Types that are implemented in the `knime-python` repository.
     */
    private void registerBuiltinPortTypeConverters() {
        m_builtinPortObjectConverterMap.put(BufferedDataTable.class,
            new PortObjectConverters.TablePortObjectConverter());
        m_builtinPortObjectSpecConverterMap.put(DataTableSpec.class,
            new PortObjectSpecConverters.TablePortObjectSpecConverter());

        m_builtinPortObjectConverterMap.put(PythonBinaryBlobFileStorePortObject.class,
            new PortObjectConverters.PythonBinaryPortObjectConverter());
        m_builtinPortObjectSpecConverterMap.put(PythonBinaryBlobPortObjectSpec.class,
            new PortObjectSpecConverters.PythonBinaryPortObjectSpecConverter());

        m_builtinPortObjectConverterMap.put(ImagePortObject.class, new PortObjectConverters.ImagePortObjectConverter());
        m_builtinPortObjectSpecConverterMap.put(ImagePortObjectSpec.class,
            new PortObjectSpecConverters.PythonImagePortObjectSpecConverter());

        m_builtinPortObjectConverterMap.put(PythonTransientConnectionPortObject.class,
            new PortObjectConverters.PythonConnectionPortObjectConverter());
        m_builtinPortObjectSpecConverterMap.put(PythonTransientConnectionPortObjectSpec.class,
            new PortObjectSpecConverters.PythonConnectionPortObjectSpecConverter());

        m_builtinPortObjectConverterMap.put(WorkflowPortObject.class,
            new PortObjectConverters.PythonWorkflowPortObjectConverter());
        m_builtinPortObjectSpecConverterMap.put(WorkflowPortObjectSpec.class,
            new PortObjectSpecConverters.PythonWorkflowPortObjectSpecConverter());

    }

    /**
     * @param identifier The string identifier used in Python code to determine the PortType
     * @return The port type for the given identifier.
     */
    public static PortType getPortTypeForIdentifier(final String identifier) {
        var extensionPortType = InstanceHolder.INSTANCE.m_extensionConverterRegistry.getPortType(identifier);
        if (extensionPortType != null) {
            return extensionPortType;
        } else if (identifier.equals("PortType.TABLE")) {
            return BufferedDataTable.TYPE;
        } else if (identifier.startsWith("PortType.BINARY")) {
            return PythonBinaryBlobFileStorePortObject.TYPE;
        } else if (identifier.startsWith("ConnectionPortType")) {
            return PythonTransientConnectionPortObject.TYPE;
        } else if (identifier.startsWith("PortType.IMAGE")) {
            return ImagePortObject.TYPE;
        } else if (identifier.startsWith("PortType.CREDENTIAL")) {
            return CredentialPortObject.TYPE;
        } else if (identifier.startsWith("PortType.HUB_AUTHENTICATION")) {
            return AbstractHubAuthenticationPortObject.TYPE;
        } else if (identifier.startsWith("PortType.WORKFLOW")) {
            return WorkflowPortObject.TYPE;
        } else {
            // for other custom ports
            return PythonBinaryBlobFileStorePortObject.TYPE;
        }
    }

    /**
     * Converts the provided {@link PortObjectSpec} implementor to the corresponding {@link PythonPortObjectSpec}
     * wrapper.
     *
     * @param spec KNIME-native {@link PortObjectSpec}
     * @return The Port Object Spec wrapped in {@link PythonPortObjectSpec}, which can be provided to the Python proxy
     */
    public static PythonPortObjectSpec convertPortObjectSpecToPython(final PortObjectSpec spec) {
        if (spec == null) {
            return null;
        }

        var instance = InstanceHolder.INSTANCE;

        try {
            return instance.m_extensionConverters.convertSpecToPython(spec, new PortObjectSpecConversionContext() {
            });
        } catch (NoConverterFoundException ex) { // NOSONAR
            // fine, the code below checks whether we have a builtin converter available
        }

        PythonPortObjectSpecConverter converter =
            findConverterForClass(spec.getClass(), instance.m_builtinPortObjectSpecConverterMap);

        if (converter == null) {
            throw new IllegalStateException("No Port Object Spec converter found for " + spec.getClass().getName());
        }

        if (converter instanceof KnimeToPythonPortObjectSpecConverter) {
            @SuppressWarnings("unchecked")
            KnimeToPythonPortObjectSpecConverter<PortObjectSpec, PythonPortObjectSpec> knimeToPythonConverter =
                (KnimeToPythonPortObjectSpecConverter<PortObjectSpec, PythonPortObjectSpec>)converter;
            return knimeToPythonConverter.toPython(spec);
        } else {
            throw new IllegalStateException("Registered Port Object Spec converter for " + spec.getClass().getName()
                + " does not implement KNIME to Python conversion.");
        }
    }

    /**
     * Converts the provided {@link PythonPortObjectSpec} received from Python to the corresponding KNIME-native
     * {@link PortObjectSpec}.
     *
     * @param pythonSpec The Port Object Spec to be converted back to its KNIME-native {@link PortObjectSpec}
     *            counterpart
     * @return The KNIME-native {@link PortObjectSpec} extracted from the JSON encoding of the
     *         {@link PythonPortObjectSpec}
     */
    public static PortObjectSpec convertPortObjectSpecFromPython(final PythonPortObjectSpec pythonSpec) {
        if (pythonSpec == null) {
            return null;
        }

        var instance = InstanceHolder.INSTANCE;
        String specClassName = pythonSpec.getJavaClassName();
        if (pythonSpec instanceof PythonExtensionPortObjectSpec extensionSpec) {
            try {
                return instance.m_extensionConverters.convertSpecFromPython(extensionSpec,
                    new PortObjectSpecConversionContext() {
                });
            } catch (NoConverterFoundException ex) { // NOSONAR
                // fine, the code below checks whether we have a builtin converter available
            }
    }

        var specClass = instance.getClassFromClassName(specClassName);
        PythonPortObjectSpecConverter converter =
            findConverterForClass(specClass, instance.m_builtinPortObjectSpecConverterMap);

        if (converter == null) {
            throw new IllegalStateException("No Port Object Spec converter found for " + specClassName);
        }

        var payload = pythonSpec.toJsonString();

        if (converter instanceof PythonToKnimePortObjectSpecConverter) {
            @SuppressWarnings("unchecked")
            PythonToKnimePortObjectSpecConverter<PortObjectSpec> pythonToKnimeConverter =
                (PythonToKnimePortObjectSpecConverter<PortObjectSpec>)converter;
            return pythonToKnimeConverter.fromJsonString(payload);
        } else {
            throw new IllegalStateException("Registered Port Object Spec converter for " + specClassName
                + " does not implement Python to KNIME conversion.");
        }
    }

    /**
     * Converts the provided {@link PortObject} implementor to the corresponding {@link PythonPortObject} wrapper.
     *
     * @param portObject KNIME-native {@link PortObject}
     * @param context The conversion context providing objects needed during the conversion process
     * @return The Port Object wrapped in {@link PythonPortObject}, which can be provided to the Python proxy
     */
    public static PythonPortObject convertPortObjectToPython(final PortObject portObject,
        final PortObjectConversionContext context) {
        if (portObject == null) {
            throw new IllegalStateException("Cannot convert `null` portObject from KNIME to Python");
        }

        var instance = InstanceHolder.INSTANCE;

        try {
            return instance.m_extensionConverters.convertObjectToPython(portObject, context);
        } catch (NoConverterFoundException ex) { // NOSONAR
            // fine, the code below checks whether we have a builtin converter available
        }

        PythonPortObjectConverter converter =
            findConverterForClass(portObject.getClass(), instance.m_builtinPortObjectConverterMap);

        if (converter == null) {
            throw new IllegalStateException("No Port Object converter found for " + portObject.getClass().getName());
        }

        if (converter instanceof KnimeToPythonPortObjectConverter) {
            @SuppressWarnings("unchecked")
            KnimeToPythonPortObjectConverter<PortObject, PythonPortObject> knimeToPythonConverter =
                (KnimeToPythonPortObjectConverter<PortObject, PythonPortObject>)converter;
            return knimeToPythonConverter.toPython(portObject, context);
        } else {
            throw new IllegalStateException("Registered Port Object converter for " + portObject.getClass().getName()
                + " does not implement KNIME to Python conversion.");
        }
    }

    /**
     * Converts the provided array of KNIME-native {@link PortObject} implementors to the corresponding
     * {@link PythonPortObject} wrappers.
     *
     * @param inData The stream of Port Objects to be converted to their Python-native {@link PythonPortObject}
     * @param knimeToPythonConversionContext context needed for the conversion
     * @return The Port Object array wrapped in {@link PythonPortObject}, which can be provided to the Python proxy
     */
    public static PythonPortObject[] convertPortObjectsToPython(final Stream<PortObject> inData,
        final PortObjectConversionContext knimeToPythonConversionContext) {
        return inData
            .map(po -> PythonPortTypeRegistry.convertPortObjectToPython(po, knimeToPythonConversionContext))
            .toArray(PythonPortObject[]::new);
    }


    /**
     * Converts the provided PurePythonPortObject-interfaced object received from Python to the corresponding
     * KNIME-native {@link PortObject}.
     *
     * @param purePythonPortObject The `PurePython` Port Object to be converted back to its KNIME-native
     *            {@link PortObject} counterpart
     * @param context The conversion context providing objects needed during the conversion process
     * @return The KNIME-native {@link PortObject} extracted from the `PurePython` object
     */
    public static PortObject convertPortObjectFromPython(final PythonPortObject purePythonPortObject,
        final PortObjectConversionContext context) {
        if (purePythonPortObject == null) {
            throw new IllegalStateException("Cannot convert 'null' portObject from Python to KNIME");
        }

        var instance = InstanceHolder.INSTANCE;

        String javaClassName = purePythonPortObject.getJavaClassName();

        if (purePythonPortObject instanceof PythonExtensionPortObject extensionPortObject) {
            try {
                return instance.m_extensionConverters.convertObjFromPython(extensionPortObject,
                    context);
            } catch (NoConverterFoundException ex) { // NOSONAR
                // fine, the code below checks whether we have a builtin converter available
            }
        }

        var portObjectClass = instance.getClassFromClassName(javaClassName);
        PythonPortObjectConverter converter =
            findConverterForClass(portObjectClass, instance.m_builtinPortObjectConverterMap);

        if (converter == null) {
            throw new IllegalStateException("No Port Object converter found for " + javaClassName);
        }

        if (converter instanceof PythonToKnimePortObjectConverter) {
            @SuppressWarnings("unchecked")
            PythonToKnimePortObjectConverter<PythonPortObject, PortObject> pythonToKnimeConverter =
                (PythonToKnimePortObjectConverter<PythonPortObject, PortObject>)converter;
            return pythonToKnimeConverter.fromPython(purePythonPortObject, context);
        } else {
            throw new IllegalStateException("Registered Port Object converter for " + javaClassName
                + " does not implement Python to KNIME conversion.");
        }
    }

    /**
     * Converts the provided array of {@link PythonPortObject} implementors received from Python to the corresponding
     * KNIME-native {@link PortObject PortObjects}
     *
     * @param inData The stream of Port Objects to be converted back to their KNIME-native {@link PortObject}
     *            counterparts
     * @param pythonToKnimeConversionContext context needed for the conversion
     * @return The KNIME-native {@link PortObject} array extracted from the `PurePython` objects
     */
    public static PortObject[] convertPortObjectsFromPython(final Stream<PythonPortObject> inData,
        final PortObjectConversionContext pythonToKnimeConversionContext) {
        return inData
            .map(po -> PythonPortTypeRegistry.convertPortObjectFromPython(po, pythonToKnimeConversionContext))
            .toArray(PortObject[]::new);
    }

    /**
     * Searches for a registered converter for the given Port Object or Port Object Spec class by traversing its class
     * hierarchy until a match is found.
     *
     * If no exact match is found, interfaces the class implements are considered first, then the next superclass.
     *
     * @param <T> Converter type specified by the caller
     * @param targetClass The class object whose converter is to be found
     * @param classToConverterMap The map containing class-to-converter mappings
     * @return The converter instance if found; null otherwise
     */
    private static <T> T findConverterForClass(final Class<?> targetClass, final Map<Class<?>, T> classToConverterMap) {
        Class<?> currentClass = targetClass;

        while (currentClass != null) {
            T converter = classToConverterMap.get(currentClass);
            if (converter != null) {
                return converter;
            }

            // check for interfaces if no match found
            for (Class<?> interfaceClass : currentClass.getInterfaces()) {
                converter = classToConverterMap.get(interfaceClass);
                if (converter != null) {
                    return converter;
                }
            }

            // move to superclass
            // TODO: we can do an early stop if we reach some sensible traversal depth or by defining a "stop"/"base" Class
            // after which it no longer makes sense to keep the traversal going
            currentClass = currentClass.getSuperclass();
        }
        return null;
    }

    /**
     * Retrieves a {@link Class} object associated with the specified class name. If the class is not found in the cache
     * map, it attempts to load the class dynamically.
     *
     * We receive Java class names as strings from the Python side, and this method allows us to get the corresponding
     * Class object to then perform a class hieararchy-aware converter lookup.
     *
     * @param className The fully qualified name of the class to retrieve
     * @return The {@link Class} object corresponding to the given class name
     */
    private Class<?> getClassFromClassName(final String className) {
        Class<?> retrievedClass = m_classNameToClassMap.get(className);

        if (retrievedClass == null) {
            try {
                retrievedClass = Class.forName(className);
                m_classNameToClassMap.put(className, retrievedClass);
            } catch (ClassNotFoundException ex) {
                throw new IllegalStateException("Could not find Java class for class name " + className);
            }
        }

        return retrievedClass;
    }

}
