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
package org.knime.python2.extensions.serializationlibrary;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.IExtensionPoint;
import org.eclipse.core.runtime.IExtensionRegistry;
import org.eclipse.core.runtime.Platform;
import org.knime.core.node.NodeLogger;
import org.knime.python2.Activator;
import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibrary;
import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibraryFactory;

/**
 * Class administrating all {@link SerializationLibraryExtension}s.
 *
 * @author Clemens von Schwerin, KNIME.com, Konstanz, Germany
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class SerializationLibraryExtensions {

    private static final String EXT_POINT_ID = "org.knime.python2.serializationlibrary";

    private static final String EXT_POINT_ATTR_ID = "id";

    private static final String EXT_POINT_ATTR_JAVA_CLASS = "java-serializationlibrary-factory";

    private static final String EXT_POINT_ATTR_PYTHON_CLASS = "python-serializationlibrary";

    private static SerializationLibraryExtensions instance;

    /**
     * Initializes all registered {@link SerializationLibraryExtension}s.
     */
    public static synchronized void init() {
        getInitializedInstance();
    }

    private static synchronized SerializationLibraryExtensions getInitializedInstance() {
        if (instance == null) {
            instance = new SerializationLibraryExtensions();
        }
        return instance;
    }

    private final Map<String, SerializationLibraryExtension> m_extensions;

    private SerializationLibraryExtensions() {
        final Map<String, SerializationLibraryExtension> extensions = new HashMap<>();
        final IExtensionRegistry registry = Platform.getExtensionRegistry();
        final IExtensionPoint point = registry.getExtensionPoint(EXT_POINT_ID);
        if (point == null) {
            final String errorMessage = "Invalid extension point: '" + EXT_POINT_ID + "'.";
            NodeLogger.getLogger(SerializationLibraryExtensions.class).error(errorMessage);
            throw new IllegalStateException(errorMessage);
        }
        final IConfigurationElement[] configs =
            Platform.getExtensionRegistry().getConfigurationElementsFor(EXT_POINT_ID);
        for (final IConfigurationElement config : configs) {
            try {
                final String id = config.getAttribute(EXT_POINT_ATTR_ID);
                if (id != null && !id.isEmpty()) {
                    if (!extensions.containsKey(id)) {
                        final Object o = config.createExecutableExtension(EXT_POINT_ATTR_JAVA_CLASS);
                        if (o instanceof SerializationLibraryFactory) {
                            final String contributor = config.getContributor().getName();
                            final String pythonFilePath = config.getAttribute(EXT_POINT_ATTR_PYTHON_CLASS);
                            final File pythonFile = Activator.getFile(contributor, pythonFilePath);
                            if (pythonFile != null) {
                                final SerializationLibraryFactory serializationLibrary = (SerializationLibraryFactory)o;
                                extensions.put(id, new SerializationLibraryExtension(id, pythonFile.getAbsolutePath(),
                                    serializationLibrary));
                            } else {
                                NodeLogger.getLogger(SerializationLibraryExtensions.class)
                                    .error("Invalid extension '" + id + "' registered at extension point "
                                        + EXT_POINT_ID + ": Attribute '" + EXT_POINT_ATTR_PYTHON_CLASS
                                        + "' does not specify an existing Python module.");
                            }
                        } else {
                            NodeLogger.getLogger(SerializationLibraryExtensions.class)
                                .error("Invalid extension '" + id + "' registered at extension point " + EXT_POINT_ID
                                    + ": Attribute '" + EXT_POINT_ATTR_JAVA_CLASS
                                    + "' does not implement SerializationLibraryFactory.");
                        }
                    } else {
                        NodeLogger.getLogger(SerializationLibraryExtensions.class)
                            .error("Invalid extension '" + id + "' registered at extension point " + EXT_POINT_ID
                                + ": An extension of the same id is already registered.");
                    }
                } else {
                    NodeLogger.getLogger(SerializationLibraryExtensions.class)
                        .error("Invalid extension registered at extension point " + EXT_POINT_ID + ": Attribute '"
                            + EXT_POINT_ATTR_ID + "' is null or empty.");
                }
            } catch (final CoreException e) {
                NodeLogger.getLogger(SerializationLibraryExtensions.class).error(e.getMessage(), e);
            } catch (Exception e) {
                NodeLogger.getLogger(SerializationLibraryExtensions.class).error(e.getMessage(), e);
                throw e;
            }
        }
        m_extensions = Collections.unmodifiableMap(extensions);
    }

    /**
     * @return a collection of all available serialization library extensions
     */
    public static Collection<SerializationLibraryExtension> getExtensions() {
        return getInitializedInstance().m_extensions.values();
    }

    /**
     * Returns the serialization library factory from the extension of the given id.
     *
     * @param id the library extension's id
     * @return the corresponding serialization library factory
     * @throws IllegalArgumentException if no serialization library extension is available for the given id
     */
    public static SerializationLibraryFactory getSerializationLibraryFactory(final String id) {
        final Map<String, SerializationLibraryExtension> extensions =
            new HashMap<>(getInitializedInstance().m_extensions);
        final SerializationLibraryExtension extension = extensions.get(id);
        if (extension == null) {
            final String availableExtensions = Arrays.toString(extensions.entrySet().toArray());
            throw new IllegalArgumentException(
                "No serialization library available for id '" + id + "'. Available libraries: " + availableExtensions);
        }
        return extension.getJavaSerializationLibraryFactory();
    }

    /**
     * Creates a new serialization library instance from the extension of the given id.
     *
     * @param id the library extension's id
     * @return a serialization library
     * @throws IllegalArgumentException if no serialization library extension is available for the given id
     */
    public static SerializationLibrary getSerializationLibrary(final String id) {
        return getSerializationLibraryFactory(id).createInstance();
    }

    /**
     * Returns the path of the serialization library's Python module.
     *
     * @param id the library's id
     * @return the serialization library path
     * @throws IllegalArgumentException if no serialization library extension is available for the given id
     */
    public static String getSerializationLibraryPath(final String id) {
        final SerializationLibraryExtension extension = getInitializedInstance().m_extensions.get(id);
        if (extension == null) {
            throw new IllegalArgumentException("No serialization library path available for id '" + id + "'.");
        }
        return extension.getPythonSerializationLibraryPath();
    }

    /**
     * Returns the human readable name for a serialization library id.
     *
     * @param id a serialization library id
     * @return a human readable name if id matches, null otherwise
     */
    public static String getNameForId(final String id) {
        final SerializationLibraryExtension ext = getInitializedInstance().m_extensions.get(id);
        if (ext != null) {
            return ext.getJavaSerializationLibraryFactory().getName();
        }
        return null;
    }
}
