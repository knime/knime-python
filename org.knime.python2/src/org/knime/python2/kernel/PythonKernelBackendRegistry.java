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
 *   Jul 25, 2021 (marcel): created
 */
package org.knime.python2.kernel;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.IExtensionPoint;
import org.eclipse.core.runtime.IExtensionRegistry;
import org.eclipse.core.runtime.Platform;
import org.knime.core.node.NodeLogger;

/**
 * Not intended as a full-blown registry but rather as a way to make the new Python kernel back end from
 * org.knime.python3 known to org.knime.python2 without introducing a (circular) dependency to org.knime.python3. The
 * corresponding extension point is not intended to be extended by implementations other than
 * {@value #PYTHON2_BACKEND_ID} and {@value #PYTHON3_BACKEND_ID}.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class PythonKernelBackendRegistry {

    /**
     * The (reserved) identifier of the legacy (org.knime.python2) Python kernel back end.
     */
    public static final String PYTHON2_BACKEND_ID = "org.knime.python2.kernel.Python2KernelBackendFactory";

    /**
     * The (reserved) identifier of the new (org.knime.python3) Python kernel back end.
     */
    public static final String PYTHON3_BACKEND_ID = "org.knime.python3.legacy.Python3KernelBackendFactory";

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonKernelBackendRegistry.class);

    private static final String EXT_POINT_ID = "org.knime.python2.PythonKernelBackend";

    private static final String EXT_POINT_ATTR_CLASS = "PythonKernelBackendFactory";

    private static final PythonKernelBackendRegistry INSTANCE = new PythonKernelBackendRegistry();

    /**
     * @param identifier The identifier of the back end.
     * @return The back end that has the given identifier.
     * @throws IllegalArgumentException If no back end exists that has the given identifier.
     */
    public static PythonKernelBackendFactory getBackend(final String identifier) {
        final PythonKernelBackendFactory backend = INSTANCE.m_backends.get(identifier);
        if (backend == null) {
            throw new IllegalArgumentException("Python kernel back end does not exist: " + identifier);
        }
        return backend;
    }

    private final Map<String, PythonKernelBackendFactory> m_backends = new HashMap<>(2);

    private PythonKernelBackendRegistry() {
        try {
            final IExtensionRegistry registry = Platform.getExtensionRegistry();
            final IExtensionPoint point = registry.getExtensionPoint(EXT_POINT_ID);
            if (point == null) {
                final String msg = "Invalid extension point: '" + EXT_POINT_ID + "'.";
                LOGGER.error(msg);
                throw new IllegalStateException(msg);
            }
            for (final IConfigurationElement elem : point.getConfigurationElements()) {
                registerBackend(elem);
            }
        } catch (final Exception e) { // NOSONAR
            LOGGER.error(
                "An exception occurred while registering extensions at extension point '" + EXT_POINT_ID + "'.", e);
        }
    }

    private void registerBackend(final IConfigurationElement elem) {
        try {
            final PythonKernelBackendFactory backend =
                (PythonKernelBackendFactory)elem.createExecutableExtension(EXT_POINT_ATTR_CLASS);
            m_backends.put(backend.getIdentifier(), backend);
        } catch (final Exception t) { // NOSONAR
            final String extension = elem.getDeclaringExtension().getUniqueIdentifier();
            LOGGER.error(
                "An exception occurred while registering an extension at extension point '" + EXT_POINT_ID + "'.", t);
            LOGGER.error("Extension '" + extension + "' was ignored.", t);
        }
    }
}
