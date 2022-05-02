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
 *   Mar 4, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.IContributor;
import org.eclipse.core.runtime.IExtension;
import org.eclipse.core.runtime.IExtensionPoint;
import org.eclipse.core.runtime.IExtensionRegistry;
import org.eclipse.core.runtime.Platform;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.FileUtil;

/**
 * Registry for the PythonExtension extension point at which bundled extensions are registered.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class PythonExtensionRegistry {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonExtensionRegistry.class);

    private static final String EXT_POINT_ID = "org.knime.python3.nodes.PythonExtension";

    static final List<PyExtensionEntry> PY_EXTENSIONS = collectPathsToPyExtensions();

    private static List<PyExtensionEntry> collectPathsToPyExtensions() {
        var extPoint = getExtensionPoint();
        return Stream.of(extPoint.getExtensions())//
            .flatMap(PythonExtensionRegistry::extractPathsToExtensions)//
            .collect(Collectors.toList());
    }

    private static IExtensionPoint getExtensionPoint() {
        final IExtensionRegistry registry = Platform.getExtensionRegistry();
        final IExtensionPoint point = registry.getExtensionPoint(EXT_POINT_ID);
        assert point != null : "Invalid extension point id: " + EXT_POINT_ID;
        return point;
    }

    private static Stream<PyExtensionEntry> extractPathsToExtensions(final IExtension extension) {
        var contributor = extension.getContributor();
        final var bundleName = contributor.getName();
        var bundle = Platform.getBundle(bundleName);
        return Stream.of(extension.getConfigurationElements())//
            .filter(PythonExtensionRegistry::isPyExtension)//
            .map(PythonExtensionRegistry::extractPluginRelativePath)//
            .map(p -> FileLocator.find(bundle, p))//
            .filter(u -> filterWithError(u, contributor))//
            .map(PythonExtensionRegistry::toPath)//
            .filter(Optional::isPresent)//
            .map(Optional::get)//
            .map(p -> new PyExtensionEntry(p, bundleName));
    }

    private static boolean filterWithError(final URL url, final IContributor contributor) {
        final var isNull = url == null;
        if (isNull) {
            LOGGER.errorWithFormat("Failed to resolve Python extension location provided by contributor %s.",
                contributor.getName());
        }
        return !isNull;
    }

    private static Optional<Path> toPath(final URL url) {
        try {
            return Optional.of(FileUtil.resolveToPath(FileLocator.toFileURL(url)));
        } catch (URISyntaxException ex) {
            LOGGER.error("The resolved file location is invalid.", ex);
        } catch (IOException ex) {
            LOGGER.error("Failed to convert a Eclipse URL to an absolute file URL.", ex);
        }
        return Optional.empty();
    }

    private static boolean isPyExtension(final IConfigurationElement element) {
        return element.getAttribute("ExtensionPath") != null;
    }

    private static org.eclipse.core.runtime.Path extractPluginRelativePath(final IConfigurationElement pyExtension) {
        var pathInPlugin = pyExtension.getAttribute("ExtensionPath");
        return new org.eclipse.core.runtime.Path(pathInPlugin);
    }

    static final class PyExtensionEntry {
        private final Path m_path;
        private final String m_bundleName;

        PyExtensionEntry(final Path path, final String bundleName) {
            m_path = path;
            m_bundleName = bundleName;
        }

        Path getPath() {
            return m_path;
        }

        String getBundleName() {
            return m_bundleName;
        }

    }

    private PythonExtensionRegistry() {

    }
}
