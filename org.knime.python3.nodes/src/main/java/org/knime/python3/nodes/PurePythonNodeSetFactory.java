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
 *   Feb 21, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.extension.CategoryExtension;
import org.knime.python3.PythonGateway;
import org.knime.python3.nodes.PythonExtensionRegistry.PyExtensionEntry;
import org.knime.python3.nodes.extension.ExtensionNode;
import org.knime.python3.nodes.extension.ExtensionNodeSetFactory;
import org.knime.python3.nodes.extension.KnimeExtension;
import org.knime.python3.nodes.proxy.NodeProxyProvider;
import org.knime.python3.nodes.proxy.PythonNodeProxy;
import org.knime.python3.nodes.pycentric.PythonCentricExtensionParser;

/**
 * {@link ExtensionNodeSetFactory} for ALL nodes written purely in Python. On startup, it collects all registered Python
 * extensions and provides there nodes to the node repository.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class PurePythonNodeSetFactory extends ExtensionNodeSetFactory {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PurePythonNodeSetFactory.class);

    /**
     * Implementing classes allow to create a {@link PyNodeExtension} from a {@link Path} where the Python part of the
     * extension resides.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public interface PythonExtensionParser {

        /**
         * Parses the extension found at the provided path.
         *
         * @param path to the extension
         * @return the parsed extension
         * @throws IOException if parsing failed
         */
        PyNodeExtension parseExtension(final Path path) throws IOException;
    }

    private static final List<PyExtensionEntry> PYTHON_NODE_EXTENSION_PATHS = PythonExtensionRegistry.PY_EXTENSIONS;

    private static final PythonExtensionParser EXTENSION_PARSER = new PythonCentricExtensionParser();

    /**
     * Constructor.
     */
    public PurePythonNodeSetFactory() {
        super(PurePythonNodeSetFactory::parseExtensions);
    }

    private static Stream<KnimeExtension> parseExtensions() {
        return Stream.concat(getExtensionsFromPreferences(), getExtensionsFromExtensionPoint())//
            .filter(Objects::nonNull)//
            // if the same extension is defined by property and by extension point,
            // then we take the one from the property because the property is
            // intended for use during Python node development
            .distinct();
    }

    private static Stream<KnimeExtension> getExtensionsFromPreferences() {
        return PythonExtensionPreferences.getPathsToCustomExtensions()//
            .map(p -> parseExtension(p, "unknown"));
    }

    private static Stream<KnimeExtension> getExtensionsFromExtensionPoint() {
        return PYTHON_NODE_EXTENSION_PATHS.stream()//
            .map(e -> parseExtension(e.getPath(), e.getBundleName()));
    }

    private static final KnimeExtension parseExtension(final Path extensionPath, final String bundleName) {
        try {
            var extension = EXTENSION_PARSER.parseExtension(extensionPath);
            return new ResolvedPythonExtension(extension, bundleName);
        } catch (Exception ex) { //NOSONAR
            // any kind of exception must be prevented, otherwise a single corrupted extension would prevent the whole
            // class from loading
            LOGGER.error(String.format("Failed to parse Python node extension at path '%s'.", extensionPath), ex);
            return null;
        }
    }

    static final class ResolvedPythonExtension implements KnimeExtension {

        private final PyNodeExtension m_extension;

        private final String m_bundleName;

        ResolvedPythonExtension(final PyNodeExtension extension, final String bundleName) {
            m_extension = extension;
            m_bundleName = bundleName;
        }

        @Override
        public String getId() {
            return m_extension.getId();
        }

        @Override
        public String getVersion() {
            return m_extension.getVersion();
        }

        @Override
        public Optional<String> getBundleName() {
            return Optional.ofNullable(m_bundleName);
        }

        @Override
        public Stream<CategoryExtension> getCategories() {
            return m_extension.getCategories().map(b -> b.withPluginId(m_bundleName).build());
        }

        @Override
        public Stream<ExtensionNode> getNodes() {
            return m_extension.getNodeStream();
        }

        PythonGateway<KnimeNodeBackend> createGateway() throws IOException, InterruptedException {
            return m_extension.createGateway();
        }

        PythonNodeProxy createProxy(final KnimeNodeBackend backend, final String nodeId) {
            return m_extension.createNodeProxy(backend, nodeId);
        }

        @Override
        public NodeProxyProvider createProxyProvider(final String nodeId) throws InvalidSettingsException {
            if (PythonExtensionPreferences.debugMode(getId())) {
                return new PurePythonExtensionNodeProxyProvider(this, nodeId);
            } else {
                return new CachedNodeProxyProvider(this, nodeId);
            }
        }

        @Override
        public ExtensionNode getNode(final String nodeId) {
            return m_extension.getNode(nodeId);
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            } else if (obj instanceof ResolvedPythonExtension) {
                var other = (ResolvedPythonExtension)obj;
                return getId().equals(other.getId());
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return getId().hashCode();
        }
    }

}
