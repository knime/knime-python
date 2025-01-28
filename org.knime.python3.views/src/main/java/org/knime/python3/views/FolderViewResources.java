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
 *   Nov 22, 2022 (benjamin): created
 */
package org.knime.python3.views;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.knime.core.node.NodeLogger;
import org.knime.core.webui.page.PageBuilder;

/**
 * A folder of resources for a view.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class FolderViewResources implements ViewResources {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(FolderViewResources.class);

    private final Path m_path;

    private final String m_relativePathPrefix;

    private boolean m_areStatic;

    /**
     * @param path the absolute path to the resources
     * @param relativePathPrefix the relative path under which the resources will be accessable
     * @param areStatic if the resources are considered as static and won't change
     */
    public FolderViewResources(final Path path, final String relativePathPrefix, final boolean areStatic) {
        m_path = path;
        m_relativePathPrefix = relativePathPrefix;
        m_areStatic = areStatic;
    }

    @Override
    public void addToPageBuilder(final PageBuilder pageBuilder) {
        pageBuilder.addResources(this::openResource, m_relativePathPrefix, m_areStatic, StandardCharsets.UTF_8);
    }

    private InputStream openResource(final String name) {
        final var path = m_path.resolve(name);
        try {
            return Files.newInputStream(path);
        } catch (final IOException ex) {
            LOGGER.warn(
                String.format("Could not load resource with name '%s' from path '%s': %s", name, path, ex.getMessage()),
                ex);
            return null;
        }
    }
}
