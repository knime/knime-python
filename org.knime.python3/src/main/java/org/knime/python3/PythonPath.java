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
 *   May 6, 2021 (benjamin): created
 */
package org.knime.python3;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.knime.core.node.NodeLogger;

/**
 * A representation of a PYTHONPATH with Python modules on it. The {@link PythonPath} contains a list of strings which
 * point to folders containing modules.
 *
 * Use a {@link PythonPathBuilder} to create a {@link PythonPath}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class PythonPath {

    private final Set<String> m_paths = new LinkedHashSet<>();

    private PythonPath(final List<String> paths) {
        m_paths.addAll(paths);
    }

    /**
     * @return the list of absolute paths to the folders containing the Python modules
     */
    public List<String> getPaths() {
        return Collections.unmodifiableList(new ArrayList<>(m_paths));
    }

    /**
     * @return a string which can be used as the PYTHONPATH environment variable. The paths are separated by the
     *         platform specific separator.
     */
    public String getPythonPath() {
        return String.join(File.pathSeparator, m_paths);
    }

    @Override
    public int hashCode() {
        return Objects.hash(m_paths);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof PythonPath)) {
            return false;
        }
        return Objects.equals(((PythonPath)obj).m_paths, m_paths);
    }

    @Override
    public String toString() {
        return getPythonPath();
    }

    /**
     * Convenience method to create a builder for PythonPaths.
     *
     * @return a builder for PythonPath objects
     */
    public static PythonPathBuilder builder() {
        return new PythonPathBuilder();
    }

    /** A builder for {@link PythonPath}. */
    public static final class PythonPathBuilder {

        private final Set<String> m_paths;

        private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonPathBuilder.class);

        /** Create a new builder for {@link PythonPath}. */
        public PythonPathBuilder() {
            m_paths = new LinkedHashSet<>();
        }

        /**
         * Create a new builder already containing the paths from the given {@link PythonPath}.
         *
         * @param path the path to build upon
         */
        public PythonPathBuilder(final PythonPath path) {
            this();
            m_paths.addAll(path.m_paths);
        }

        /**
         * Add the given absolute path to the {@link PythonPath}.
         *
         * @param path absolute path to a folder containing Python modules
         * @return the builder
         */
        public PythonPathBuilder add(final Path path) {
            if (!m_paths.add(path.toAbsolutePath().toString())) {
                LOGGER.coding(String.format("The path '%s' is already present.",
                    path.toAbsolutePath()));
            }
            return this;
        }

        /**
         * Build the {@link PythonPath}.
         *
         * @return the {@link PythonPath}.
         */
        public PythonPath build() {
            return new PythonPath(new ArrayList<>(m_paths));
        }
    }
}
