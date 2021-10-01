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
 *   Oct 1, 2021 (marcel): created
 */
package org.knime.python3;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;

import org.eclipse.core.runtime.FileLocator;
import org.knime.core.util.FileUtil;
import org.osgi.framework.FrameworkUtil;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class PythonSourceDirectoryLocator {

    /**
     * The default relative path of the Python source code directory (to its bundle's base directory).
     */
    public static final String DEFAULT_SOURCE_DIRECTORY = "src/main/python";

    private PythonSourceDirectoryLocator() {}

    /**
     * Resolves the location of the Python source code directory of the given class's containing bundle to an absolute
     * path that is suitable to be added to Python's module path. The method assumes that the source code directory is
     * located at {@value #DEFAULT_SOURCE_DIRECTORY} relative to the bundle's base directory.
     *
     * @param clazz The class whose bundle's Python source code directory to resolve.
     * @return The absolute path to the Python source code directory.
     */
    public static Path getPathFor(final Class<?> clazz) {
        return getPathFor(clazz, DEFAULT_SOURCE_DIRECTORY);
    }

    /**
     * Resolves the location of the given relative Python source code directory to the base directory of the given
     * class's containing bundle. The returned absolute path is suitable to be added to Python's module path.
     *
     * @param clazz The class whose bundle's Python source code directory to resolve.
     * @param sourceDirectory The relative path of the source code directory.
     * @return The absolute path to the Python source code directory.
     */
    public static Path getPathFor(final Class<?> clazz, final String sourceDirectory) {
        try {
            final var bundle = FrameworkUtil.getBundle(clazz);
            final var url = FileLocator
                .toFileURL(FileLocator.find(bundle, new org.eclipse.core.runtime.Path(sourceDirectory), null));
            return FileUtil.resolveToPath(url);
        } catch (URISyntaxException | IOException ex) {
            throw new IllegalStateException("Failed to resolve Python source code directory '" + sourceDirectory +
                "' of the bundle of class '" + clazz + "'.", ex);
        }
    }
}
