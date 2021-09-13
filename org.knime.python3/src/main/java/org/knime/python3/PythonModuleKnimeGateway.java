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

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.knime.core.util.FileUtil;
import org.knime.python3.PythonPath.PythonPathBuilder;
import org.osgi.framework.FrameworkUtil;

/**
 * TODO(review) the Module handling could also be done differently. Having a PythonModule interface and singleton
 * implementations?
 *
 * Utilities for making the <code>knime_gateway</code> Python module available to a Python process.
 *
 * The function {@link #getPythonModuleFor(Class)} is a utility for accessing Python modules in the Java resource folder
 * "py_modules".
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class PythonModuleKnimeGateway {

    private PythonModuleKnimeGateway() {
        // Static utilities
    }

    /**
     * @return the absolute path to the Python module <code>knime_gateway</code>
     */
    public static String getPythonModule() {
        return getPythonModuleFor(PythonModuleKnimeGateway.class);
    }

    /**
     * Add the <code>knime_gateway</code> Python module to the builder.
     *
     * @param builder the builder for creating the path
     */
    public static void addToPythonPathBuilder(final PythonPathBuilder builder) {
        builder.add(getPythonModule());
    }

    /**
     * Get the absolute path to the resource named "py_modules" for the given class. This path can be added to the
     * {@link PythonPath} to make the modules at this location available to a Python script.
     *
     * @param clazz the class from which the resource should be loaded
     * @return the path to the "py_modules" resource
     */
    public static String getPythonModuleFor(final Class<?> clazz) {
        return getPythonModuleFor(clazz, "src/main/python");
    }

    /**
     * Get the absolute path to the resource named "py_modules" for the given class. This path can be added to the
     * {@link PythonPath} to make the modules at this location available to a Python script.
     *
     * @param clazz the class from which the resource should be loaded
     * @return the path to the "py_modules" resource
     */
    public static String getPythonModuleFor(final Class<?> clazz, final String pathToModuleFolder) {
        try {
            final URL resourceURL = getResourceURL(clazz, pathToModuleFolder);
            return FileUtil.resolveToPath(resourceURL).toString();
        } catch (URISyntaxException | IOException ex) {
            throw new IllegalStateException("Failed to find python module for class " + clazz, ex);
        }
    }

    private static URL getResourceURL(final Class<?> clazz, final String resourceName) throws IOException {
        final var bundle = FrameworkUtil.getBundle(clazz);
        if (bundle == null) {
            // not OSGI -> use class loader to get the resource
            // FIXME: the returned URL is probably not helpful if we're inside a JAR. We need to configure Maven to keep
            // the Python code outside the JAR.
            return clazz.getResource(Paths.get(resourceName).isAbsolute() ? resourceName : ("/" + resourceName));
        } else {
            // OSGI -> use the bundle to get the resource
            return FileLocator.toFileURL(FileLocator.find(bundle, new Path(resourceName), null));
        }
    }

}
