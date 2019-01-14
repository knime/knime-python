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
package org.knime.python2;

import java.io.File;
import java.net.URL;

import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.core.runtime.Platform;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.FileUtil;
import org.knime.python2.PythonKernelTester.PythonKernelTestResult;
import org.knime.python2.extensions.serializationlibrary.SerializationLibraryExtensions;
import org.knime.python2.generic.templates.SourceCodeTemplatesExtensions;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

/**
 * Activator for this plugin.
 *
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 */
public class Activator implements BundleActivator {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(Activator.class);

    /**
     * The id of the plugin activated by this class.
     */
    public static final String PLUGIN_ID = "org.knime.python2";

    @Override
    public void start(final BundleContext bundleContext) throws Exception {
        // When this plugin is loaded test the python installation
        new Thread(() -> {
            PythonKernelTestResult python2Res = PythonKernelTester.testPython2Installation(getPython2Command(),
                PythonPreferencePage.getRequiredSerializerModules(), false);
            if (python2Res.hasError()) {
                LOGGER.debug("Your configured python installation has issues preventing the KNIME Python integration"
                    + "\nIssue python version 2: " + python2Res.getErrorLog());
            }

            PythonKernelTestResult python3Res = PythonKernelTester.testPython3Installation(getPython3Command(),
                PythonPreferencePage.getRequiredSerializerModules(), false);
            if (python3Res.hasError()) {
                LOGGER.debug("Your configured python installation has issues preventing the KNIME Python integration"
                    + "\nIssue python version 3: " + python3Res.getErrorLog());
            }
        }).start();
        SerializationLibraryExtensions.init();
        SourceCodeTemplatesExtensions.init();
    }

    @Override
    public void stop(final BundleContext bundleContext) throws Exception {
        // no op
    }

    /**
     * Return the command to start python 2.
     *
     * @return The command to start python 2
     */
    public static String getPython2Command() {
        return PythonPreferencePage.getPython2Path();
    }

    /**
     * Return the command to start python 3.
     *
     * @return The command to start python 3
     */
    public static String getPython3Command() {
        return PythonPreferencePage.getPython3Path();
    }

    /**
     * Returns the file contained in the plugin with the given ID.
     *
     * @param symbolicName ID of the plugin containing the file
     * @param relativePath File path inside the plugin
     * @return The file
     */
    public static File getFile(final String symbolicName, final String relativePath) {
        try {
            final Bundle bundle = Platform.getBundle(symbolicName);
            final URL url = FileLocator.find(bundle, new Path(relativePath), null);
            return url != null ? FileUtil.getFileFromURL(FileLocator.toFileURL(url)) : null;
        } catch (final Exception e) {
            LOGGER.debug(e.getMessage(), e);
            return null;
        }
    }
}
