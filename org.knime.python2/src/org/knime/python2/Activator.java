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
import java.util.Collection;

import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.core.runtime.Platform;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.FileUtil;
import org.knime.python2.extensions.serializationlibrary.SerializationLibraryExtensions;
import org.knime.python2.generic.templates.SourceCodeTemplatesExtensions;
import org.knime.python2.kernel.PythonKernelQueue;
import org.knime.python2.prefs.PythonPreferences;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

/**
 * Activator for the org.knime.python2 plugin.
 *
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class Activator implements BundleActivator {

    /**
     * The id of the org.knime.python2 plugin that is activated by this class.
     */
    public static final String PLUGIN_ID = "org.knime.python2";

    @Override
    public void start(final BundleContext bundleContext) throws Exception {
        // We need to collect all registered serializers before testing the installation since serializers may specify
        // additional required modules.
        SerializationLibraryExtensions.init();
        PythonPreferences.init();
        // Test Python installation.
        new Thread(() -> {
            Collection<PythonModuleSpec> requiredSerializerModules = null;
            try {
                requiredSerializerModules = PythonPreferences.getCurrentlyRequiredSerializerModules();
            } catch (final IllegalArgumentException ex) {
                // Preferences not yet properly configured by the user. Fall through.
            }
            if (requiredSerializerModules != null) {
                PythonKernelTester.testPython2Installation(PythonPreferences.getPython2CommandPreference(),
                    requiredSerializerModules, false);
                PythonKernelTester.testPython3Installation(PythonPreferences.getPython3CommandPreference(),
                    requiredSerializerModules, false);
            }
        }).start();
        SourceCodeTemplatesExtensions.init();
    }

    @Override
    public void stop(final BundleContext bundleContext) throws Exception {
        PythonKernelQueue.close();
    }

    /**
     * Returns the file contained in the plugin with the given ID.
     *
     * @param symbolicName The ID of the plugin containing the file.
     * @param relativePath The file path inside the plugin.
     * @return The file.
     */
    public static File getFile(final String symbolicName, final String relativePath) {
        try {
            final Bundle bundle = Platform.getBundle(symbolicName);
            final URL url = FileLocator.find(bundle, new Path(relativePath), null);
            return url != null ? FileUtil.getFileFromURL(FileLocator.toFileURL(url)) : null;
        } catch (final Exception e) {
            NodeLogger.getLogger(Activator.class).debug(e.getMessage(), e);
            return null;
        }
    }
}
