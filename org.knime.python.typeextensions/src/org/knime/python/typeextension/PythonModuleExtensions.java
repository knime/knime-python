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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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
package org.knime.python.typeextension;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.Platform;
import org.knime.core.node.NodeLogger;
import org.knime.python.typeextensions.Activator;

/**
 * Class for administrating all modules that are added to the PYTHONPATH via the org.knime.python.modules extension
 * point.
 *
 * @author Clemens von Schwerin, KNIME.com, Konstanz, Germany
 *
 */
public class PythonModuleExtensions {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonModuleExtensions.class);

    private static Set<String> pythonModulePaths;

    /**
     * Add all additional python modules specified via the modules extension point to the PYTHONPATH
     */
    public static void init() {
        pythonModulePaths = new HashSet<String>();
        final IConfigurationElement[] configs =
            Platform.getExtensionRegistry().getConfigurationElementsFor("org.knime.python.modules");
        for (final IConfigurationElement config : configs) {
            final String pluginId = config.getContributor().getName();
            final String path = config.getAttribute("path");
            final File file = Activator.getFile(pluginId, path);
            if ((file == null) || !file.exists() || !file.isDirectory()) {
                LOGGER.warn("Could not find the directory " + path + " in plugin " + pluginId);
            } else {
                pythonModulePaths.add(file.getAbsolutePath());
            }
        }
    }

    /**
     * Gets the PYTHONPATH.
     *
     * @return the PYTHONPATH
     */
    public static String getPythonPath() {
        return StringUtils.join(pythonModulePaths, File.pathSeparator);
    }

}
