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
 */
package org.knime.python2.envconfigs;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Paths;

import org.apache.commons.lang3.SystemUtils;
import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.core.runtime.Platform;
import org.knime.core.util.FileUtil;
import org.osgi.framework.Bundle;

/**
 * Gives programmatic access to the Conda environment configuration files contained in this plugin. The files can be
 * used to create Conda environments that contain all packages required by the KNIME Python integration.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class CondaEnvironments {

    private static final String PLUGIN_ID = "org.knime.python2.envconfigs";

    private static final String CONDA_CONFIGS_DIRECTORY = "envconfigs";

    private static final String PYTHON2_CONFIG_FILE = "py2_knime.yml";

    private static final String PYTHON3_CONFIG_FILE = "py3_knime.yml";

    private static final String START_SCRIPT_PREFIX = "start_py";

    private CondaEnvironments() {
        // Utility class.
    }

    /**
     * @return The path to the configuration file that can be used to create Python 2 Conda environments that contain
     *         all packages required by the KNIME Python integration.<br>
     *         The returned path may differ between different operating systems.
     */
    public static String getPathToPython2CondaConfigFile() {
        return getPathToCondaConfigFile(PYTHON2_CONFIG_FILE);
    }

    /**
     * @return The path to the configuration file that can be used to create Python 3 Conda environments that contain
     *         all packages required by the KNIME Python integration.<br>
     *         The returned path may differ between different operating systems.
     */
    public static String getPathToPython3CondaConfigFile() {
        return getPathToCondaConfigFile(PYTHON3_CONFIG_FILE);
    }

    /**
     * Returns a specific Python 2 Conda configuration file. Use {@link #getPathToPython2CondaConfigFile()} for the
     * default configuration.
     *
     * @param subDirectory the sub directory of this configuration.
     * @return The path to the configuration file that can be used to create Python 2 Conda environments
     */
    public static String getPathToPython2CondaConfigFile(final String subDirectory) {
        return getPathToCondaConfigFile(PYTHON2_CONFIG_FILE, subDirectory);
    }

    /**
     * Returns a specific Python 3 Conda configuration file. Use {@link #getPathToPython3CondaConfigFile()} for the
     * default configuration.
     *
     * @param subDirectory the sub directory of this configuration.
     * @return The path to the configuration file that can be used to create Python 3 Conda environments
     */
    public static String getPathToPython3CondaConfigFile(final String subDirectory) {
        return getPathToCondaConfigFile(PYTHON3_CONFIG_FILE, subDirectory);
    }

    private static String getPathToCondaConfigFile(final String configFile) {
        final String osSubDirectory = getConfigSubDirectoryForOS();
        final String relativePathToDescriptionFile =
            Paths.get(CONDA_CONFIGS_DIRECTORY, osSubDirectory, configFile).toString();
        return getFile(relativePathToDescriptionFile).getAbsolutePath();
    }

    private static String getPathToCondaConfigFile(final String configFile, final String subDirectory) {
        final String osSubDirectory = getConfigSubDirectoryForOS();
        final String relativePathToDescriptionFile =
            Paths.get(CONDA_CONFIGS_DIRECTORY, osSubDirectory, subDirectory, configFile).toString();
        return getFile(relativePathToDescriptionFile).getAbsolutePath();
    }

    /**
     * @return The path to the Conda start script. The script can be used to launch an arbitrary Conda environment. It
     *         takes two mandatory arguments: the path to the Conda executable and the name of the environment to start.
     *         The script can be executed with the arguments provided by using {@link ProcessBuilder#command(String...)}
     *         and {@link ProcessBuilder#start()}.<br>
     *         The returned path may differ between different operating systems.
     */
    public static String getPathToCondaStartScript() {
        final String osSubDirectory = getConfigSubDirectoryForOS();
        final String osStartScriptFilExtension = getStartScriptFileExtensionForOS();
        final String relativePathToStartScript =
            Paths.get(CONDA_CONFIGS_DIRECTORY, osSubDirectory, START_SCRIPT_PREFIX + "." + osStartScriptFilExtension)
                .toString();
        return getFile(relativePathToStartScript).getAbsolutePath();
    }

    private static String getConfigSubDirectoryForOS() {
        final String osSubDirectory;
        if (SystemUtils.IS_OS_LINUX) {
            osSubDirectory = "linux";
        } else if (SystemUtils.IS_OS_MAC) {
            osSubDirectory = "macos";
        } else if (SystemUtils.IS_OS_WINDOWS) {
            osSubDirectory = "windows";
        } else {
            throw createUnknownOSException();
        }
        return osSubDirectory;
    }

    private static String getStartScriptFileExtensionForOS() {
        final String osStartScriptFileExtension;
        if (SystemUtils.IS_OS_LINUX || SystemUtils.IS_OS_MAC) {
            osStartScriptFileExtension = "sh";
        } else if (SystemUtils.IS_OS_WINDOWS) {
            osStartScriptFileExtension = "bat";
        } else {
            throw createUnknownOSException();
        }
        return osStartScriptFileExtension;
    }

    private static UnsupportedOperationException createUnknownOSException() {
        final String osName = SystemUtils.OS_NAME;
        if (osName == null) {
            throw new UnsupportedOperationException(
                "Could not detect your operating system. This is necessary for Conda environment configuration. "
                    + "Please make sure KNIME has the proper access rights to your system.");
        } else {
            throw new UnsupportedOperationException(
                "Conda environment configuration is only supported on Windows, Mac, and Linux. Your operating "
                    + "system is: " + SystemUtils.OS_NAME);
        }
    }

    private static File getFile(final String relativePath) {
        File file = null;
        final Bundle bundle = Platform.getBundle(PLUGIN_ID);
        final URL url = FileLocator.find(bundle, new Path(relativePath), null);
        if (url != null) {
            try {
                file = FileUtil.getFileFromURL(FileLocator.toFileURL(url));
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }
        if (file == null) {
            throw new IllegalStateException(
                "File at location '" + relativePath + "' of bundle '" + PLUGIN_ID + "' cannot be resolved.");
        }
        return file;
    }
}
