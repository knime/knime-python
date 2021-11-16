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
import org.knime.core.util.Version;
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

    private static final String YAML_FILE_EXTENSION = ".yml";

    private CondaEnvironments() {
        // Utility class.
    }

    /**
     * Returns the default environment definition file for the given Python version.
     *
     * @param pythonVersion The Python version of the environment. Must match a version for which a predefined
     *            environment file is available.
     * @return The path to the configuration file that can be used to create Conda environments of the given Python
     *         version that contain all packages required by the KNIME Python integration.<br>
     *         The returned path may differ between different operating systems.
     */
    public static String getPathToCondaConfigFile(final Version pythonVersion) {
        return getPathToCondaConfigFile(getFileStemForVersion(pythonVersion) + YAML_FILE_EXTENSION);
    }

    /**
     * Returns a specific environment definition file for the given Python version. Use
     * {@link #getPathToCondaConfigFile(Version)} for the default configuration.
     *
     * @param pythonVersion The Python version of the environment. Must match a version for which a predefined
     *            environment file is available.
     * @param tag The application specific "tag" of the configuration file.
     * @return The path to the configuration file that can be used to create Conda environments of the given Python
     *         version.<br>
     *         The returned path may differ between different operating systems.
     */
    public static String getPathToCondaConfigFile(final Version pythonVersion, final String tag) {
        return getPathToCondaConfigFile(getFileStemForVersion(pythonVersion) + "_" + tag + YAML_FILE_EXTENSION);
    }

    private static String getFileStemForVersion(final Version pythonVersion) {
        return "py" //
            + Integer.toString(pythonVersion.getMajor()) //
            + (pythonVersion.getMinor() != 0 ? Integer.toString(pythonVersion.getMinor()) : "") //
            + (pythonVersion.getRevision() != 0 ? Integer.toString(pythonVersion.getRevision()) : "") //
            + "_knime";
    }

    private static String getPathToCondaConfigFile(final String configFile) {
        final String osSubDirectory = getConfigSubDirectoryForOS();
        final String relativePathToDescriptionFile =
            Paths.get(CONDA_CONFIGS_DIRECTORY, osSubDirectory, configFile).toString();
        return getFile(relativePathToDescriptionFile).getAbsolutePath();
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
