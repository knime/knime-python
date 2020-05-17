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
 *   May 6, 2020 (marcel): created
 */
package org.knime.python2;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.SystemUtils;

/**
 * Conda-specific implementation of {@link PythonCommand}. Allows to build Python processes for a given Conda
 * installation and environment name. Takes care of resolving PATH-related issues on Windows.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class CondaPythonCommand extends AbstractPythonCommand {

    private static final String CONDA_ENVIRONMENTS_DIRECTORY_NAME = "envs";

    private static final String PATH_ENVIRONMENT_VARIABLE_NAME = "PATH";

    /**
     * Note: path as in the PATH environment variable, not the path to the Python executable.
     */
    private final String m_pathPrefix;

    /**
     * Constructs a {@link PythonCommand} that describes a Python process of the given Python version that is run in the
     * Conda environment identified by the given Conda installation directory and the given Conda environment name.<br>
     * The validity of the given arguments is not tested.
     *
     * @param pythonVersion The version of Python environments launched by this command.
     * @param condaInstallationDirectoryPath The path to the directory of the Conda installation.
     * @param environmentName The name of the Conda environment.
     */
    public CondaPythonCommand(final PythonVersion pythonVersion, final String condaInstallationDirectoryPath,
        final String environmentName) {
        super(pythonVersion, Arrays.asList(createExecutableString(condaInstallationDirectoryPath, environmentName)));
        // On Windows, we need to prepend a number of library paths to the PATH environment variable to resolve issues
        // that may occur when Python modules are searching for DLLs.
        if (SystemUtils.IS_OS_WINDOWS) {
            final List<String> pathPrefixes = new ArrayList<>();
            final String environmentDirectory =
                createEnvironmentDirectoryString(condaInstallationDirectoryPath, environmentName);
            addToPrefixes(pathPrefixes, environmentDirectory);
            addToPrefixes(pathPrefixes, environmentDirectory, "Library", "mingw-w64", "bin");
            addToPrefixes(pathPrefixes, environmentDirectory, "Library", "usr", "bin");
            addToPrefixes(pathPrefixes, environmentDirectory, "Library", "bin");
            addToPrefixes(pathPrefixes, environmentDirectory, "Scripts");
            addToPrefixes(pathPrefixes, environmentDirectory, "bin");
            addToPrefixes(pathPrefixes, condaInstallationDirectoryPath, "condabin");
            m_pathPrefix = String.join(File.pathSeparator, pathPrefixes);
        } else {
            m_pathPrefix = null;
        }
    }

    /**
     * Paths are determined as per https://docs.anaconda.com/anaconda/user-guide/tasks/integration/python-path/
     */
    private static String createExecutableString(final String condaInstallationDirectoryPath,
        final String environmentName) {
        final String environmentDirectory =
            createEnvironmentDirectoryString(condaInstallationDirectoryPath, environmentName);
        final Path executablePath;
        if (SystemUtils.IS_OS_LINUX || SystemUtils.IS_OS_MAC) {
            executablePath = Paths.get(environmentDirectory, "bin", "python");
        } else if (SystemUtils.IS_OS_WINDOWS) {
            executablePath = Paths.get(environmentDirectory, "python.exe");
        } else {
            throw Conda.createUnknownOSException();
        }
        return executablePath.toString();
    }

    /**
     * Paths are determined as per https://docs.anaconda.com/anaconda/user-guide/tasks/integration/python-path/
     */
    private static String createEnvironmentDirectoryString(final String condaInstallationDirectoryPath,
        final String environmentName) {
        if (environmentName.equals(Conda.ROOT_ENVIRONMENT_NAME)) {
            return condaInstallationDirectoryPath;
        } else {
            return Paths.get(condaInstallationDirectoryPath, CONDA_ENVIRONMENTS_DIRECTORY_NAME, environmentName)
                .toString();
        }
    }

    private static void addToPrefixes(final List<String> prefixes, final String first, final String... more) {
        prefixes.add(Paths.get(first, more).toString());
    }

    @Override
    public ProcessBuilder createProcessBuilder() {
        final ProcessBuilder pb = super.createProcessBuilder();
        if (m_pathPrefix != null) {
            pb.environment().merge(PATH_ENVIRONMENT_VARIABLE_NAME, m_pathPrefix,
                (oldPath, pathPrefix) -> pathPrefix + File.pathSeparatorChar + oldPath);
        }
        return pb;
    }
}
