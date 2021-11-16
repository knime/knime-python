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
 *   Feb 26, 2019 (marcel): created
 */
package org.knime.python2.config;

import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.util.Version;
import org.knime.python2.PythonVersion;

/**
 * Initiates, observes, and {@link #cancelEnvironmentCreation(CondaEnvironmentCreationStatus) cancels} Conda environment
 * creation processes for a specific Conda installation and Python version. Allows clients to subscribe to changes in
 * the status of such creation processes.
 * <P>
 * Note: The current implementation only allows one active creation process at a time.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class CondaEnvironmentCreationObserver extends AbstractCondaEnvironmentCreationObserver {

    /**
     * The created instance is {@link #getIsEnvironmentCreationEnabled() disabled by default}.
     *
     * @param environmentPythonVersion The Python version of the Conda environments created by this instance.
     * @param condaDirectoryPath The Conda directory path. Changes in the model are reflected by this instance.
     */
    public CondaEnvironmentCreationObserver(final PythonVersion environmentPythonVersion,
        final SettingsModelString condaDirectoryPath) {
        super(environmentPythonVersion, condaDirectoryPath);
    }

    /**
     * @return The default environment name for the next environment created by this instance. Returns an empty string
     *         in case calling Conda failed.<br>
     *         Note that this method makes no guarantees about the uniqueness of the returned name if invoked in
     *         parallel to an ongoing environment creation process.
     */
    public String getDefaultEnvironmentName() {
        return getDefaultEnvironmentName("");
    }

    /**
     * Initiates a new Conda environment creation process using a predefined environment definition. Only allowed if
     * this instance is {@link #getIsEnvironmentCreationEnabled() enabled}.
     *
     * @param environmentName The name of the environment. Must not already exist in the local Conda installation. May
     *            be {@code null} or empty in which case a unique default name is used.
     * @param pythonVersion The Python version of the environment. Must match a version for which a predefined
     *            environment file is available.
     * @param status The status object that is will be notified about changes in the state of the initiated creation
     *            process. Can also be used to {@link #cancelEnvironmentCreation(CondaEnvironmentCreationStatus) cancel}
     *            the creation process. A new status object must be used for each new creation process.
     */
    public void startEnvironmentCreation(final String environmentName, final Version pythonVersion,
        final CondaEnvironmentCreationStatus status) {
        startEnvironmentCreation(environmentName, null, pythonVersion, status);
    }

    /**
     * Initiates a new Conda environment creation process using a given environment definition. Only allowed if this
     * instance is {@link #getIsEnvironmentCreationEnabled() enabled}.
     *
     * @param environmentName The name of the environment. Must not already exist in the local Conda installation. May
     *            be {@code null} or empty in which case a unique default name is used.
     * @param pathToEnvFile The path to the environment definition file.
     * @param status The status object that is will be notified about changes in the state of the initiated creation
     *            process. Can also be used to {@link #cancelEnvironmentCreation(CondaEnvironmentCreationStatus) cancel}
     *            the creation process. A new status object must be used for each new creation process.
     */
    public void startEnvironmentCreation(final String environmentName, final String pathToEnvFile,
        final CondaEnvironmentCreationStatus status) {
        startEnvironmentCreation(environmentName, pathToEnvFile, null, status);
    }
}
