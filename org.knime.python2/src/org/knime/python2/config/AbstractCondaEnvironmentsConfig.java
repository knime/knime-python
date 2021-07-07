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
 *   Jul 7, 2021 (marcel): created
 */
package org.knime.python2.config;

import java.nio.file.Paths;

import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.python2.conda.CondaEnvironmentIdentifier;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public abstract class AbstractCondaEnvironmentsConfig implements PythonConfig {

    /**
     * Configuration key for the path to the Conda installation directory.
     */
    public static final String CFG_KEY_CONDA_DIRECTORY_PATH = "condaDirectoryPath";

    public static final CondaEnvironmentIdentifier PLACEHOLDER_CONDA_ENV =
        new CondaEnvironmentIdentifier("no environment available", "no_conda_environment_selected");

    private final SettingsModelString m_condaDirectory;

    // Not meant for saving/loading. We just want observable values here to communicate with the view:

    private static final String DUMMY_CFG_KEY = "dummy";

    private final SettingsModelString m_condaInstallationInfo = new SettingsModelString(DUMMY_CFG_KEY, "");

    private final SettingsModelString m_condaInstallationError = new SettingsModelString(DUMMY_CFG_KEY, "");

    public AbstractCondaEnvironmentsConfig() {
        this(getDefaultCondaInstallationDirectory());
    }

    /**
     * @return The default value for the Conda installation directory path config entry.
     */
    private static String getDefaultCondaInstallationDirectory() {
        try {
            final String condaRoot = System.getenv("CONDA_ROOT");
            if (condaRoot != null) {
                return Paths.get(condaRoot).toString();
            }
        } catch (final Exception ex) { // NOSONAR Optional step
            // Ignore and continue with fallback.
        }
        try {
            // CONDA_EXE, if available, points to <CONDA_ROOT>/{bin|Scripts}/conda(.exe). We want <CONDA_ROOT>.
            final String condaExe = System.getenv("CONDA_EXE");
            if (condaExe != null) {
                return Paths.get(condaExe).getParent().getParent().toString();
            }
        } catch (final Exception ex) { // NOSONAR Optional step
            // Ignore and continue with fallback.
        }
        try {
            final String userHome = System.getProperty("user.home");
            if (userHome != null) {
                return Paths.get(userHome, "anaconda3").toString();
            }
        } catch (final Exception ex) { // NOSONAR Optional step
            // Ignore and continue with fallback.
        }
        return "";
    }

    public AbstractCondaEnvironmentsConfig(final String defaultCondaInstallationDirectory) {
        m_condaDirectory = new SettingsModelString(CFG_KEY_CONDA_DIRECTORY_PATH, defaultCondaInstallationDirectory);
    }

    /**
     * @return The path to the Conda installation directory.
     */
    public SettingsModelString getCondaDirectoryPath() {
        return m_condaDirectory;
    }

    /**
     * @return The installation status message of the local Conda installation. Not meant for saving/loading.
     */
    public SettingsModelString getCondaInstallationInfo() {
        return m_condaInstallationInfo;
    }

    /**
     * @return The installation error message of the local Conda installation. Not meant for saving/loading.
     */
    public SettingsModelString getCondaInstallationError() {
        return m_condaInstallationError;
    }

    @Override
    public void saveDefaultsTo(final PythonConfigStorage storage) {
        storage.saveStringModel(m_condaDirectory);
    }

    @Override
    public void saveConfigTo(final PythonConfigStorage storage) {
        storage.saveStringModel(m_condaDirectory);
    }

    @Override
    public void loadConfigFrom(final PythonConfigStorage storage) {
        storage.loadStringModel(m_condaDirectory);
    }
}
