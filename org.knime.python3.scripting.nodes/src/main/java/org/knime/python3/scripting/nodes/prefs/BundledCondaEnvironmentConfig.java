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
 *   2 Apr 2022 (Carsten Haubold): created
 */
package org.knime.python3.scripting.nodes.prefs;

import java.util.Objects;

import org.knime.conda.envbundling.environment.CondaEnvironmentRegistry;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.python2.PythonCommand;
import org.knime.python2.PythonVersion;
import org.knime.python3.BundledPythonCommand;

/**
 * The {@link BundledCondaEnvironmentConfig} is a {@link PythonEnvironmentConfig} that points to a bundled conda
 * environment which is identified by the string passed into the constructor.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class BundledCondaEnvironmentConfig extends AbstractPythonEnvironmentConfig
    implements PythonEnvironmentsConfig {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(BundledCondaEnvironmentConfig.class);

    /**
     * Configuration key for the path to the Bundled Python 3 executable ("environment").
     */
    public static final String CFG_KEY_PYTHON3_PATH = "bundledCondaEnvPath";

    private final SettingsModelString m_bundledCondaEnvironment;

    /**
     * @param bundledCondaEnvIdentifier
     */
    public BundledCondaEnvironmentConfig(final String bundledCondaEnvIdentifier) {
        m_bundledCondaEnvironment = new SettingsModelString(CFG_KEY_PYTHON3_PATH, bundledCondaEnvIdentifier);
    }

    @Override
    public void saveDefaultsTo(final PythonConfigStorage storage) {
        storage.saveStringModel(m_bundledCondaEnvironment);
    }

    @Override
    public void saveConfigTo(final PythonConfigStorage storage) {
        storage.saveStringModel(m_bundledCondaEnvironment);
    }

    @Override
    public void loadConfigFrom(final PythonConfigStorage storage) {
        storage.loadStringModel(m_bundledCondaEnvironment);
    }

    @Override
    public PythonEnvironmentConfig getPython3Config() {
        return this;
    }

    /**
     * @return true if the selected conda environment is available
     */
    public boolean isAvailable() {
        final var condaEnv = CondaEnvironmentRegistry.getEnvironment(m_bundledCondaEnvironment.getStringValue());
        return condaEnv != null;
    }

    @Override
    public PythonCommand getPythonCommand() {
        final var condaEnv = CondaEnvironmentRegistry.getEnvironment(m_bundledCondaEnvironment.getStringValue());
        if (condaEnv == null) {
            final var errorMsg = "You have selected the 'Bundled' option in KNIME Python preferences, "
                + "but there is no bundled Python environment available. "
                + "Please update your settings in the KNIME Python preference page.";
            LOGGER.error(errorMsg);
            throw new IllegalStateException(errorMsg);
        }

        return new BundledToLegacyCommandAdapter(new BundledPythonCommand(condaEnv.getPath().toString()));
    }

    private static final class BundledToLegacyCommandAdapter implements PythonCommand {

        private final BundledPythonCommand m_bundledCommand;

        public BundledToLegacyCommandAdapter(final BundledPythonCommand bundledCommand) {
            m_bundledCommand = bundledCommand;
        }

        @Override
        public PythonVersion getPythonVersion() {
            return PythonVersion.PYTHON3;
        }

        @Override
        public ProcessBuilder createProcessBuilder() {
            return m_bundledCommand.createProcessBuilder();
        }

        @Override
        public int hashCode() {
            return m_bundledCommand.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof BundledToLegacyCommandAdapter)) {
                return false;
            }
            return Objects.equals(((BundledToLegacyCommandAdapter)obj).m_bundledCommand, m_bundledCommand);
        }
    }
}
