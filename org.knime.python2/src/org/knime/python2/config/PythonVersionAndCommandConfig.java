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
 *   Dec 1, 2020 (marcel): created
 */
package org.knime.python2.config;

import java.util.function.Supplier;

import org.knime.python2.CondaPythonCommand;
import org.knime.python2.PythonCommand;
import org.knime.python2.PythonVersion;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class PythonVersionAndCommandConfig {

    private static final String CFG_PYTHON_VERSION = "pythonVersionOption";

    private static final String CFG_PYTHON2_COMMAND = "python2Command";

    private static final String CFG_PYTHON3_COMMAND = "python3Command";

    private final PythonVersionNodeConfig m_pythonVersionConfig;

    private final PythonCommandConfig m_python2SelectionConfig;

    private final PythonCommandConfig m_python3SelectionConfig;

    /**
     * @param pythonVersion The initial Python version.
     * @param condaInstallationPathPreference A supplier that returns the centrally configured directory of the Conda
     *            installation. Needed to create {@link CondaPythonCommand Conda-based commands}.
     * @param python2CommandPreference A supplier that returns the Python 2 command to use if no node-specific command
     *            is configured.
     * @param python3CommandPreference A supplier that returns the Python 3 command to use if no node-specific command
     *            is configured.
     */
    public PythonVersionAndCommandConfig(final PythonVersion pythonVersion,
        final Supplier<String> condaInstallationPathPreference, final Supplier<PythonCommand> python2CommandPreference,
        final Supplier<PythonCommand> python3CommandPreference) {
        m_pythonVersionConfig = new PythonVersionNodeConfig(CFG_PYTHON_VERSION, pythonVersion);
        m_python2SelectionConfig = new PythonCommandConfig(CFG_PYTHON2_COMMAND, PythonVersion.PYTHON2,
            condaInstallationPathPreference, python2CommandPreference);
        m_python3SelectionConfig = new PythonCommandConfig(CFG_PYTHON3_COMMAND, PythonVersion.PYTHON3,
            condaInstallationPathPreference, python3CommandPreference);
    }

    /**
     * @return The config of the Python version.
     */
    public PythonVersionNodeConfig getPythonVersionConfig() {
        return m_pythonVersionConfig;
    }

    /**
     * @return The config of the Python 2 command.
     */
    public PythonCommandConfig getPython2CommandConfig() {
        return m_python2SelectionConfig;
    }

    /**
     * @return The config of the Python 3 command.
     */
    public PythonCommandConfig getPython3CommandConfig() {
        return m_python3SelectionConfig;
    }
}
