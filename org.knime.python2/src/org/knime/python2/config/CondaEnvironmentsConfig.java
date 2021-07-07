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
 *   Feb 21, 2019 (marcel): created
 */
package org.knime.python2.config;

import org.knime.python2.PythonVersion;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class CondaEnvironmentsConfig extends AbstractCondaEnvironmentsConfig implements PythonEnvironmentsConfig {

    private static final String CFG_KEY_PYTHON2_CONDA_ENV_DIR = "python2CondaEnvironmentDirectoryPath";

    /** Only used for legacy support. See {@link #loadConfigFrom(PythonConfigStorage)} below. */
    private static final String LEGACY_CFG_KEY_PYTHON2_CONDA_ENV_NAME = "python2CondaEnvironmentName";

    private static final String CFG_KEY_PYTHON3_CONDA_ENV_DIR = "python3CondaEnvironmentDirectoryPath";

    /** Only used for legacy support. See {@link #loadConfigFrom(PythonConfigStorage)} below. */
    private static final String LEGACY_CFG_KEY_PYTHON3_CONDA_ENV_NAME = "python3CondaEnvironmentName";

    private final CondaEnvironmentConfig m_python2EnvironmentConfig = new CondaEnvironmentConfig( //
        PythonVersion.PYTHON2, //
        CFG_KEY_PYTHON2_CONDA_ENV_DIR, //
        LEGACY_CFG_KEY_PYTHON2_CONDA_ENV_NAME, //
        PLACEHOLDER_CONDA_ENV, //
        getCondaDirectoryPath() //
    );

    private final CondaEnvironmentConfig m_python3EnvironmentConfig = new CondaEnvironmentConfig( //
        PythonVersion.PYTHON3, //
        CFG_KEY_PYTHON3_CONDA_ENV_DIR, //
        LEGACY_CFG_KEY_PYTHON3_CONDA_ENV_NAME, //
        PLACEHOLDER_CONDA_ENV, //
        getCondaDirectoryPath() //
    );

    @Override
    public CondaEnvironmentConfig getPython2Config() {
        return m_python2EnvironmentConfig;
    }

    @Override
    public CondaEnvironmentConfig getPython3Config() {
        return m_python3EnvironmentConfig;
    }

    @Override
    public void saveDefaultsTo(final PythonConfigStorage storage) {
        super.saveDefaultsTo(storage);
        m_python2EnvironmentConfig.saveDefaultsTo(storage);
        m_python3EnvironmentConfig.saveDefaultsTo(storage);
    }

    @Override
    public void saveConfigTo(final PythonConfigStorage storage) {
        super.saveConfigTo(storage);
        m_python2EnvironmentConfig.saveConfigTo(storage);
        m_python3EnvironmentConfig.saveConfigTo(storage);
    }

    @Override
    public void loadConfigFrom(final PythonConfigStorage storage) {
        super.loadConfigFrom(storage);
        m_python2EnvironmentConfig.loadConfigFrom(storage);
        m_python3EnvironmentConfig.loadConfigFrom(storage);
    }
}
