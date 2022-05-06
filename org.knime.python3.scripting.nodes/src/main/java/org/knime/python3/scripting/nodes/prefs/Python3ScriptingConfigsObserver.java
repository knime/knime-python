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
 *   Jan 25, 2019 (marcel): created
 */
package org.knime.python3.scripting.nodes.prefs;

import java.util.Collection;
import java.util.List;

import org.knime.core.util.Version;
import org.knime.python2.PythonModuleSpec;
import org.knime.python2.PythonVersion;
import org.knime.python2.config.CondaEnvironmentCreationObserver;
import org.knime.python2.config.CondaEnvironmentsConfig;
import org.knime.python2.config.ManualEnvironmentsConfig;
import org.knime.python2.config.PythonConfigsObserver;
import org.knime.python2.config.PythonEnvironmentType;
import org.knime.python2.config.PythonEnvironmentTypeConfig;
import org.knime.python2.config.PythonEnvironmentsConfig;
import org.knime.python2.config.PythonVersionConfig;
import org.knime.python2.config.SerializerConfig;

/**
 * Specialization of the {@link PythonConfigsObserver} for the org.knime.python3 scripting nodes. Those nodes do no
 * longer offer support for Python version 2 and do not allow to choose a serialization method, but add the option to
 * use a bundled conda environment compared to the {@link PythonConfigsObserver} from org.knime.python2.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public class Python3ScriptingConfigsObserver extends PythonConfigsObserver {

    private final BundledCondaEnvironmentConfig m_bundledCondaEnvironmentConfig;

    /**
     * @param environmentTypeConfig Environment type. Changes to the selected environment type are observed.
     * @param condaEnvironmentsConfig Conda environment configuration. Changes to the conda directory path as well as to
     *            the Python 3 environment are observed.
     * @param python3EnvironmentCreator Manages the creation of new Python 3 Conda environments. Enabled/disabled by
     *            this instance based on the validity of the local Conda installation. Finished creation processes are
     *            observed.
     * @param manualEnvironmentsConfig Manual environment configuration. Changes to the Python 3 paths are observed.
     * @param bundledCondaEnvironmentConfig Bundled conda environment configuration.
     */
    public Python3ScriptingConfigsObserver(final PythonEnvironmentTypeConfig environmentTypeConfig,
        final CondaEnvironmentsConfig condaEnvironmentsConfig,
        final CondaEnvironmentCreationObserver python3EnvironmentCreator,
        final ManualEnvironmentsConfig manualEnvironmentsConfig,
        final BundledCondaEnvironmentConfig bundledCondaEnvironmentConfig) {

        super(new PythonVersionConfig(), environmentTypeConfig, condaEnvironmentsConfig,
            new CondaEnvironmentCreationObserver(PythonVersion.PYTHON2), python3EnvironmentCreator,
            manualEnvironmentsConfig, new SerializerConfig());
        m_bundledCondaEnvironmentConfig = bundledCondaEnvironmentConfig;
    }

    @Override
    protected PythonEnvironmentsConfig getEnvironmentsOfCurrentType() {
        final PythonEnvironmentType environmentType = getEnvironmentType();
        if (PythonEnvironmentType.BUNDLED == environmentType) {
            return m_bundledCondaEnvironmentConfig;
        }

        return super.getEnvironmentsOfCurrentType();

    }

    @Override
    public void testCurrentPreferences() {
        final PythonEnvironmentType environmentType = getEnvironmentType();
        if (PythonEnvironmentType.BUNDLED == environmentType) {
            // TODO: what to test for a bundled conda environment? compare the env to its specs?
            if (!m_bundledCondaEnvironmentConfig.isAvailable()) {
                // This should never happen!
                // TODO: adjust the feature name if we decide to change that in AP-18855
                m_bundledCondaEnvironmentConfig.getPythonInstallationError().setStringValue(
                    "Bundled conda environment is not available, please reinstall the 'KNIME Conda channel pythonscripting' feature.");
            }
        } else {
            super.testCurrentPreferences();
        }
    }

    @Override
    protected Collection<PythonModuleSpec> getAdditionalRequiredModules() {
        return List.of(//
            new PythonModuleSpec("py4j"), //
            new PythonModuleSpec("pyarrow", new Version(6, 0, 0), true, new Version(7, 0, 0), false)//
        );
    }
}
