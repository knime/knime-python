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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.knime.conda.Conda;
import org.knime.conda.CondaEnvironmentIdentifier;
import org.knime.conda.prefs.CondaPreferences;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.util.Version;
import org.knime.python3.scripting.nodes.prefs.PythonKernelTester.PythonKernelTestResult;

/**
 * Specialization of the PythonConfigsObserver for the org.knime.python3 scripting nodes. Those nodes do no longer offer
 * support for Python version 2 and do not allow to choose a serialization method, but add the option to use a bundled
 * conda environment compared to the PythonConfigsObserver from org.knime.python2.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
class Python3ScriptingConfigsObserver extends AbstractPythonConfigsObserver {

    private static final String CONDA_ENV_CREATION_INFO = """

            Note: You can create a new Python Conda environment that contains all
            packages required by the KNIME Python integration by running the command

                conda create --name <ENV_NAME> -c knime -c conda-forge knime-python-scripting python=3.9

            in a conda console.""";

    private final PythonEnvironmentTypeConfig m_environmentTypeConfig;

    private final CondaEnvironmentsConfig m_condaEnvironmentsConfig;

    private final ManualEnvironmentsConfig m_manualEnvironmentsConfig;

    private final BundledCondaEnvironmentConfig m_bundledCondaEnvironmentConfig;

    /**
     * @param environmentTypeConfig Environment type. Changes to the selected environment type are observed.
     * @param condaEnvironmentsConfig Conda environment configuration. Changes to the conda directory path as well as to
     *            the Python 3 environment are observed.
     * @param manualEnvironmentsConfig Manual environment configuration. Changes to the Python 3 paths are observed.
     * @param bundledCondaEnvironmentConfig Bundled conda environment configuration.
     */
    public Python3ScriptingConfigsObserver(final PythonEnvironmentTypeConfig environmentTypeConfig,
        final CondaEnvironmentsConfig condaEnvironmentsConfig, final ManualEnvironmentsConfig manualEnvironmentsConfig,
        final BundledCondaEnvironmentConfig bundledCondaEnvironmentConfig) {
        m_environmentTypeConfig = environmentTypeConfig;
        m_condaEnvironmentsConfig = condaEnvironmentsConfig;
        m_manualEnvironmentsConfig = manualEnvironmentsConfig;

        // Initialize view-model of default Python environment (since this was/is not persisted):

        updateDefaultPythonEnvironment();

        // Test all environments of the respective type on environment type change:
        environmentTypeConfig.getEnvironmentType().addChangeListener(e -> {
            updateDefaultPythonEnvironment();
            testCurrentPreferences();
        });

        // Test Conda environments on change:
        condaEnvironmentsConfig.getPython3Config().getEnvironmentDirectory()
            .addChangeListener(e -> testPythonEnvironment(true));

        // Test manual environments on change:
        manualEnvironmentsConfig.getPython3Config().getExecutablePath()
            .addChangeListener(e -> testPythonEnvironment(false));

        m_bundledCondaEnvironmentConfig = bundledCondaEnvironmentConfig;
    }

    private PythonEnvironmentsConfig getEnvironmentsOfCurrentType() {
        final PythonEnvironmentType environmentType = getEnvironmentType();
        if (PythonEnvironmentType.BUNDLED == environmentType) {
            return m_bundledCondaEnvironmentConfig;
        } else if (PythonEnvironmentType.CONDA.equals(environmentType)) {
            return m_condaEnvironmentsConfig;
        } else if (PythonEnvironmentType.MANUAL.equals(environmentType)) {
            return m_manualEnvironmentsConfig;
        } else {
            throw new IllegalStateException("Selected environment type '" + environmentType.getName() + "' is neither "
                + "conda nor manual. This is an implementation error.");
        }
    }

    /**
     * Initiates installation tests for all environments of the currently selected {@link PythonEnvironmentType} as well
     * as for the currently selected serializer. Depending on the selected environment type, the status of each of these
     * tests is published to all installation status models in either the observed CondaEnvironmentConfig or the
     * observed ManualEnvironmentConfig.
     */
    public void testCurrentPreferences() {
        final PythonEnvironmentType environmentType = getEnvironmentType();
        if (PythonEnvironmentType.BUNDLED == environmentType) {
            // TODO: what to test for a bundled conda environment? compare the env to its specs?
            if (!m_bundledCondaEnvironmentConfig.isAvailable()) {
                // This should never happen!
                m_bundledCondaEnvironmentConfig.getPythonInstallationError().setStringValue(
                    "Bundled conda environment is not available, please reinstall the 'KNIME Python Integration'.");
            }
        } else if (PythonEnvironmentType.CONDA.equals(environmentType)) {
            refreshAndTestCondaConfig();
        } else if (PythonEnvironmentType.MANUAL.equals(environmentType)) {
            testPythonEnvironment(false);
        } else {
            throw new IllegalStateException("Selected environment type '" + environmentType.getName() + "' is neither "
                + "conda nor manual. This is an implementation error.");
        }
    }

    private static Collection<PythonModuleSpec> getAdditionalRequiredModules() {
        return List.of(//
            new PythonModuleSpec("py4j"), //
            new PythonModuleSpec("pyarrow", new Version(5, 0, 0), true)//
        );
    }

    /**
     * @return The currently selected PythonEnvironmentType
     */
    protected PythonEnvironmentType getEnvironmentType() {
        return PythonEnvironmentType.fromId(m_environmentTypeConfig.getEnvironmentType().getStringValue());
    }

    private void updateDefaultPythonEnvironment() {
        if (PythonEnvironmentType.BUNDLED.equals(getEnvironmentType())) {
            // We do not configure a default environment if bundling is selected,
            // that will happen once the user selects "Conda" or "Manual" for the first time.
            return;
        }

        final List<PythonEnvironmentConfig> notDefaultEnvironments = new ArrayList<>(2);
        Collections.addAll(notDefaultEnvironments, m_condaEnvironmentsConfig.getPython3Config(),
            m_manualEnvironmentsConfig.getPython3Config());

        final PythonEnvironmentsConfig environmentsOfCurrentType = getEnvironmentsOfCurrentType();
        final PythonEnvironmentConfig defaultEnvironment = environmentsOfCurrentType.getPython3Config();
        notDefaultEnvironments.remove(defaultEnvironment);

        for (final PythonEnvironmentConfig notDefaultEnvironment : notDefaultEnvironments) {
            notDefaultEnvironment.getIsDefaultPythonEnvironment().setBooleanValue(false);
        }
        defaultEnvironment.getIsDefaultPythonEnvironment().setBooleanValue(true);
    }

    private void refreshAndTestCondaConfig() {
        new Thread(() -> {
            final Conda conda;
            try {
                conda = testCondaInstallation();
            } catch (final Exception ex) {
                return;
            }
            final List<CondaEnvironmentIdentifier> availableEnvironments;
            try {
                availableEnvironments = getAvailableCondaEnvironments(conda);
            } catch (final Exception ex) {
                return;
            }

            try {
                setAvailableCondaEnvironments(availableEnvironments);
                testPythonEnvironment(true);
            } catch (Exception ex) {
                // Ignore
            }
        }).start();
    }

    private Conda testCondaInstallation() throws Exception {
        final SettingsModelString condaInfoMessage = m_condaEnvironmentsConfig.getCondaInstallationInfo();
        final SettingsModelString condaErrorMessage = m_condaEnvironmentsConfig.getCondaInstallationError();
        try {
            condaInfoMessage.setStringValue("Testing Conda installation...");
            condaErrorMessage.setStringValue("");
            onCondaInstallationTestStarting();
            final String condaDir = CondaPreferences.getCondaInstallationDirectory();
            final Conda conda = new Conda(condaDir);
            conda.testInstallation();
            String condaVersionString = conda.getVersionString();
            try {
                condaVersionString =
                    "Conda version: " + Conda.condaVersionStringToVersion(condaVersionString).toString();
            } catch (final IllegalArgumentException ex) {
                // Ignore and use raw version string.
            }
            condaInfoMessage.setStringValue("Using Conda at '" + condaDir + "'. " + condaVersionString);
            condaErrorMessage.setStringValue("");
            onCondaInstallationTestFinished("");
            return conda;
        } catch (final Exception ex) {
            condaInfoMessage.setStringValue("");
            condaErrorMessage.setStringValue(ex.getMessage());
            clearAvailableCondaEnvironments();
            setCondaEnvironmentStatusMessages("", "");
            onCondaInstallationTestFinished(ex.getMessage());
            throw ex;
        }
    }

    private List<CondaEnvironmentIdentifier> getAvailableCondaEnvironments(final Conda conda) throws Exception {
        try {
            setCondaEnvironmentStatusMessages("Collecting available environments...", "");
            return conda.getEnvironments();
        } catch (final Exception ex) {
            m_condaEnvironmentsConfig.getCondaInstallationError().setStringValue(ex.getMessage());
            clearAvailableCondaEnvironments();
            setCondaEnvironmentStatusMessages("", "Available environments could not be detected.");
            throw ex;
        }
    }

    private void clearAvailableCondaEnvironments() {
        setAvailableCondaEnvironments(Collections.emptyList());
    }

    private void setCondaEnvironmentStatusMessages(final String infoMessage, final String errorMessage) {
        final CondaEnvironmentConfig condaEnvironmentConfig = m_condaEnvironmentsConfig.getPython3Config();
        condaEnvironmentConfig.getPythonInstallationInfo().setStringValue(infoMessage);
        condaEnvironmentConfig.getPythonInstallationError().setStringValue(errorMessage);
    }

    private void setAvailableCondaEnvironments(List<CondaEnvironmentIdentifier> availableEnvironments) {
        final CondaEnvironmentConfig condaConfig = m_condaEnvironmentsConfig.getPython3Config();
        if (availableEnvironments.isEmpty()) {
            availableEnvironments = Arrays.asList(CondaEnvironmentIdentifier.PLACEHOLDER_CONDA_ENV);
        }
        condaConfig.getAvailableEnvironments()
            .setValue(availableEnvironments.toArray(new CondaEnvironmentIdentifier[0]));
        final String currentlySelectedEnvironment = condaConfig.getEnvironmentDirectory().getStringValue();
        if (availableEnvironments.stream()
            .noneMatch(env -> Objects.equals(env.getDirectoryPath(), currentlySelectedEnvironment))) {
            condaConfig.getEnvironmentDirectory().setStringValue(availableEnvironments.get(0).getDirectoryPath());
        }
    }

    private void testPythonEnvironment(final boolean isConda) {
        final PythonEnvironmentsConfig environmentsConfig;
        final PythonEnvironmentType environmentType;
        final boolean isCondaPlaceholder;
        if (isConda) {
            isCondaPlaceholder = isPlaceholderEnvironmentSelected();
            environmentsConfig = m_condaEnvironmentsConfig;
            environmentType = PythonEnvironmentType.CONDA;
        } else {
            isCondaPlaceholder = false;
            environmentsConfig = m_manualEnvironmentsConfig;
            environmentType = PythonEnvironmentType.MANUAL;
        }
        final PythonEnvironmentConfig environmentConfig = environmentsConfig.getPython3Config();
        final SettingsModelString infoMessage = environmentConfig.getPythonInstallationInfo();
        final SettingsModelString errorMessage = environmentConfig.getPythonInstallationError();
        if (isCondaPlaceholder) {
            infoMessage.setStringValue("");
            errorMessage.setStringValue("No environment available. Please create a new one." + CONDA_ENV_CREATION_INFO);
            return;
        }
        final Collection<PythonModuleSpec> requiredSerializerModules = getAdditionalRequiredModules();
        infoMessage.setStringValue("Testing Python 3 environment...");
        errorMessage.setStringValue("");
        new Thread(() -> {
            onEnvironmentInstallationTestStarting(environmentType);
            final PythonKernelTestResult testResult = PythonKernelTester
                .testPython3Installation(environmentConfig.getPythonCommand(), requiredSerializerModules, true);
            infoMessage.setStringValue(testResult.getVersion());
            String errorLog = testResult.getErrorLog();
            if (errorLog != null && !errorLog.isEmpty()) {
                errorLog += CONDA_ENV_CREATION_INFO;
            }
            errorMessage.setStringValue(errorLog);
            onEnvironmentInstallationTestFinished(environmentType, testResult);
        }).start();
    }

    private boolean isPlaceholderEnvironmentSelected() {
        final SettingsModelString condaEnvironmentDirectory =
            m_condaEnvironmentsConfig.getPython3Config().getEnvironmentDirectory();
        return CondaEnvironmentIdentifier.PLACEHOLDER_CONDA_ENV.getDirectoryPath()
            .equals(condaEnvironmentDirectory.getStringValue());
    }
}
