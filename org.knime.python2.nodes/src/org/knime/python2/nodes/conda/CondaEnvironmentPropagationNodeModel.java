/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 *
 * History
 *   Sep 25, 2014 (Patrick Winter): created
 */
package org.knime.python2.nodes.conda;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.commons.lang3.SystemUtils;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeProgressMonitor;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.flowvariable.FlowVariablePortObject;
import org.knime.core.node.port.flowvariable.FlowVariablePortObjectSpec;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.util.Pair;
import org.knime.python2.CondaEnvironmentPropagation;
import org.knime.python2.CondaEnvironmentPropagation.CondaEnvironmentSpec;
import org.knime.python2.CondaEnvironmentPropagation.CondaEnvironmentType;
import org.knime.python2.conda.Conda;
import org.knime.python2.conda.CondaEnvironmentCreationMonitor;
import org.knime.python2.conda.CondaEnvironmentIdentifier;
import org.knime.python2.conda.CondaPackageSpec;
import org.knime.python2.PythonVersion;
import org.knime.python2.config.CondaEnvironmentsConfig;
import org.knime.python2.kernel.PythonKernelQueue;
import org.knime.python2.prefs.PythonPreferences;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
final class CondaEnvironmentPropagationNodeModel extends NodeModel {

    private static final String CFG_KEY_CONDA_ENV = "conda_environment";

    private static final String CFG_KEY_ENV_VALIDATION_METHOD = "environment_validation";

    private static final String VALIDATION_METHOD_NAME = "name";

    private static final String VALIDATION_METHOD_NAME_PACKAGES = "name_packages";

    private static final String VALIDATION_METHOD_OVERWRITE = "overwrite";

    private static final String CFG_KEY_SOURCE_OS_NAME = "source_operating_system";

    private static final String SOURCE_OS_LINUX = "linux";

    private static final String SOURCE_OS_MAC = "mac";

    private static final String SOURCE_OS_WINDOWS = "windows";

    private static final Set<String> CROSS_PLATFORM_EXCLUDED_PACKAGES =
        Collections.unmodifiableSet(new HashSet<>(Arrays.asList( //
            "appnope", // MacOS-only
            "libcxx", // no Linux
            "libcxxabi", // no Linux
            "libgfortran", // no Linux
            "mkl_fft", // conflicts on Linux coming from Windows
            "mkl_random", // conflicts on Linux coming from Windows
            "vc", //
            "m2w64-libwinpthread-git", //
            "icc_rt", //
            "m2w64-gmp", //
            "pywinpty", //
            "wincertstore", //
            "msys2-conda-epoch", //
            "winpty", //
            "m2w64-gcc-libs", //
            "m2w64-gcc-libgfortran", //
            "vs2015_runtime", //
            "win_inet_pton", //
            "m2w64-gcc-libs-core")));

    static SettingsModelString createCondaEnvironmentNameModel() {
        return new SettingsModelString(CFG_KEY_CONDA_ENV, CondaEnvironmentsConfig.PLACEHOLDER_CONDA_ENV_NAME);
    }

    static CondaPackagesConfig createPackagesConfig() {
        return new CondaPackagesConfig();
    }

    static String[] createEnvironmentValidationMethodKeys() {
        return new String[]{VALIDATION_METHOD_NAME, VALIDATION_METHOD_NAME_PACKAGES, VALIDATION_METHOD_OVERWRITE};
    }

    static SettingsModelString createEnvironmentValidationMethodModel() {
        return new SettingsModelString(CFG_KEY_ENV_VALIDATION_METHOD, createEnvironmentValidationMethodKeys()[0]);
    }

    static SettingsModelString createSourceOsModel() {
        return new SettingsModelString(CFG_KEY_SOURCE_OS_NAME, getCurrentOsType());
    }

    private final SettingsModelString m_environmentNameModel = createCondaEnvironmentNameModel();

    private final CondaPackagesConfig m_packagesConfig = createPackagesConfig();

    private final SettingsModelString m_validationMethodModel = createEnvironmentValidationMethodModel();

    private final SettingsModelString m_sourceOsModel = createSourceOsModel();

    public CondaEnvironmentPropagationNodeModel() {
        super(new PortType[0], new PortType[]{FlowVariablePortObject.TYPE});
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_environmentNameModel.saveSettingsTo(settings);
        m_packagesConfig.saveSettingsTo(settings);
        m_validationMethodModel.saveSettingsTo(settings);
        m_sourceOsModel.saveSettingsTo(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_environmentNameModel.validateSettings(settings);
        CondaPackagesConfig.validateSettings(settings);
        m_validationMethodModel.validateSettings(settings);
        m_sourceOsModel.validateSettings(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_environmentNameModel.loadSettingsFrom(settings);
        m_packagesConfig.loadSettingsFrom(settings);
        m_validationMethodModel.loadSettingsFrom(settings);
        m_sourceOsModel.loadSettingsFrom(settings);
    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // Nothing to do.
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // Nothing to do.
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final Conda conda = createConda();
        final List<CondaEnvironmentIdentifier> environments;
        try {
            environments = conda.getEnvironments();
        } catch (IOException ex) {
            throw new InvalidSettingsException(ex.getMessage(), ex);
        }

        if (CondaEnvironmentsConfig.PLACEHOLDER_CONDA_ENV_NAME.equals(m_environmentNameModel.getStringValue())) {
            autoConfigureOnSourceMachine(conda, environments);
        }

        final List<CondaPackageSpec> packages = m_packagesConfig.getIncludedPackages();
        if (packages.isEmpty()) {
            throw new InvalidSettingsException(
                "Cannot propagate an empty environment. Please select at least one package.");
        }
        final List<CondaPackageSpec> installedByPip = new ArrayList<>();
        final boolean pipExists = Conda.filterPipInstalledPackages(packages, installedByPip, null);
        if (!pipExists && !installedByPip.isEmpty()) {
            throw new InvalidSettingsException("There are packages included in the environment that need to be "
                + "installed using pip. Therefore you also need to include package 'pip'.");
        }

        final String environmentName = m_environmentNameModel.getStringValue();
        final Optional<CondaEnvironmentIdentifier> environment = findEnvironment(environmentName, environments);
        if (environment.isPresent()) {
            pushEnvironmentFlowVariable(environmentName, environment.get().getDirectoryPath());
            return new PortObjectSpec[]{FlowVariablePortObjectSpec.INSTANCE};
        } else {
            // Environment not present: create it during execution.
            return null; // NOSONAR -- null is a valid return value that conforms to this method's contract.
        }
    }

    /**
     * Auto-configuration: select environment configured in the Preferences.
     */
    private void autoConfigureOnSourceMachine(final Conda conda, final List<CondaEnvironmentIdentifier> environments)
        throws InvalidSettingsException {
        final PythonVersion pythonVersion = PythonPreferences.getPythonVersionPreference();
        final String environmentDirectory = pythonVersion == PythonVersion.PYTHON2 //
            ? PythonPreferences.getPython2CondaEnvironmentDirectoryPath() //
            : PythonPreferences.getPython3CondaEnvironmentDirectoryPath();
        if (CondaEnvironmentsConfig.PLACEHOLDER_CONDA_ENV_DIR.equals(environmentDirectory)) {
            throw new InvalidSettingsException("Please make sure Conda is properly configured in the Preferences "
                + "of the KNIME Python Integration.\nThen select the Conda environment to propagate via the "
                + "configuration dialog of this node.");
        }

        final Optional<String> environmentNameOpt = environments.stream() //
            .filter(e -> e.getDirectoryPath().equals(environmentDirectory)) //
            .map(CondaEnvironmentIdentifier::getName) //
            .findFirst();
        final String environmentName;
        if (environmentNameOpt.isPresent()) {
            environmentName = environmentNameOpt.get();
        } else if (!environments.isEmpty()) {
            // Environment selected in the Preferences does not exist anymore, default to first environment in the list.
            environmentName = environments.get(0).getName();
        } else {
            throw new InvalidSettingsException("No Conda environments available.\nPlease review the Conda "
                + "installation specified in the Preferences of the KNIME Python Integration.\nThen select the "
                + "Conda environment to propagate via the configuration dialog of this node.");
        }
        m_environmentNameModel.setStringValue(environmentName);

        List<CondaPackageSpec> packages;
        try {
            packages = conda.getPackages(environmentName);
        } catch (final IOException ex) {
            throw new InvalidSettingsException(ex.getMessage(), ex);
        }
        m_packagesConfig.setPackages(packages, Collections.emptyList());
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final Conda conda = createConda();
        final String environmentName = m_environmentNameModel.getStringValue();
        final boolean sameOs = getCurrentOsType().equals(m_sourceOsModel.getStringValue());

        List<CondaPackageSpec> packages = m_packagesConfig.getIncludedPackages();
        if (!sameOs) {
            packages = packages.stream() //
                .filter(pkg -> !CROSS_PLATFORM_EXCLUDED_PACKAGES.contains(pkg.getName())) //
                .collect(Collectors.toList());
        }

        final Pair<Boolean, String> p = checkWhetherToCreateEnvironment(conda, environmentName, packages,
            m_validationMethodModel.getStringValue(), sameOs);
        final boolean createEnvironment = p.getFirst();
        final String creationMessage = p.getSecond();

        if (createEnvironment) {
            exec.setMessage(creationMessage);
            NodeLogger.getLogger(CondaEnvironmentPropagationNodeModel.class).info(creationMessage);

            conda.createEnvironment(environmentName, packages, sameOs,
                new Monitor(packages.size(), exec.getProgressMonitor()));
        }

        final List<CondaEnvironmentIdentifier> environments = conda.getEnvironments();
        final Optional<CondaEnvironmentIdentifier> environment = findEnvironment(environmentName, environments);
        if (!environment.isPresent()) {
            if (createEnvironment) {
                throw new IllegalStateException("Failed to create Conda environment '" + environmentName
                    + "' for unknown reasons.\nPlease check the log for any relevant information.");
            } else {
                throw new IllegalStateException("Conda environment '" + environmentName + "' does not exist anymore.\n"
                    + "Please ensure that KNIME has exclusive control over Conda environment creation and deletion.");
            }
        }

        if (createEnvironment) {
            // If a new environment has been created (either overwriting an existing environment or "overwriting" a
            // previously non-existent environment), the entries in the kernel queue that reference the old environment
            // are rendered obsolete and therefore need to be invalidated.
            // Unfortunately, clearing only the entries of the queue that reference the old environment is not
            // straightforwardly done in the queue's current implementation, therefore we need to clear the entire queue
            // for now.
            PythonKernelQueue.clear();
        }

        pushEnvironmentFlowVariable(environmentName, environment.get().getDirectoryPath());
        return new PortObject[]{FlowVariablePortObject.INSTANCE};
    }

    private static Pair<Boolean, String> checkWhetherToCreateEnvironment(final Conda conda,
        final String environmentName, final List<CondaPackageSpec> requiredPackages, final String validationMethod,
        final boolean sameOs) throws IOException, InvalidSettingsException {
        final boolean nameExists = findEnvironment(environmentName, conda.getEnvironments()).isPresent();

        final boolean createEnvironment;
        String creationMessage;
        if (VALIDATION_METHOD_NAME.equals(validationMethod)
            || VALIDATION_METHOD_NAME_PACKAGES.equals(validationMethod)) {
            if (nameExists && VALIDATION_METHOD_NAME_PACKAGES.equals(validationMethod)) {
                final List<CondaPackageSpec> existingPackages = conda.getPackages(environmentName);
                // Ignore/zero build specs if source and target operating systems differ.
                final UnaryOperator<CondaPackageSpec> adaptToOs =
                    pkg -> sameOs ? pkg : new CondaPackageSpec(pkg.getName(), pkg.getVersion(), "", pkg.getChannel());
                final Set<CondaPackageSpec> existingPackagesBuildSpecAdapted = existingPackages.stream() //
                    .map(adaptToOs) //
                    .collect(Collectors.toSet());
                final boolean requiredPackagesExist = requiredPackages.stream() //
                    .map(adaptToOs) //
                    .allMatch(existingPackagesBuildSpecAdapted::contains);
                createEnvironment = !requiredPackagesExist;
                creationMessage = "Environment '" + environmentName
                    + "' exists but does not contain all of the configured packages. Overwriting the environment.";
            } else {
                createEnvironment = !nameExists;
                creationMessage = "Environment '" + environmentName + "' does not exist. Creating the environment.";
            }
        } else if (VALIDATION_METHOD_OVERWRITE.equals(validationMethod)) {
            createEnvironment = true;
            creationMessage = nameExists //
                ? ("Environment '" + environmentName + "' exists. Overwriting the environment.") //
                : ("Environment '" + environmentName + "' does not exist. Creating the environment.");
        } else {
            throw new InvalidSettingsException("Invalid validation method selected. Allowed methods: "
                + Arrays.toString(createEnvironmentValidationMethodKeys()));
        }
        return new Pair<>(createEnvironment, creationMessage + " This might take a while...");
    }

    @Override
    protected void reset() {
        // Nothing to do.
    }

    private static String getCurrentOsType() {
        final String osType;
        if (SystemUtils.IS_OS_LINUX) {
            osType = SOURCE_OS_LINUX;
        } else if (SystemUtils.IS_OS_MAC) {
            osType = SOURCE_OS_MAC;
        } else if (SystemUtils.IS_OS_WINDOWS) {
            osType = SOURCE_OS_WINDOWS;
        } else {
            throw Conda.createUnknownOSException();
        }
        return osType;
    }

    static Conda createConda() throws InvalidSettingsException {
        final String condaInstallationPath = PythonPreferences.getCondaInstallationPath();
        try {
            return new Conda(condaInstallationPath, false);
        } catch (final IOException ex) {
            throw new InvalidSettingsException("Failed to reach out to the Conda installation located at '"
                + condaInstallationPath + "'.\nPlease make sure Conda is properly configured in the Preferences of "
                + "the KNIME Python Integration.\nThen select the Conda environment to propagate via the "
                + "configuration dialog of this node.", ex);
        }
    }

    private static Optional<CondaEnvironmentIdentifier> findEnvironment(final String environmentName,
        final List<CondaEnvironmentIdentifier> environments) {
        return environments.stream() //
            .filter(e -> e.getName().equals(environmentName)) //
            .findFirst();
    }

    private void pushEnvironmentFlowVariable(final String environmentName, final String environmentDirectoryPath) {
        pushFlowVariable(CondaEnvironmentPropagation.FLOW_VAR_NAME, CondaEnvironmentType.INSTANCE,
            new CondaEnvironmentSpec(new CondaEnvironmentIdentifier(environmentName, environmentDirectoryPath)));
    }

    private static final class Monitor extends CondaEnvironmentCreationMonitor {

        private final double m_progressPerPackage;

        private final NodeProgressMonitor m_monitor;

        private final NodeContext m_nodeContext;

        public Monitor(final int numPackages, final NodeProgressMonitor monitor) {
            m_progressPerPackage = 1d / numPackages;
            m_monitor = monitor;
            m_nodeContext = NodeContext.getContext();
        }

        @Override
        protected void handleWarningMessage(final String warning) {
            NodeContext.pushContext(m_nodeContext);
            try {
                NodeLogger.getLogger(CondaEnvironmentPropagationNodeModel.class).warn(warning);
            } finally {
                NodeContext.removeLastContext();
            }
        }

        @Override
        protected void handlePackageDownloadProgress(final String currentPackage, final boolean packageFinished,
            final double progress) {
            if (!packageFinished) {
                m_monitor.setMessage("Downloading package '" + currentPackage + "'...");
            } else {
                final Double totalProgress = m_monitor.getProgress();
                m_monitor.setProgress((totalProgress != null ? totalProgress : 0d) + m_progressPerPackage,
                    "Creating Conda environment...");
            }
        }
    }
}
