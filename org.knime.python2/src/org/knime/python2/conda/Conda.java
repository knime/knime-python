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
 *   Feb 2, 2019 (marcel): created
 */
package org.knime.python2.conda;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.SystemUtils;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.FileUtil;
import org.knime.core.util.Version;
import org.knime.python2.CondaPythonCommand;
import org.knime.python2.PythonCommand;
import org.knime.python2.PythonVersion;
import org.knime.python2.envconfigs.CondaEnvironments;
import org.knime.python2.kernel.PythonCanceledExecutionException;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * Interface to an external Conda installation.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class Conda {

    private static final Version CONDA_MINIMUM_VERSION = new Version(4, 6, 2);

    private static final Version CONDA_ENV_EXPORT_FROM_HISTORY_MINIMUM_VERSION = new Version(4, 7, 12);

    private static final String ROOT_ENVIRONMENT_NAME = "base";

    private static final String ROOT_ENVIRONMENT_LEGACY_NAME = "root";

    private static final String DEFAULT_PYTHON2_ENV_PREFIX = "py2_knime";

    private static final String DEFAULT_PYTHON3_ENV_PREFIX = "py3_knime";

    private static final String JSON = "--json";

    private static final Pattern CHANNEL_SEPARATOR = Pattern.compile("::");

    private static final Pattern VERSION_BUILD_SEPARATOR = Pattern.compile("=");

    /**
     * Creates and returns a {@link PythonCommand} that describes a Python process of the given Python version that is
     * run in the Conda environment identified by the given Conda installation directory and the given Conda environment
     * name.<br>
     * The validity of the given arguments is not tested.
     *
     * @param pythonVersion The Python version of the Python environment.
     * @param condaInstallationDirectoryPath The path to the directory of the Conda installation.
     * @param environmentDirectoryPath The path to the directory of the Conda environment. The directory does not
     *            necessarily need to be located inside the Conda installation directory, which is why a path is
     *            required.
     * @return A command to start a Python process in the given environment using the given Conda installation.
     */
    public static PythonCommand createPythonCommand(final PythonVersion pythonVersion,
        final String condaInstallationDirectoryPath, final String environmentDirectoryPath) {
        return new CondaPythonCommand(pythonVersion, condaInstallationDirectoryPath, environmentDirectoryPath);
    }

    /**
     * Converts a version string of the form "conda &ltmajor&gt.&ltminor&gt.&ltmicro&gt" into a {@link Version} object.
     *
     * @param condaVersionString The version string.
     * @return The parsed version.
     * @throws IllegalArgumentException If the version string cannot be parsed or produces an invalid version.
     */
    public static Version condaVersionStringToVersion(String condaVersionString) {
        try {
            condaVersionString = condaVersionString.split(" ")[1].trim();
            return new Version(condaVersionString);
        } catch (final Exception ex) {
            throw new IllegalArgumentException(ex.getMessage(), ex);
        }
    }

    /**
     * Path to the Conda executable.
     */
    private final String m_executable;

    /**
     * Lazily initialized by {@link #getEnvironments()}.
     */
    private String m_rootPrefix = null;

    /**
     * Creates an interface to the given Conda installation. Tests the validity of the given path and the functioning of
     * the installation, and throws an {@link IOException} if either of the tests fails.
     *
     * @param condaInstallationDirectoryPath The path to the root directory of the Conda installation.
     *
     * @throws IOException If the given directory does not point to a valid and functioning Conda installation. This
     *             includes cases where the given directory or any relevant files within that directory cannot be read
     *             (and/or possibly executed) by this application.
     */
    public Conda(final String condaInstallationDirectoryPath) throws IOException {
        this(condaInstallationDirectoryPath, true);
    }

    /**
     * Creates an interface to the given Conda installation. Tests the validity of the given path. Optionally, tests the
     * functioning of the installation. Throws an {@link IOException} if either of the tests fails. Performing the
     * functionality test is recommended but can be omitted for performance reasons in cases where the functioning of
     * the installation is known a priori or not strictly required at construction time of this instance.
     *
     * @param condaInstallationDirectoryPath The path to the root directory of the Conda installation.
     * @param testInstallation Whether to the test the functioning of the installation.
     * @throws IOException If the given directory does not point to a valid and functioning Conda installation. This
     *             includes cases where the given directory or any relevant files within that directory cannot be read
     *             (and/or possibly executed) by this application.
     */
    public Conda(String condaInstallationDirectoryPath, final boolean testInstallation) throws IOException {
        final File directoryFile = resolveToInstallationDirectoryFile(condaInstallationDirectoryPath);
        try {
            condaInstallationDirectoryPath = directoryFile.getCanonicalPath();
        } catch (final SecurityException ex) {
            // Stick with the unresolved path.
            condaInstallationDirectoryPath = directoryFile.getPath();
        }
        m_executable = getExecutableFromInstallationDirectoryForOS(condaInstallationDirectoryPath);

        if (testInstallation) {
            testInstallation();
        }
    }

    private static File resolveToInstallationDirectoryFile(final String installationDirectoryPath) throws IOException {
        final File installationDirectory = new File(installationDirectoryPath);
        try {
            if (!installationDirectory.exists()) {
                throw new IOException("The directory at the given path does not exist.\nPlease specify the path to "
                    + "the directory of your local Conda installation.");
            }
            if (!installationDirectory.isDirectory()) {
                throw new IOException("The given path does not point to a directory.\nPlease point to the root "
                    + "directory of your local Conda installation.");
            }
        } catch (final SecurityException ex) {
            final String errorMessage = "The directory at the given path cannot be read. Please make sure KNIME has "
                + "the proper access rights for the directory and retry.";
            throw new IOException(errorMessage, ex);
        }
        return installationDirectory;
    }

    private static String getExecutableFromInstallationDirectoryForOS(final String installationDirectoryPath)
        throws IOException {
        String[] relativePathToExecutableSegments;
        if (SystemUtils.IS_OS_LINUX || SystemUtils.IS_OS_MAC) {
            relativePathToExecutableSegments = new String[]{"bin", "conda"};
        } else if (SystemUtils.IS_OS_WINDOWS) {
            relativePathToExecutableSegments = new String[]{"condabin", "conda.bat"};
        } else {
            throw createUnknownOSException();
        }
        Path executablePath;
        try {
            executablePath = resolveToExecutablePath(installationDirectoryPath, relativePathToExecutableSegments);
        } catch (final IOException ex) {
            if (SystemUtils.IS_OS_WINDOWS) {
                // Legacy support on Windows. Older installations of Conda don't have "condabin/conda.bat". We won't
                // support such versions (cf. #testInstallation()) but still want to be able to resolve them. This
                // allows us to print a more precise error message ("wrong version installed" vs. "not installed at
                // all").
                relativePathToExecutableSegments = new String[]{"Scripts", "conda.exe"};
                executablePath = resolveToExecutablePath(installationDirectoryPath, relativePathToExecutableSegments);
            } else {
                throw ex;
            }
        }
        return executablePath.toString();
    }

    private static Path resolveToExecutablePath(final String installationDirectoryPath,
        final String[] relativePathToExecutableSegments) throws IOException {
        final Path executablePath;
        try {
            executablePath = Paths.get(installationDirectoryPath, relativePathToExecutableSegments);
        } catch (final InvalidPathException ex) {
            final String errorMessage = ex.getMessage() + "\nThis is an implementation error.";
            throw new IOException(errorMessage, ex);
        }
        try {
            if (!executablePath.toFile().exists()) {
                NodeLogger.getLogger(Conda.class)
                    .debug("Specified Conda executable at '" + executablePath.toFile().getPath() + "' does not exist.");
                throw new IOException("The given path does not point to a valid Conda installation.\nPlease point to "
                    + "the root directory of your local Conda installation.");
            }
        } catch (final UnsupportedOperationException ex) {
            // Skip test.
        }
        return executablePath;
    }

    /**
     * @return An {@link UnsupportedOperationException} stating that the local operating system could not be detected or
     *         is not supported.
     */
    public static UnsupportedOperationException createUnknownOSException() {
        final String osName = SystemUtils.OS_NAME;
        if (osName == null) {
            throw new UnsupportedOperationException(
                "Could not detect your operating system. This is necessary for Conda environment generation and use. "
                    + "Please make sure KNIME has the proper access rights to your system.");
        } else {
            throw new UnsupportedOperationException(
                "Conda environment generation and use is only supported on Windows, Mac, and Linux. Your operating "
                    + "system is: " + SystemUtils.OS_NAME);
        }
    }

    /**
     * Test Conda installation by trying to get its version. Method throws an exception if Conda could not be called
     * properly. We also check the version bound since we currently require Conda {@link #CONDA_MINIMUM_VERSION} or
     * later.
     *
     * @throws IOException If the installation test failed.
     */
    private void testInstallation() throws IOException {
        String versionString = getVersionString();
        final Version version;
        try {
            version = condaVersionStringToVersion(versionString);
        } catch (final Exception ex) {
            // Skip test if we can't identify version.
            NodeLogger.getLogger(Conda.class).warn("Could not detect installed Conda version. Please note that a "
                + "minimum version of " + CONDA_MINIMUM_VERSION + " is required.", ex);
            return;
        }
        if (version.compareTo(CONDA_MINIMUM_VERSION) < 0) {
            // Root environment name differs between older and newer versions of Conda.
            final String rootEnvironmentName =
                version.compareTo(new Version(4, 4, 0)) < 0 ? ROOT_ENVIRONMENT_LEGACY_NAME : ROOT_ENVIRONMENT_NAME;
            throw new IOException("Conda version is " + version.toString() + ". Required minimum version is "
                + CONDA_MINIMUM_VERSION + ".\nPlease update Conda (e.g., by executing \"conda update -n "
                + rootEnvironmentName + " conda\" in a terminal) and retry.");
        }
    }

    /**
     * {@code conda --version}
     *
     * @return The raw output of the corresponding Conda command.
     * @throws IOException If an error occurs during execution of the underlying command.
     * @see #condaVersionStringToVersion(String)
     */
    public String getVersionString() throws IOException {
        final AtomicReference<String> version = new AtomicReference<>();
        callCondaAndAwaitTermination(new CondaExecutionMonitor() {

            @Override
            protected void handleCustomNonJsonOutput(final String output) {
                version.set(output);
            }
        }, "--version");
        return version.get();
    }

    /**
     * {@code conda env list}
     *
     * @return The descriptions of the existing Conda environments.
     * @throws IOException If an error occurs during execution of the underlying command.
     */
    public List<CondaEnvironmentIdentifier> getEnvironments() throws IOException {
        if (m_rootPrefix == null) {
            m_rootPrefix = getRootPrefix();
        }
        final List<CondaEnvironmentIdentifier> environments = new ArrayList<>();
        callCondaAndAwaitTermination(new CondaExecutionMonitor() {

            @Override
            protected void handleCustomJsonOutput(final TreeNode json) {
                final ArrayNode environmentsJson = (ArrayNode)json.get("envs");
                for (int i = 0; i < environmentsJson.size(); i++) {
                    final String environmentPath = environmentsJson.get(i).textValue();
                    final String environmentName;
                    if (environmentPath.equals(m_rootPrefix)) {
                        environmentName = ROOT_ENVIRONMENT_NAME;
                    } else {
                        environmentName = new File(environmentPath).getName();
                    }
                    environments.add(new CondaEnvironmentIdentifier(environmentName, environmentPath));
                }
            }
        }, "env", "list", JSON);
        return environments;

    }

    private List<String> getEnvironmentNames() throws IOException {
        return getEnvironments() //
            .stream() //
            .map(CondaEnvironmentIdentifier::getName) //
            .collect(Collectors.toList());
    }

    private String getRootPrefix() throws IOException {
        final AtomicReference<String> rootPrefix = new AtomicReference<>();
        callCondaAndAwaitTermination(new CondaExecutionMonitor() {

            @Override
            protected void handleCustomJsonOutput(final TreeNode json) {
                rootPrefix.set(((JsonNode)json.get("root_prefix")).textValue());
            }
        }, "info", JSON);
        return rootPrefix.get();
    }

    /**
     * {@code conda list --name <environmentName>}
     *
     * @param environmentName The name of the environment whose packages to return.
     * @return The packages contained in the environment.
     * @throws IOException If an error occurs during execution of the underlying Conda command.
     */
    public List<CondaPackageSpec> getPackages(final String environmentName) throws IOException {
        final List<CondaPackageSpec> packages = new ArrayList<>();
        callCondaAndAwaitTermination(new CondaExecutionMonitor() {

            @Override
            protected void handleCustomJsonOutput(final TreeNode json) {
                final ArrayNode packagesJson = (ArrayNode)json;
                for (int i = 0; i < packagesJson.size(); i++) {
                    final JsonNode packageJson = packagesJson.get(i);
                    final String name = packageJson.get("name").textValue();
                    final String version = packageJson.get("version").textValue();
                    final String build = packageJson.get("build_string").textValue();
                    final String channel = packageJson.get("channel").textValue();
                    packages.add(new CondaPackageSpec(name, version, build, channel));
                }
            }
        }, "list", "--name", environmentName, JSON);
        return packages;
    }

    /**
     * {@code conda env export --name <environmentName> --from-history} is only available from Conda 4.7.12 onward. This
     * method determines whether the corresponding method in this class -- {@link #getPackageNamesFromHistory(String)}
     * -- can safely be invoked.
     *
     * @return {@code true} if {@link #getPackageNamesFromHistory(String)} is available, {@code false} otherwise (also
     *         if determining the Conda version failed).
     */
    public boolean isPackageNamesFromHistoryAvailable() {
        try {
            final String versionString = getVersionString();
            final Version version = condaVersionStringToVersion(versionString);
            return version.compareTo(CONDA_ENV_EXPORT_FROM_HISTORY_MINIMUM_VERSION) >= 0;
        } catch (final Exception ex) {
            NodeLogger.getLogger(Conda.class).debug("Could not detect installed Conda version.", ex);
            return false;
        }
    }

    /**
     * {@code conda env export --name <environmentName> --from-history}
     * <P>
     * Channel and build/version affixes are stripped from the raw output of the command.
     * <P>
     * Note that this method is only available from Conda 4.7.12 onward, see
     * {@link #isPackageNamesFromHistoryAvailable()}.
     *
     * @param environmentName The name of the environment whose packages to return.
     * @return The names of the explicitly installed packages contained in the environment.
     * @throws IOException If an error occurs during execution of the underlying Conda command.
     */
    public List<String> getPackageNamesFromHistory(final String environmentName) throws IOException {
        final List<String> packageNames = new ArrayList<>();
        callCondaAndAwaitTermination(new CondaExecutionMonitor() {

            @Override
            protected void handleCustomJsonOutput(final TreeNode json) {
                final ArrayNode packagesJson = (ArrayNode)json.get("dependencies");
                for (int i = 0; i < packagesJson.size(); i++) {
                    String name = packagesJson.get(i).textValue();
                    final String[] splitChannel = CHANNEL_SEPARATOR.split(name, 2);
                    name = splitChannel[splitChannel.length - 1];
                    final String[] splitVersionAndBuild = VERSION_BUILD_SEPARATOR.split(name, 2);
                    name = splitVersionAndBuild[0];
                    packageNames.add(name);
                }
            }
        }, "env", "export", "--name", environmentName, "--from-history", JSON);
        return packageNames;
    }

    /**
     * @return A default name for a Python 2 environment. It is ensured that the name does not already exist in this
     *         Conda installation.
     * @throws IOException If an error occurs during execution of the underlying Conda commands.
     */
    public String getDefaultPython2EnvironmentName() throws IOException {
        return getDefaultPythonEnvironmentName(PythonVersion.PYTHON2, "");
    }

    /**
     * @return A default name for a Python 3 environment. It is ensured that the name does not already exist in this
     *         Conda installation.
     * @throws IOException If an error occurs during execution of the underlying Conda commands.
     */
    public String getDefaultPython3EnvironmentName() throws IOException {
        return getDefaultPythonEnvironmentName(PythonVersion.PYTHON3, "");
    }

    /**
     * @param suffix a suffix for the environment name
     * @return A name for a Python 2 environment. It is ensured that the name does not already exist in this Conda
     *         installation.
     * @throws IOException If an error occurs during execution of the underlying Conda commands.
     */
    public String getPython2EnvironmentName(final String suffix) throws IOException {
        return getDefaultPythonEnvironmentName(PythonVersion.PYTHON2, suffix);
    }

    /**
     * @param suffix a sufix for the environment name
     * @return A name for a Python 3 environment. It is ensured that the name does not already exist in this Conda
     *         installation.
     * @throws IOException If an error occurs during execution of the underlying Conda commands.
     */
    public String getPython3EnvironmentName(final String suffix) throws IOException {
        return getDefaultPythonEnvironmentName(PythonVersion.PYTHON3, suffix);
    }

    private String getDefaultPythonEnvironmentName(final PythonVersion pythonVersion, final String suffix)
        throws IOException {
        final String environmentPrefix =
            (pythonVersion == PythonVersion.PYTHON2 ? DEFAULT_PYTHON2_ENV_PREFIX : DEFAULT_PYTHON3_ENV_PREFIX)
                + (suffix.isEmpty() ? "" : ("_" + suffix));
        String environmentName = environmentPrefix;
        long possibleEnvironmentSuffix = 1;
        final List<String> environmentNames = getEnvironmentNames();
        while (environmentNames.contains(environmentName)) {
            environmentName = environmentPrefix + "_" + possibleEnvironmentSuffix;
            possibleEnvironmentSuffix++;
        }
        return environmentName;
    }

    /**
     * Creates a new Python 2 Conda environment that contains all packages required by the KNIME Python integration.
     *
     * @param monitor Receives progress of the creation process. Allows to cancel the environment creation from within
     *            another thread.
     * @param environmentName The name of the environment. Must not already exist in this Conda installation. May be
     *            {@code null} or empty in which case a {@link #getDefaultPython2EnvironmentName() default name} is
     *            used.
     * @return A description of the created Conda environment.
     * @throws IOException If an error occurs during execution of the underlying Conda commands. This also includes
     *             cases where an environment of name {@code environmentName} is already present in this Conda
     *             installation.
     * @throws PythonCanceledExecutionException If environment creation was canceled via the given monitor.
     * @throws UnsupportedOperationException If creating a default environment is not supported for the local operating
     *             system.
     */
    public CondaEnvironmentIdentifier createDefaultPython2Environment(final String environmentName,
        final CondaEnvironmentCreationMonitor monitor) throws IOException, PythonCanceledExecutionException {
        return createEnvironmentFromFile(PythonVersion.PYTHON2, CondaEnvironments.getPathToPython2CondaConfigFile(),
            environmentName, monitor);
    }

    /**
     * Creates a new Python 3 Conda environment that contains all packages required by the KNIME Python integration.
     *
     * @param monitor Receives progress of the creation process. Allows to cancel the environment creation from within
     *            another thread.
     * @param environmentName The name of the environment. Must not already exist in this Conda installation. May be
     *            {@code null} or empty in which case a {@link #getDefaultPython3EnvironmentName() default name} is
     *            used.
     * @return A description of the created Conda environment.
     * @throws IOException If an error occurs during execution of the underlying Conda commands. This also includes
     *             cases where an environment of name {@code environmentName} is already present in this Conda
     *             installation.
     * @throws PythonCanceledExecutionException If environment creation was canceled via the given monitor.
     * @throws UnsupportedOperationException If creating a default environment is not supported for the local operating
     *             system.
     */
    public CondaEnvironmentIdentifier createDefaultPython3Environment(final String environmentName,
        final CondaEnvironmentCreationMonitor monitor) throws IOException, PythonCanceledExecutionException {
        return createEnvironmentFromFile(PythonVersion.PYTHON3, CondaEnvironments.getPathToPython3CondaConfigFile(),
            environmentName, monitor);
    }

    /**
     * {@code conda env create --file <pathToFile> --name <environmentName or generated name>}.<br>
     * The environment name specified in the file is ignored and replaced by either {@code environmentName} if it's
     * non-{@code null} and non-empty or a unique name that considers the already existing environments of this Conda
     * installation. The generated name is based on the given Python version.
     *
     * @param pythonVersion The major version of the Python environment to create. Determines the generated name of the
     *            environment if {@code environmentName} is {@code null}.
     * @param pathToFile The path to the environment description file.
     * @param environmentName The name of the environment. Must not already exist in this Conda installation. May be
     *            {@code null} or empty in which case a default name is used.
     * @param monitor Receives progress of the creation process. Allows to cancel the environment creation from within
     *            another thread.
     * @return A description of the created environment.
     * @throws IOException If an error occurs during execution of the underlying command. This also includes cases where
     *             an environment of name {@code environmentName} is already present in this Conda installation.
     * @throws PythonCanceledExecutionException If environment creation was canceled via the given monitor.
     */
    public CondaEnvironmentIdentifier createEnvironmentFromFile(final PythonVersion pythonVersion,
        final String pathToFile, String environmentName, final CondaEnvironmentCreationMonitor monitor)
        throws IOException, PythonCanceledExecutionException {
        if (environmentName == null || environmentName.isEmpty()) {
            environmentName = getDefaultPythonEnvironmentName(pythonVersion, "");
        } else {
            final List<String> existingEnvironmentNames = getEnvironmentNames();
            if (existingEnvironmentNames.contains(environmentName)) {
                throw new IOException(
                    "Conda environment '" + environmentName + "' already exists. Please use a different, unique name.");
            }
        }
        IOException failure = null;
        try {
            createEnvironmentFromFile(pathToFile, environmentName, false, monitor);
        } catch (IOException ex) {
            failure = ex;
        }
        // Check if environment creation was successful. Fail if not.

        final List<CondaEnvironmentIdentifier> environments = getEnvironments();
        for (final CondaEnvironmentIdentifier environment : environments) {
            if (Objects.equals(environmentName, environment.getName())) {
                return environment;
            }
        }
        if (failure == null) {
            failure = new IOException("Failed to create Conda environment.");
        }
        throw failure;
    }

    /**
     * {@code conda env create --file <pathToFile> [--name <optionalEnvironmentName>] [--force]}
     */
    private void createEnvironmentFromFile(final String pathToFile, final String optionalEnvironmentName,
        final boolean overwriteExistingEnvironment, final CondaEnvironmentCreationMonitor monitor)
        throws IOException, PythonCanceledExecutionException {
        final List<String> arguments = new ArrayList<>(6);
        Collections.addAll(arguments, "env", "create", "--file", pathToFile);
        if (optionalEnvironmentName != null) {
            Collections.addAll(arguments, "--name", optionalEnvironmentName);
        }
        if (overwriteExistingEnvironment) {
            arguments.add("--force");
        }
        arguments.add(JSON);
        callCondaAndMonitorExecution(monitor, arguments.toArray(new String[0]));
    }

    /**
     * {@code conda env create --file <file generated from arguments> --name <environmentName> --force}
     * <P>
     * Overwrites environment {@code <environmentName>} if it already exists.
     *
     * @param environmentName The name of the environment to create. If an environment with the given name already
     *            exists in this Conda installation, it will be overwritten.
     * @param packages The packages to install.
     * @param includeBuildSpecs If {@code true}, the packages' {@link CondaPackageSpec#getBuild() build specs} are
     *            respected/enforced during environment creation, otherwise they are ignored.
     * @param monitor Receives progress of the creation process. Allows to cancel the environment creation from within
     *            another thread.
     * @return The environment.yml file of the created environment. Note that this is a temporary file that is deleted
     *         when the JVM is shut down. Manually copy it if you want to preserve it.
     * @throws IOException If an error occurs during execution of the underlying command.
     * @throws PythonCanceledExecutionException If environment creation was canceled via the given monitor.
     */
    public File createEnvironment(final String environmentName, final List<CondaPackageSpec> packages,
        final boolean includeBuildSpecs, final CondaEnvironmentCreationMonitor monitor)
        throws IOException, PythonCanceledExecutionException {
        final List<CondaPackageSpec> installedByPip = new ArrayList<>();
        final List<CondaPackageSpec> installedByConda = new ArrayList<>();
        final boolean pipExists = filterPipInstalledPackages(packages, installedByPip, installedByConda);

        final List<Object> dependencies = installedByConda.stream() //
            .map(pkg -> {
                String dependency = pkg.getChannel() + "::" + pkg.getName() + "=" + pkg.getVersion();
                if (includeBuildSpecs) {
                    dependency += "=" + pkg.getBuild();
                }
                return dependency;
            }) //
            .collect(Collectors.toList());

        if (!installedByPip.isEmpty()) {
            if (!pipExists) {
                throw new IllegalArgumentException("There are packages in the environment that are to be installed "
                    + "using pip. Therefore you also need to include package 'pip'.");
            }
            final List<String> pipDependencies = installedByPip.stream() //
                .map(p -> p.getName() + "==" + p.getVersion()) //
                .collect(Collectors.toList());
            final Map<String, Object> pip = new LinkedHashMap<>();
            pip.put("pip", pipDependencies);
            dependencies.add(pip);
        }

        final Map<String, Object> entries = new LinkedHashMap<>();
        entries.put("name", environmentName);
        entries.put("dependencies", dependencies);

        final File environmentFile = FileUtil.createTempFile("environment_", ".yml");
        final DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setIndent(3);
        options.setIndicatorIndent(2);
        final Yaml yaml = new Yaml(options);
        try (final Writer writer =
            new OutputStreamWriter(new FileOutputStream(environmentFile), StandardCharsets.UTF_8)) {
            yaml.dump(entries, writer);
        }

        createEnvironmentFromFile(environmentFile.getPath(), null, true, monitor);
        return environmentFile;
    }

    /**
     * Traverses {@code packages} and adds each package either to {@code outInstalledByPip} or to
     * {@code outInstalledByConda} depending on its source channel. Also returns whether package {@code pip} is
     * contained in {@code packages}.
     *
     * @param packages The packages to filter.
     * @param outInstalledByPip Will be populated with the packages installed via pip (i.e., from a PyPI channel).
     * @param outInstalledByConda Will be populated with the packages installed via conda (i.e., from a Conda channel).
     * @return {@code true} if {@code pip} is contained in the given packages, {@code false} otherwise.
     */
    public static boolean filterPipInstalledPackages(final List<CondaPackageSpec> packages,
        final List<CondaPackageSpec> outInstalledByPip, final List<CondaPackageSpec> outInstalledByConda) {
        boolean pipExists = false;
        for (final CondaPackageSpec pkg : packages) {
            if (!pipExists && "pip".equals(pkg.getName())) {
                pipExists = true;
            }
            if ("pypi".equals(pkg.getChannel())) {
                if (outInstalledByPip != null) {
                    outInstalledByPip.add(pkg);
                }
            } else {
                if (outInstalledByConda != null) {
                    outInstalledByConda.add(pkg);
                }
            }
        }
        return pipExists;
    }

    private void callCondaAndAwaitTermination(final CondaExecutionMonitor monitor, final String... arguments)
        throws IOException {
        try {
            callCondaAndMonitorExecution(monitor, arguments);
        } catch (final PythonCanceledExecutionException ex) {
            throw new IOException("Execution was interrupted.", ex);
        }
    }

    private void callCondaAndMonitorExecution(final CondaExecutionMonitor monitor, final String... arguments)
        throws IOException, PythonCanceledExecutionException {
        final boolean hasJsonOutput = Arrays.asList(arguments).contains(JSON);
        final Process conda = startCondaProcess(arguments);
        try {
            monitor.monitorExecution(conda, hasJsonOutput);
        } finally {
            conda.destroy(); // Should not be necessary, but let's play safe here.
        }
    }

    private Process startCondaProcess(final String... arguments) throws IOException {
        final List<String> argumentList = new ArrayList<>(1 + arguments.length);
        argumentList.add(m_executable);
        Collections.addAll(argumentList, arguments);
        final ProcessBuilder pb = new ProcessBuilder(argumentList);
        return pb.start();
    }
}
