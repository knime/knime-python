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
package org.knime.python2;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UncheckedIOException;
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
import java.util.concurrent.Executors;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.FileUtil;
import org.knime.core.util.Version;
import org.knime.python2.envconfigs.CondaEnvironments;
import org.knime.python2.kernel.PythonCancelable;
import org.knime.python2.kernel.PythonCanceledExecutionException;
import org.knime.python2.util.PythonUtils;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Interface to an external Conda installation.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class Conda {

    private static final Version CONDA_MINIMUM_VERSION = new Version(4, 6, 2);

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
     * Creates an interface to the given Conda installation. Tests the validity of the installation and throws an
     * {@link IOException} if it is invalid.
     *
     * @param condaInstallationDirectoryPath The path to the root directory of the Conda installation.
     *
     * @throws IOException If the given directory does not point to a valid Conda installation. This includes cases
     *             where the given directory or any relevant files within that directory cannot be read (and/or possibly
     *             executed) by this application.
     */
    public Conda(String condaInstallationDirectoryPath) throws IOException {
        final File directoryFile = resolveToInstallationDirectoryFile(condaInstallationDirectoryPath);
        try {
            condaInstallationDirectoryPath = directoryFile.getCanonicalPath();
        } catch (final SecurityException ex) {
            // Stick with the unresolved path.
            condaInstallationDirectoryPath = directoryFile.getPath();
        }
        m_executable = getExecutableFromInstallationDirectoryForOS(condaInstallationDirectoryPath);

        testInstallation();
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

    static UnsupportedOperationException createUnknownOSException() {
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
        return callCondaAndAwaitTermination("--version");
    }

    /**
     * {@code conda env list}
     *
     * @return The descriptions of the existing Conda environments.
     * @throws IOException If an error occurs during execution of the underlying command.
     */
    public List<CondaEnvironmentSpec> getEnvironments() throws IOException {
        if (m_rootPrefix == null) {
            m_rootPrefix = getRootPrefix();
        }
        final String jsonOutput = callCondaAndAwaitTermination("env", "list", JSON);
        try (final JsonReader reader = Json.createReader(new StringReader(jsonOutput))) {
            final JsonArray environmentsJson = reader.readObject().getJsonArray("envs");
            final List<CondaEnvironmentSpec> environments = new ArrayList<>(environmentsJson.size());
            for (int i = 0; i < environmentsJson.size(); i++) {
                final String environmentPath = environmentsJson.getString(i);
                final String environmentName;
                if (environmentPath.equals(m_rootPrefix)) {
                    environmentName = ROOT_ENVIRONMENT_NAME;
                } else {
                    environmentName = new File(environmentPath).getName();
                }
                environments.add(new CondaEnvironmentSpec(environmentName, environmentPath));
            }
            return environments;
        }
    }

    private List<String> getEnvironmentNames() throws IOException {
        return getEnvironments() //
            .stream() //
            .map(CondaEnvironmentSpec::getName) //
            .collect(Collectors.toList());
    }

    private String getRootPrefix() throws IOException {
        final String jsonOutput = callCondaAndAwaitTermination("info", JSON);
        try (final JsonReader reader = Json.createReader(new StringReader(jsonOutput))) {
            return reader.readObject().getString("root_prefix");
        }
    }

    /**
     * {@code conda list --name <environmentName>}
     *
     * @param environmentName The name of the environment whose packages to return.
     * @return The packages contained in the environment.
     * @throws IOException If an error occurs during execution of the underlying Conda command.
     */
    public List<CondaPackageSpec> getPackages(final String environmentName) throws IOException {
        final String jsonOutput = callCondaAndAwaitTermination("list", "--name", environmentName, JSON);
        try (final JsonReader reader = Json.createReader(new StringReader(jsonOutput))) {
            final JsonArray packagesJson = reader.readArray();
            final List<CondaPackageSpec> packages = new ArrayList<>(packagesJson.size());
            for (int i = 0; i < packagesJson.size(); i++) {
                final JsonObject packageJson = packagesJson.getJsonObject(i);
                final String name = packageJson.getString("name");
                final String version = packageJson.getString("version");
                final String build = packageJson.getString("build_string");
                final String channel = packageJson.getString("channel");
                packages.add(new CondaPackageSpec(name, version, build, channel));
            }
            return packages;
        }
    }

    /**
     * {@code conda env export --name <environmentName> --from-history}
     * <P>
     * Channel and build/version affixes are stripped from the raw output of the command.
     *
     * @param environmentName The name of the environment whose packages to return.
     * @return The names of the explicitly installed packages contained in the environment.
     * @throws IOException If an error occurs during execution of the underlying Conda command.
     */
    public List<String> getPackageNamesFromHistory(final String environmentName) throws IOException {
        final String jsonOutput =
            callCondaAndAwaitTermination("env", "export", "--name", environmentName, "--from-history", JSON);
        try (final JsonReader reader = Json.createReader(new StringReader(jsonOutput))) {
            final JsonArray packagesJson = reader.readObject().getJsonArray("dependencies");
            final List<String> packageNames = new ArrayList<>(packagesJson.size());
            for (int i = 0; i < packagesJson.size(); i++) {
                String name = packagesJson.getString(i);
                final String[] splitChannel = CHANNEL_SEPARATOR.split(name, 2);
                name = splitChannel[splitChannel.length - 1];
                final String[] splitVersionAndBuild = VERSION_BUILD_SEPARATOR.split(name, 2);
                name = splitVersionAndBuild[0];
                packageNames.add(name);
            }
            return packageNames;
        }
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
    public CondaEnvironmentSpec createDefaultPython2Environment(final String environmentName,
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
    public CondaEnvironmentSpec createDefaultPython3Environment(final String environmentName,
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
    public CondaEnvironmentSpec createEnvironmentFromFile(final PythonVersion pythonVersion, final String pathToFile,
        String environmentName, final CondaEnvironmentCreationMonitor monitor)
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

        final List<CondaEnvironmentSpec> environments = getEnvironments();
        for (final CondaEnvironmentSpec environment : environments) {
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

    private void callCondaAndMonitorExecution(final CondaExecutionMonitor monitor, final String... arguments)
        throws IOException, PythonCanceledExecutionException {
        final Process conda = startCondaProcess(arguments);
        Thread outputListener = null;
        Thread errorListener = null;
        try {
            outputListener = new Thread(() -> monitor.handleOutputStream(conda.getInputStream()));
            errorListener = new Thread(() -> monitor.handleErrorStream(conda.getErrorStream()));
            outputListener.start();
            errorListener.start();
            final int condaExitCode = awaitTermination(conda, monitor);
            if (condaExitCode != 0) {
                throw new IOException("Conda process terminated with error code " + condaExitCode + ".");
            }
        } finally {
            // Should not be necessary, but let's play safe here.
            conda.destroy();
            if (outputListener != null) {
                outputListener.interrupt();
            }
            if (errorListener != null) {
                errorListener.interrupt();
            }
        }
    }

    private String callCondaAndAwaitTermination(final String... arguments) throws IOException {
        final boolean isJsonOutput = Arrays.asList(arguments).contains(JSON);
        final Process conda = startCondaProcess(arguments);
        try {
            // Get regular output.
            final StringWriter outputWriter = new StringWriter();
            IOUtils.copy(conda.getInputStream(), outputWriter, StandardCharsets.UTF_8);
            final String regularOutput = outputWriter.toString();
            // Get error output.
            final StringWriter errorWriter = new StringWriter();
            IOUtils.copy(conda.getErrorStream(), errorWriter, StandardCharsets.UTF_8);
            String errorOutput = errorWriter.toString();

            int condaExitCode = awaitTermination(conda, null);
            if (condaExitCode != 0) {
                String errorMessage = "";
                if (isJsonOutput) {
                    try (final JsonReader reader = Json.createReader(new StringReader(regularOutput))) {
                        final JsonObject json = reader.readObject();
                        if (json.containsKey("error")) {
                            if (json.containsKey("message")) {
                                errorMessage = json.getString("message");
                            } else {
                                errorMessage = json.getString("error");
                            }
                        }
                    }
                }
                if (!isJsonOutput || errorMessage.isEmpty()) {
                    if (!errorOutput.isEmpty() && !isWarning(errorOutput)) {
                        errorMessage = "Failed to execute Conda";
                        if (errorOutput.contains("CONNECTION FAILED") && errorOutput.contains("SSLError")) {
                            errorMessage += ".\nPlease uninstall and reinstall Conda.\n";
                        } else {
                            errorMessage += ":\n";
                        }
                        errorMessage += errorOutput;
                    } else {
                        errorMessage = "Conda process terminated with error code " + condaExitCode + ".";
                        if (!errorOutput.isEmpty()) {
                            errorMessage += "\nFurther output: " + errorMessage;
                        }
                    }
                }
                throw new IOException(errorMessage);
            }
            return regularOutput;
        } catch (IOException ex) {
            throw ex;
        } catch (PythonCanceledExecutionException ex) {
            throw new IOException("Execution was interrupted.", ex);
        } finally {
            // Should not be necessary, but let's play safe here.
            conda.destroy();
        }
    }

    private Process startCondaProcess(final String... arguments) throws IOException {
        final List<String> argumentList = new ArrayList<>(1 + arguments.length);
        argumentList.add(m_executable);
        Collections.addAll(argumentList, arguments);
        final ProcessBuilder pb = new ProcessBuilder(argumentList);
        return pb.start();
    }

    /**
     * @param monitor May be {@code null}.
     * @throws PythonCanceledExecutionException If either {@code monitor} is not {@code null} and its
     *             {@link CondaExecutionMonitor#cancel()} was called or if {@code monitor} is {@code null} and the
     *             calling thread was interrupted.
     */
    private static int awaitTermination(final Process conda, final CondaExecutionMonitor monitor)
        throws IOException, PythonCanceledExecutionException {
        try {
            return monitor != null //
                ? PythonUtils.Misc.executeCancelable(conda::waitFor, Executors.newSingleThreadExecutor(),
                    new PythonCancelableFromCondaExecutionMonitor(monitor))
                : conda.waitFor();
        } catch (final PythonCanceledExecutionException ex) {
            conda.destroy();
            throw ex;
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            conda.destroy();
            throw new PythonCanceledExecutionException();
        }
    }

    private static boolean isWarning(String errorMessage) {
        errorMessage = errorMessage.trim();
        if (errorMessage.startsWith("==> WARNING: A newer version of conda exists. <==")) {
            final String[] lines = errorMessage.split("\n");
            final String lastLine = lines[lines.length - 1];
            if (lastLine.trim().startsWith("$ conda update -n base")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Describes a Conda environment.
     */
    public static final class CondaEnvironmentSpec {

        private final String m_name;

        private final String m_directoryPath;

        /**
         * Creates a new specification of a Conda environment.
         *
         * @param name The name of the Conda environment.
         * @param directoryPath The absolute path to the Conda environment's directory.
         */
        public CondaEnvironmentSpec(final String name, final String directoryPath) {
            m_name = name;
            m_directoryPath = directoryPath;
        }

        /**
         * @return The name of the Conda environment.
         */
        public String getName() {
            return m_name;
        }

        /**
         * @return The absolute path to the Conda environment's directory.
         */
        public String getDirectoryPath() {
            return m_directoryPath;
        }

        @Override
        public int hashCode() {
            return Objects.hash(m_name, m_directoryPath);
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof CondaEnvironmentSpec)) {
                return false;
            }
            final CondaEnvironmentSpec other = (CondaEnvironmentSpec)obj;
            return Objects.equals(other.m_name, m_name) //
                && Objects.equals(other.m_directoryPath, m_directoryPath);
        }
    }

    /**
     * Allows to monitor the progress of a Conda environment creation command. Conda only reports progress for package
     * downloads, at the moment.
     */
    public abstract static class CondaEnvironmentCreationMonitor extends CondaExecutionMonitor {

        /**
         * Asynchronous callback that allows to process an error output message (usually just a line at a time) of the
         * monitored Conda command.<br>
         * Exceptions thrown by this callback are discarded.
         *
         * @param error The error message, neither {@code null} nor empty.
         */
        protected abstract void handleErrorMessage(String error);

        /**
         * Asynchronous callback that allows to process progress in the download of a Python package.<br>
         * Exceptions thrown by this callback are discarded.
         *
         * @param currentPackage The package for which progress is reported.
         * @param packageFinished {@code true} if downloading the current package is finished, {@code false} otherwise.
         *            Should be accompanied by a {@code progress} value of 1.
         * @param progress The progress as a fraction in [0, 1].
         */
        protected abstract void handlePackageDownloadProgress(String currentPackage, boolean packageFinished,
            double progress);

        /**
         * Asynchronous callback that allows to process JSON output that is not interpreted by
         * {@link CondaEnvironmentCreationMonitor#handleOutputStream(InputStream)}.<br>
         * Exceptions thrown by this callback are discarded.
         *
         * @param jsonTreeNode The node that represents the root element of the read JSON output.
         */
        protected void handleOtherJsonOutput(final TreeNode jsonTreeNode) {
            // no-op by default
        }

        @Override
        protected final void handleOutputStream(final InputStream outputStream) {
            try (final JsonParser parser =
                new JsonFactory(new ObjectMapper()).createParser(new BufferedInputStream(outputStream))) {
                parseJsonOutput(parser);
            } catch (final IOException ex) {
                if (!isCanceledOrInterrupted()) {
                    throw new UncheckedIOException(ex);
                }
            }
        }

        private void parseJsonOutput(final JsonParser parser) throws IOException {
            TreeNode object;
            while (!isCanceledOrInterrupted()) {
                try {
                    object = parser.readValueAsTree();
                    if (object == null) {
                        // EOF
                        break;
                    }
                    // We only interpret package download progress reports and error messages here.
                    final TreeNode fetch = object.get("fetch");
                    if (fetch != null) {
                        final String packageNameValue = ((JsonNode)fetch).textValue().split(" ")[0];
                        final boolean finishedValue = ((JsonNode)object.get("finished")).booleanValue();
                        final double maxvalValue = ((JsonNode)object.get("maxval")).doubleValue();
                        final double progressValue = ((JsonNode)object.get("progress")).doubleValue();
                        handlePackageDownloadProgress(packageNameValue, finishedValue, progressValue / maxvalValue);
                    } else {
                        final TreeNode error = object.get("error");
                        if (error != null) {
                            String errorMessageValue;
                            final TreeNode message = object.get("message");
                            if (message != null) {
                                errorMessageValue = ((JsonNode)message).textValue();
                            } else {
                                errorMessageValue = ((JsonNode)error).textValue();
                            }
                            final TreeNode reason = object.get("reason");
                            if (reason != null && ((JsonNode)reason).textValue().equals("CONNECTION FAILED")) {
                                errorMessageValue += "\nPlease check your internet connection.";
                            }
                            handleErrorMessage(errorMessageValue);
                        } else {
                            handleOtherJsonOutput(object);
                        }
                    }
                } catch (final JsonParseException ex) {
                    // Ignore and continue; wait for proper output.
                }
            }
        }

        @Override
        protected void handleErrorStream(final InputStream errorStream) {
            try (final BufferedReader reader =
                new BufferedReader(new InputStreamReader(new BufferedInputStream(errorStream)))) {
                String line;
                while (!isCanceledOrInterrupted() && (line = reader.readLine()) != null) {
                    line = line.trim();
                    if (!line.equals("")) {
                        handleErrorMessage(line);
                    }
                }
            } catch (final IOException ex) {
                if (!isCanceledOrInterrupted()) {
                    throw new UncheckedIOException(ex);
                }
            }
        }

        private boolean isCanceledOrInterrupted() {
            return super.isCanceled() || Thread.currentThread().isInterrupted();
        }
    }

    abstract static class CondaExecutionMonitor {

        private boolean m_isCanceled;

        /**
         * Asynchronous callback that allows to process the normal output of the monitored Conda command. Should be
         * {@link Thread#interrupt() interruptible}.<br>
         * Exceptions thrown by this callback are discarded.
         *
         * @param outputStream The normal output stream of the Conda command's process.
         */
        protected abstract void handleOutputStream(InputStream outputStream);

        /**
         * Asynchronous callback that allows to process the error output of the monitored Conda command. Should be
         * {@link Thread#interrupt() interruptible}.<br>
         * Exceptions thrown by this callback are discarded.
         *
         * @param errorStream The error output stream of the Conda command's process.
         */
        protected abstract void handleErrorStream(InputStream errorStream);

        /**
         * Cancels the execution of the monitored conda command.
         */
        public synchronized void cancel() {
            m_isCanceled = true;
        }

        /**
         * @return {@code true} if the command shall be canceled, {@code false} otherwise.
         */
        private synchronized boolean isCanceled() {
            return m_isCanceled;
        }
    }

    private static final class PythonCancelableFromCondaExecutionMonitor implements PythonCancelable {

        private final CondaExecutionMonitor m_monitor;

        private PythonCancelableFromCondaExecutionMonitor(final CondaExecutionMonitor monitor) {
            m_monitor = monitor;
        }

        @Override
        public void checkCanceled() throws PythonCanceledExecutionException {
            if (m_monitor.isCanceled()) {
                throw new PythonCanceledExecutionException();
            }
        }
    }
}
