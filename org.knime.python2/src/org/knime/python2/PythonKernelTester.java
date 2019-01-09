/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
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
package org.knime.python2;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.Pair;

import com.google.common.collect.Collections2;

/**
 * Static class to manage the testing of Python installations for Python versions 2 and 3.
 *
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz
 * @author Christian Dietz, KNIME GmbH, Konstanz
 */
public class PythonKernelTester {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonKernelTester.class);

    private static final String PYTHON_KERNEL_TESTER_FILE_NAME = "PythonKernelTester.py";

    private static final String PYTHON_MAJOR_VERSION_2 = "2";

    private static final String PYTHON_MINIMUM_VERSION_2 = "2.7.0";

    private static final String PYTHON_MAJOR_VERSION_3 = "3";

    /**
     * Null. No minimum version required at the moment.
     */
    private static final String PYTHON_MINIMUM_VERSION_3 = null;

    /**
     * Caches previous test results. Mapping from the Python command that was tested to a pair of the additional
     * required modules for which were tested and the test results.
     */
    private static final Map<String, Pair<List<PythonModuleSpec>, PythonKernelTestResult>> TEST_RESULTS =
        new ConcurrentHashMap<>();

    /**
     * Tests if Python can be started for the given Python 2 command and if all given required custom modules are
     * installed.
     *
     * @param python2Command The Python 2 command to test.
     * @param additionalRequiredModules Additional custom modules that must exist in the Python installation in order
     *            for the caller to work properly, must not be {@code null} but may be empty.
     * @param force Force the test to be rerun again even if the same configuration was successfully tested before.
     * @return The results of the installation test.
     */
    public static PythonKernelTestResult testPython2Installation(final String python2Command,
        final Collection<PythonModuleSpec> additionalRequiredModules, final boolean force) {
        return testPythonInstallation(python2Command, PYTHON_MAJOR_VERSION_2, PYTHON_MINIMUM_VERSION_2,
            additionalRequiredModules, force);
    }

    /**
     * Tests if Python can be started for the given Python 3 command and if all given required custom modules are
     * installed.
     *
     * @param python3Command The Python 3 command to test.
     * @param additionalRequiredModules Additional custom modules that must exist in the Python installation in order
     *            for the caller to work properly, must not be {@code null} but may be empty.
     * @param force Force the test to be rerun again even if the same configuration was successfully tested before.
     * @return The results of the installation test.
     */
    public static PythonKernelTestResult testPython3Installation(final String python3Command,
        final Collection<PythonModuleSpec> additionalRequiredModules, final boolean force) {
        return testPythonInstallation(python3Command, PYTHON_MAJOR_VERSION_3, PYTHON_MINIMUM_VERSION_3,
            additionalRequiredModules, force);
    }

    /**
     * @param minimumVersion May be {@code null} in the case where no minimum version is required.
     */
    private static synchronized PythonKernelTestResult testPythonInstallation(final String pythonCommand,
        final String majorVersion, final String minimumVersion,
        final Collection<PythonModuleSpec> additionalRequiredModules, final boolean force) {
        // Only rerun test if there isn't already a suitable test result.
        PythonKernelTestResult testResults =
            getPreviousTestResultsIfApplicable(pythonCommand, additionalRequiredModules, force);
        if (testResults != null) {
            return testResults;
        }

        final StringBuilder testLogger = new StringBuilder();
        try {
            final Process process = runPythonKernelTester(pythonCommand, majorVersion, minimumVersion,
                additionalRequiredModules, testLogger);

            // Get error output.
            final StringWriter errorWriter = new StringWriter();
            IOUtils.copy(process.getErrorStream(), errorWriter, "UTF-8");
            String errorOutput = errorWriter.toString();
            if (!errorOutput.isEmpty()) {
                testLogger.append("Error during execution: " + errorOutput + "\n");
                errorOutput = decorateErrorOutputForKnownProblems(errorOutput);
            }

            // Get regular output.
            final StringWriter outputWriter = new StringWriter();
            IOUtils.copy(process.getInputStream(), outputWriter, "UTF-8");
            final String testOutput = outputWriter.toString();
            testLogger.append("Raw test output: \n" + testOutput + "\n");

            testResults = createTestReport(errorOutput, testOutput, testLogger);
        } catch (final IOException e) {
            final String message = "Could not find Python executable at the given location: " + pythonCommand + ".";
            testLogger.append(message);
            testResults = new PythonKernelTestResult(testLogger.toString(), message, null);
        }

        // If there is something wrong with the Python installation, log the test configuration.
        if (testResults.hasError()) {
            logDetailedInfo("An error occurred while testing the Python " + majorVersion + " installation.",
                testResults);
        }
        TEST_RESULTS.put(pythonCommand, new Pair<>(new ArrayList<>(additionalRequiredModules), testResults));
        return testResults;
    }

    private static PythonKernelTestResult getPreviousTestResultsIfApplicable(final String pythonCommand,
        final Collection<PythonModuleSpec> additionalRequiredModules, final boolean force) {
        // If a previous, appropriate Python test already succeeded, we will not have to run it again and return the
        // old results here (except if we're forced to).
        if (!force) {
            final Pair<List<PythonModuleSpec>, PythonKernelTestResult> requiredModulesAndResult =
                TEST_RESULTS.get(pythonCommand);
            if (requiredModulesAndResult != null) {
                final List<PythonModuleSpec> previouslyRequiredModules = requiredModulesAndResult.getFirst();
                final PythonKernelTestResult previousTestResults = requiredModulesAndResult.getSecond();
                if (!previousTestResults.hasError() //
                    && additionalRequiredModules.containsAll(previouslyRequiredModules)
                    && previouslyRequiredModules.containsAll(additionalRequiredModules)) {
                    return previousTestResults;
                }
            }
        }
        return null;
    }

    private static Process runPythonKernelTester(final String pythonCommand, final String majorVersion,
        final String minimumVersion, final Collection<PythonModuleSpec> additionalRequiredModules,
        final StringBuilder testLogger) throws IOException {
        // Run Python kernel tester script. See file at pythonKernelTesterFilePath for expected arguments.
        final String pythonKernelTesterFilePath =
            Activator.getFile(Activator.PLUGIN_ID, "py/" + PYTHON_KERNEL_TESTER_FILE_NAME).getAbsolutePath();
        final List<String> command = new ArrayList<>(1 + 1 + 1 + (minimumVersion == null ? 0 : 1)
            + (additionalRequiredModules.isEmpty() ? 0 : 1) + additionalRequiredModules.size());
        command.add(pythonCommand);
        command.add(pythonKernelTesterFilePath);
        command.add(majorVersion);
        if (minimumVersion != null) {
            command.add(minimumVersion);
        }
        if (!additionalRequiredModules.isEmpty()) {
            command.add("-m"); // Flag for additional modules.
            command.addAll(Collections2.transform(additionalRequiredModules, PythonModuleSpec::toString));
        }
        final ProcessBuilder pb = new ProcessBuilder();
        pb.command(command);
        testLogger.append("Executed command: " + String.join(" ", pb.command()));
        final Process process = pb.start();
        testLogger.append("\nPYTHONPATH=" + pb.environment().getOrDefault("PYTHONPATH", ":"));
        testLogger.append("\nPATH=" + pb.environment().getOrDefault("PATH", ":") + "\n");
        return process;
    }

    private static String decorateErrorOutputForKnownProblems(final String errorMessage) {
        String decoratedErrorMessage = errorMessage;

        // Check if conda's "activate" could not be found:
        final String condaActivateCommand = "activate";
        // Typical error messages:
        // Windows: 'activate' is not recognized as an internal or external command, operable program or batch file.
        // Max: activate: no such file or directory
        // Linux: activate: No such file or directory
        // We just check if we can find the command name, a false positive does not really hurt.
        if (decoratedErrorMessage.contains(condaActivateCommand)) {
            final String condaActivateDecoration =
                "\nPlease make sure to add the path of the directory that contains conda's '" + condaActivateCommand
                    + "' command to PATH\nas described in the Python 2 and Python 3 setup "
                    + "guide that can be found on the KNIME website.\n"
                    + "Also make sure that the path to that directory is absolute, not relative.";
            decoratedErrorMessage += condaActivateDecoration;
        }

        return decoratedErrorMessage;
    }

    private static PythonKernelTestResult createTestReport(String errorOutput, final String testOutput,
        final StringBuilder testLogger) {
        PythonKernelTestResult testResults;
        if (!errorOutput.isEmpty()) {
            testResults = new PythonKernelTestResult(testLogger.toString(), errorOutput, null);
        } else {
            // Interpret test output, potentially contains issues found during testing.
            final String[] lines = testOutput.split("\\r?\\n");
            String version = null;
            errorOutput = "";
            for (final String line : lines) {
                if (version == null) {
                    // Ignore everything before version, could be conda stuff for example.
                    final String trimmed = line.trim();
                    version = trimmed.matches("Python version: [0-9]+[.][0-9]+[.][0-9]+") ? trimmed : null;
                } else {
                    // Everything that comes after the version line indicates an issue.
                    errorOutput += line + "\n";
                }
            }
            if (version == null) {
                errorOutput += "Python installation could not be determined.";
            }
            if (!errorOutput.isEmpty()) {
                testLogger.append("Error during testing Python version: " + errorOutput + ".");
            }
            // Version might be null in case of error.
            testResults =
                new PythonKernelTestResult(testLogger.toString(), errorOutput.isEmpty() ? null : errorOutput, version);
        }
        return testResults;
    }

    /**
     * Log detailed info obtained during testing the Python installation.
     */
    private static void logDetailedInfo(final String prefix, final PythonKernelTestResult result) {
        LOGGER.debug(prefix);
        LOGGER.debug(result.getFullTestLog());
    }

    /**
     * Results of a Python test.
     */
    public static class PythonKernelTestResult {

        private final String m_testResult;

        private final String m_version;

        private final String m_errorLog;

        /**
         * Creates a test result.
         *
         * @param fullTestLog Full log of test with all info.
         * @param errorLog Only the error. null, if no error occurred.
         * @param version The version. null, if version could not be detected.
         */
        PythonKernelTestResult(final String fullTestLog, final String errorLog, final String version) {
            m_version = version;
            m_errorLog = errorLog;
            m_testResult = fullTestLog;
        }

        /**
         * Returns the detailed Python version string (major + minor version). Returns null, if error occurred during
         * determining the version.
         *
         * @return The detailed Python version string.
         */
        public String getVersion() {
            return m_version;
        }

        /**
         * Returns detailed information about the result of the test.
         *
         * @return The result message containing detailed information.
         */
        String getFullTestLog() {
            return m_testResult;
        }

        /**
         * Returns detailed information about the result of the test.
         *
         * @return The result message containing detailed information.
         */
        public String getErrorLog() {
            return m_errorLog;
        }

        /**
         * Returns whether the Python installation is not capable of running the Python kernel.
         *
         * @return true If the installation is not capable of running the Python kernel, false otherwise.
         */
        public boolean hasError() {
            return m_errorLog != null;
        }
    }
}
