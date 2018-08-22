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
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.knime.core.node.NodeLogger;

/**
 * Static class managing testing of the python installations for python version 2 and 3.
 *
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz
 */

public class PythonKernelTester {

    private static PythonKernelTestResult m_python2TestResult = null;

    private static PythonKernelTestResult m_python3TestResult = null;

    private static List<String> m_additionalModulesPython2;

    private static List<String> m_additionalModulesPython3;

    private static NodeLogger LOGGER = NodeLogger.getLogger(PythonKernelTester.class);

    /**
     * Tests if python can be started with the currently configured command and if all required modules are installed.
     *
     * @param additionalRequiredModules additionalModules that should exist in the python installation in order for the
     *            caller to work properly - must not be null
     * @param force force the test to be issued again even if the same configuration was tested before
     * @return true if an error occured, false otherwise
     */
    public static synchronized PythonKernelTestResult
        testPython2Installation(final List<String> additionalRequiredModules, final boolean force) {
        // If python test already succeeded we do not have to run it again
        if (!force && (m_python2TestResult != null) && !m_python2TestResult.hasError()
            && additionalRequiredModules.containsAll(m_additionalModulesPython2)
            && m_additionalModulesPython2.containsAll(additionalRequiredModules)) {
            return m_python2TestResult;
        }

        m_additionalModulesPython2 = new ArrayList<String>(additionalRequiredModules);
        String arguments = "2.7.0";
        if (!additionalRequiredModules.isEmpty()) {
            arguments += " -m";
            for (String module : additionalRequiredModules) {
                arguments += " " + module;
            }
        }
        m_python2TestResult = testPythonInstallation(Activator.getPython2Command(), "PythonKernelTester.py", arguments);
        //If there is something wrong with the python installation log the testconfiguration
        if (m_python2TestResult.hasError()) {
            logDetailedInfo("Error occurred during testing Python2 installation", m_python2TestResult);
        }
        return m_python2TestResult;
    }

    /**
     * Tests if python can be started with the currently configured command and if all required modules are installed.
     *
     * @param additionalRequiredModules additionalModules that should exist in the python installation in order for the
     *            caller to work properly - must not be null
     * @param force force the test to be issued again even if the same configuration was tested before
     * @return true if an error occured, false otherwise
     */
    public static synchronized PythonKernelTestResult
        testPython3Installation(final List<String> additionalRequiredModules, final boolean force) {
        // If python test already succeeded we do not have to run it again
        if (!force && (m_python3TestResult != null) && !m_python3TestResult.hasError()
            && additionalRequiredModules.containsAll(m_additionalModulesPython3)
            && m_additionalModulesPython3.containsAll(additionalRequiredModules)) {
            return m_python3TestResult;
        }

        m_additionalModulesPython3 = new ArrayList<String>(additionalRequiredModules);
        String arguments = "3.0.0";
        if (!additionalRequiredModules.isEmpty()) {
            arguments += " -m";
            for (String module : additionalRequiredModules) {
                arguments += " " + module;
            }
        }
        m_python3TestResult = testPythonInstallation(Activator.getPython3Command(), "PythonKernelTester.py", arguments);
        //If there is something wrong with the python installation log the testconfiguration
        if (m_python3TestResult.hasError()) {
            logDetailedInfo("Error occurred during testing Python3 installation", m_python3TestResult);
        }
        return m_python3TestResult;
    }

    /**
     * Tests if python can be started with the currently configured command and if all required modules are installed.
     *
     * @return {@link PythonKernelTestResult} that contains detailed test information
     */
    private static synchronized PythonKernelTestResult testPythonInstallation(final String pythonCommand,
        final String testScript, final String arguments) {
        final StringBuffer testResultOutputBuffer = new StringBuffer();
        try {
            // Start python kernel tester script
            final String scriptPath = Activator.getFile(Activator.PLUGIN_ID, "py/" + testScript).getAbsolutePath();
            String[] args = arguments.split(" ");
            String[] pbargs = new String[args.length + 2];
            pbargs[0] = pythonCommand;
            pbargs[1] = scriptPath;
            for (int i = 0; i < args.length; i++) {
                pbargs[i + 2] = args[i];
            }
            final ProcessBuilder pb = new ProcessBuilder(pbargs);

            testResultOutputBuffer.append("Executed command: " + String.join(" ", pb.command()));

            final Process process = pb.start();
            testResultOutputBuffer.append("\nPYTHONPATH=" + pb.environment().getOrDefault("PYTHONPATH", ":") + "\n");
            testResultOutputBuffer.append("PATH=" + pb.environment().getOrDefault("PATH", ":") + "\n");
            //Get error output
            final StringWriter errorWriter = new StringWriter();
            IOUtils.copy(process.getErrorStream(), errorWriter, "UTF-8");

            String errorMessage = errorWriter.toString();
            boolean hasError = !errorMessage.isEmpty();
            if (hasError) {
                testResultOutputBuffer.append("Error during execution: " + errorMessage + "\n");
            }

            errorMessage = decorateErrorMessageForKnownProblems(errorMessage);

            // Get console output of script
            final StringWriter writer = new StringWriter();
            IOUtils.copy(process.getInputStream(), writer, "UTF-8");
            String testOutput = writer.toString();
            testResultOutputBuffer.append("Raw test output: \n" + testOutput + "\n");

            if (hasError) {
                return new PythonKernelTestResult(testResultOutputBuffer.toString(), errorMessage, null);
            } else {
                String scriptOutput = writer.toString();
                //Interpret script output -> potentially containes issues found during testing
                final String[] lines = scriptOutput.split("\\r?\\n");
                String version = null;
                errorMessage = "";
                for (final String line : lines) {
                    if (version == null) {
                        // Ignore everything before version, could be anaconda for example
                        final String trimmed = line.trim();
                        version = trimmed.matches("Python version: [0-9]+[.][0-9]+[.][0-9]+") ? trimmed : null;
                    } else {
                        //Everything that comes after the version line indicates an issue
                        errorMessage += line + "\n";
                    }
                }
                if (version == null) {
                    errorMessage += "Python installation could not be determined.";
                }

                if (!errorMessage.isEmpty()) {
                    testResultOutputBuffer.append("Error during testing Python version: " + errorMessage + ".");
                }

                // version might be null in case of error.
                return new PythonKernelTestResult(testResultOutputBuffer.toString(),
                    errorMessage.isEmpty() ? null : errorMessage, version);
            }

        } catch (final IOException e) {
            testResultOutputBuffer
                .append("Could not find python executable at the given location: " + pythonCommand + ".");
            return new PythonKernelTestResult(testResultOutputBuffer.toString(),
                "Could not find python executable at the given location: " + pythonCommand + ".", null);
        }
    }

    private static String decorateErrorMessageForKnownProblems(final String errorMessage) {
        String decoratedErrorMessage = errorMessage;

        // Check if conda's "activate" could not be found:
        String condaActivateCommand = "activate";
        // Typical error messages:
        // Windows: 'activate' is not recognized as an internal or external command, operable program or batch file.
        // Max: activate: no such file or directory
        // Linux: activate: No such file or directory
        // We just check if we can find the command name, a false positive does not really hurt.
        if (decoratedErrorMessage.contains(condaActivateCommand)) {
            String condaActivateDecoration =
                "\nPlease make sure to add the path of the directory that contains conda's '" + condaActivateCommand
                    + "' command to PATH\nas described in the Python 2 and Python 3 setup "
                    + "guide that can be found on the KNIME website.\n"
                    + "Also make sure that the path to that directory is absolute, not relative.";
            decoratedErrorMessage += condaActivateDecoration;
        }

        return decoratedErrorMessage;
    }

    /**
     * Log detailed info obtained during testing the python installation.
     *
     * @param Python3 the python major version in question (true python3, false python2)
     */
    private static synchronized void logDetailedInfo(final String prefix, final PythonKernelTestResult result) {
        LOGGER.debug(prefix);
        LOGGER.debug(result.getFullTestLog());
    }

    /**
     * Results of a python test.
     *
     * @author Patrick Winter, KNIME.com, Zurich, Switzerland
     */
    public static class PythonKernelTestResult {

        private String m_testResult;

        private String m_version;

        private String m_errorLog;

        /**
         * Creates a test result.
         *
         * @param fullTestLog full log of test with all info
         * @param errorLog only the error. null, if no error occurred
         * @param version the version. null, if version could not be detected
         */
        PythonKernelTestResult(final String fullTestLog, final String errorLog, final String version) {
            m_version = version;
            m_errorLog = errorLog;
            m_testResult = fullTestLog;
        }

        /**
         * Returns the detailed python version string (major + minor version). Returns null, if error occurred during
         * determining the version.
         *
         * @return the detailed python version string
         */
        public String getVersion() {
            return m_version;
        }

        /**
         * Returns detailed information about the result of the test.
         *
         * @return The result message containing detailed information
         */
        String getFullTestLog() {
            return m_testResult;
        }

        /**
         * Returns detailed information about the result of the test.
         *
         * @return The result message containing detailed information
         */
        public String getErrorLog() {
            return m_errorLog;
        }

        /**
         * Returns if the python installation is not capable of running the python kernel.
         *
         * @return true if the installation is not capable of running the python kernel, false otherwise
         */
        public boolean hasError() {
            return m_errorLog != null;
        }
    }
}
