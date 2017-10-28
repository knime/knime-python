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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.core.runtime.Platform;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.FileUtil;
import org.knime.python2.extensions.serializationlibrary.SerializationLibraryExtensions;
import org.knime.python2.generic.templates.SourceCodeTemplatesExtensions;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

/**
 * Activator for this plugin.
 *
 * @author Patrick Winter, KNIME.com, Zurich, Switzerland
 */
public class Activator implements BundleActivator {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(Activator.class);

    private static PythonKernelTestResult python2TestResult;

    private static PythonKernelTestResult python3TestResult;

    private static List<String> additionalModulesPython2;

    private static List<String> additionalModulesPython3;

    /**
     * The id of the plugin activated by this class.
     */
    public static final String PLUGIN_ID = "org.knime.python2";

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(final BundleContext bundleContext) throws Exception {
        // When this plugin is loaded test the python installation
        new Thread(new Runnable() {
            @Override
            public void run() {
                handleTestResult(testPython2Installation(Collections.emptyList()));
                handleTestResult(testPython3Installation(Collections.emptyList()));
            }

            private void handleTestResult(final PythonKernelTestResult testResult) {
                if (testResult.hasError()) {
                    //LOGGER.error(testResult.getMessage());
                }
            }
        }).start();
        SerializationLibraryExtensions.init();
        SourceCodeTemplatesExtensions.init();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop(final BundleContext bundleContext) throws Exception {
    }

    /**
     * Return the command to start python 2.
     *
     * @return The command to start python 2
     */
    public static String getPython2Command() {
        return PythonPreferencePage.getPython2Path();
    }

    /**
     * Return the command to start python 3.
     *
     * @return The command to start python 3
     */
    public static String getPython3Command() {
        return PythonPreferencePage.getPython3Path();
    }

    /**
     * Tests if python can be started with the currently configured command and if all required modules are installed.
     *
     * @return {@link PythonKernelTestResult} that containes detailed test information
     */
    private static synchronized PythonKernelTestResult testPythonInstallation(final String pythonCommand,
        final String testScript, final String arguments) {
        final StringBuffer infoBuffer = new StringBuffer();
        try {
            // Start python kernel tester script
            final String scriptPath = getFile(Activator.PLUGIN_ID, "py/" + testScript).getAbsolutePath();
            String[] args = arguments.split(" ");
            String[] pbargs = new String[args.length + 2];
            pbargs[0] = pythonCommand;
            pbargs[1] = scriptPath;
            for (int i = 0; i < args.length; i++) {
                pbargs[i + 2] = args[i];
            }
            final ProcessBuilder pb = new ProcessBuilder(pbargs);

            infoBuffer.append("Executed command: " + String.join(" ", pb.command()) + "\n");
            final Process process = pb.start();
            infoBuffer.append("PYTHONPATH=" + pb.environment().getOrDefault("PYTHONPATH", ":") + "\n");
            infoBuffer.append("PATH=" + pb.environment().getOrDefault("PATH", ":") + "\n");
            //Get error output
            final StringWriter errorWriter = new StringWriter();
            IOUtils.copy(process.getErrorStream(), errorWriter, "UTF-8");

            String str = errorWriter.toString();
            if (!str.isEmpty()) {
                infoBuffer.append("Error during execution: " + str + "\n");
            }

            // Get console output of script
            final StringWriter writer = new StringWriter();
            IOUtils.copy(process.getInputStream(), writer, "UTF-8");
            str = writer.toString();
            infoBuffer.append("Raw test output: \n" + str + "\n");
            // Create test result with console output as message and error code
            // != 0 as error
            return new PythonKernelTestResult(writer.toString(), infoBuffer.toString(), Optional.empty());
        } catch (final IOException e) {
            //Error should be processed by calling method using PythonKernelTestResult
            //LOGGER.error(e.getMessage(), e);
            // Python could not be started
            return new PythonKernelTestResult("", infoBuffer.toString(),
                Optional.of("Could not find python executable at the given location."));
        }
    }

    /**
     * Tests if python can be started with the currently configured command and if all required modules are installed.
     * @param additionalRequiredModules additionalModules that should exist in the python installation in order
     *                                  for the caller to work properly - must not be null
     *
     * @return {@link PythonKernelTestResult} that containes detailed test information
     */
    public static synchronized PythonKernelTestResult testPython2Installation(final List<String> additionalRequiredModules) {
        // If python test already succeeded we do not have to run it again
        if ((python2TestResult != null) && !python2TestResult.hasError()
                && additionalRequiredModules.containsAll(additionalModulesPython2)
                && additionalModulesPython2.containsAll(additionalRequiredModules)) {
            return python2TestResult;
        }
        additionalModulesPython2 = new ArrayList<String>(additionalRequiredModules);
        String arguments = "2.7.0";
        if (!additionalRequiredModules.isEmpty()) {
            arguments += " -m";
            for (String module : additionalRequiredModules) {
                arguments += " " + module;
            }
        }
        python2TestResult = testPythonInstallation(getPython2Command(), "PythonKernelTester.py", arguments);
        return python2TestResult;
    }

    /**
     * Delete the previous python test result and retest the python behind the new path.
     * @param additionalRequiredModules additionalModules that should exist in the python installation in order
     *                                  for the caller to work properly - must not be null
     * @return The new test result
     */
    public static synchronized PythonKernelTestResult retestPython2Installation(final List<String> additionalRequiredModules) {
        python2TestResult = null;
        return testPython2Installation(additionalRequiredModules);
    }

    /**
     * Tests if python can be started with the currently configured command and if all required modules are installed.
     * @param additionalRequiredModules additionalModules that should exist in the python installation in order
     *                                  for the caller to work properly - must not be null
     *
     * @return {@link PythonKernelTestResult} that containes detailed test information
     */
    public static synchronized PythonKernelTestResult testPython3Installation(final List<String> additionalRequiredModules) {
        // If python test already succeeded we do not have to run it again
        if ((python3TestResult != null) && !python3TestResult.hasError()
                && additionalRequiredModules.containsAll(additionalModulesPython3)
                && additionalModulesPython3.containsAll(additionalRequiredModules)) {
            return python3TestResult;
        }
        additionalModulesPython3 = new ArrayList<String>(additionalRequiredModules);
        String arguments = "3.1.0";
        if (!additionalRequiredModules.isEmpty()) {
            arguments += " -m";
            for (String module : additionalRequiredModules) {
                arguments += " " + module;
            }
        }
        python3TestResult = testPythonInstallation(getPython3Command(), "PythonKernelTester.py", arguments);
        return python3TestResult;
    }

    /**
     * Delete the previous python test result and retest the python behind the new path.
     * @param additionalRequiredModules additionalModules that should exist in the python installation in order
     *                                  for the caller to work properly - must not be null
     *
     * @return The new test result
     */
    public static synchronized PythonKernelTestResult retestPython3Installation(final List<String> additionalRequiredModules) {
        python3TestResult = null;
        return testPython3Installation(additionalRequiredModules);
    }

    /**
     * Returns the file contained in the plugin with the given ID.
     *
     * @param symbolicName ID of the plugin containing the file
     * @param relativePath File path inside the plugin
     * @return The file
     */
    public static File getFile(final String symbolicName, final String relativePath) {
        try {
            final Bundle bundle = Platform.getBundle(symbolicName);
            final URL url = FileLocator.find(bundle, new Path(relativePath), null);
            return url != null ? FileUtil.getFileFromURL(FileLocator.toFileURL(url)) : null;
        } catch (final Exception e) {
            LOGGER.debug(e.getMessage(), e);
            return null;
        }
    }
}
