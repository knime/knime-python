/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
package org.knime.python;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.core.runtime.Platform;
import org.knime.code.generic.templates.SourceCodeTemplatesExtensions;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.FileUtil;
import org.knime.python.kernel.PythonModuleExtensions;
import org.knime.python.typeextension.KnimeToPythonExtensions;
import org.knime.python.typeextension.PythonToKnimeExtensions;
import org.knime.python2.PythonPreferencePage;
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

	private static PythonKernelTestResult pythonTestResult;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void start(BundleContext bundleContext) throws Exception {
		// When this plugin is loaded test the python installation
		new Thread(new Runnable() {
			@Override
			public void run() {
				testPythonInstallation();
			}
		}).start();
		KnimeToPythonExtensions.init();
		PythonToKnimeExtensions.init();
		SourceCodeTemplatesExtensions.init();
		PythonModuleExtensions.init();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void stop(BundleContext bundleContext) throws Exception {
	}

	/**
	 * Return the command to start python.
	 * 
	 * @return The command to start python
	 */
	public static String getPythonCommand() {
		return PythonPreferencePage.getPythonPath();
	}

	/**
	 * Tests if python can be started with the currently configured command and
	 * if all required modules are installed.
	 * 
	 * @return {@link PythonKernelTestResult} that containes detailed test
	 *         information
	 */
	public static synchronized PythonKernelTestResult testPythonInstallation() {
		// If python test already succeeded we do not have to run it again
		if (pythonTestResult != null && !pythonTestResult.hasError()) {
			return pythonTestResult;
		}
		String pythonCommand = getPythonCommand();
		try {
			// Start python kernel tester script
			String scriptPath = getFile("org.knime.python", "py/PythonKernelTester.py").getAbsolutePath();
			ProcessBuilder pb = new ProcessBuilder(pythonCommand, scriptPath);
			Process process = pb.start();
			// Get console output of script
			StringWriter writer = new StringWriter();
			IOUtils.copy(process.getInputStream(), writer);
			// Create test result with console output as message and error code
			// != 0 as error
			pythonTestResult = new PythonKernelTestResult(writer.toString());
			return pythonTestResult;
		} catch (IOException e) {
			LOGGER.error(e.getMessage(), e);
			// Python could not be started
			return new PythonKernelTestResult("Could not start python with command '" + pythonCommand + "'");
		}
	}

	/**
	 * Delete the previous python test result and retest the python behind the
	 * new path.
	 * 
	 * @return The new test result
	 */
	public static synchronized PythonKernelTestResult retestPythonInstallation() {
		pythonTestResult = null;
		return testPythonInstallation();
	}

	/**
	 * Returns the file contained in the plugin with the given ID.
	 * 
	 * @param symbolicName
	 *            ID of the plugin containing the file
	 * @param relativePath
	 *            File path inside the plugin
	 * @return The file
	 */
	public static File getFile(final String symbolicName, final String relativePath) {
		try {
			Bundle bundle = Platform.getBundle(symbolicName);
			URL url = FileLocator.find(bundle, new Path(relativePath), null);
			return url != null ? FileUtil.getFileFromURL(FileLocator.toFileURL(url)) : null;
		} catch (Exception e) {
			LOGGER.debug(e.getMessage(), e);
			return null;
		}
	}

}
