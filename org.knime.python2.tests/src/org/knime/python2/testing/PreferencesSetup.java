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
 */
package org.knime.python2.testing;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.commons.lang3.SystemUtils;
import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.core.runtime.Platform;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.knime.core.util.FileUtil;
import org.osgi.framework.Bundle;

/**
 * This rule can be used to configure tests that require the preferences of the Python integration to be properly set
 * up.
 *
 * @noreference Note that, at the moment, the way the preferences are configured is tailored to KNIME's internal build
 *              pipeline. That is, tests that make use of this rule are in general not portable/cannot be run locally.
 *              Therefore, third-party code should not make use of this class.
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class PreferencesSetup implements TestRule {

	private static final String WORKFLOW_TESTS_CONFIG_DIR = "workflow-tests";

	private static final String PREFS_FILE_WINDOWS = "preferences-Windows.epf";

	private static final String PREFS_FILE_LINUX_MAC = "preferences-Linux.epf";

	private final String m_testBundleName;

	/**
	 * @param testBundleName The symbolic name of the {@link Bundle test bundle} for which this rule is applied.
	 *            Different Python-based test bundles may require different preferences. We import those preferences
	 *            that match the installed bundle of the given name.
	 */
	public PreferencesSetup(final String testBundleName) {
		m_testBundleName = testBundleName;
	}

	@Override
	public Statement apply(final Statement base, final Description description) {
		return new Statement() {

			@Override
			public void evaluate() throws Throwable {
				final File preferencesFile = resolvePreferencesFile(m_testBundleName);
				try (final InputStream preferencesFileStream = new FileInputStream(preferencesFile)) {
					Platform.getPreferencesService().importPreferences(preferencesFileStream);
				}
				base.evaluate();
			}
		};
	}

	/**
	 * Resolving is done relative to the bundle of the given name. The preferences file is assumed to reside in a folder
	 * named {@value #WORKFLOW_TESTS_CONFIG_DIR} at the root of the bundle's enclosing source code repository. The
	 * preferences file is assumed to be named either {@value #PREFS_FILE_WINDOWS} or {@value #PREFS_FILE_LINUX_MAC}. On
	 * Windows, the former is resolved, on Linux or macOS, the latter is resolved.
	 */
	private static File resolvePreferencesFile(final String testBundleName) throws IOException {
		final Bundle testBundle = Platform.getBundle(testBundleName);
		final URL bundleRootInternalUrl = FileLocator.find(testBundle, Path.EMPTY, null);
		final URL bundleRootUrl = FileLocator.toFileURL(bundleRootInternalUrl);
		final File bundleRoot = FileUtil.getFileFromURL(bundleRootUrl);

		final File repositoryRoot = extractRepositoryRootDirectory(bundleRoot, testBundleName);
		final File workflowTestsConfigDirectory = new File(repositoryRoot, WORKFLOW_TESTS_CONFIG_DIR);
		final String preferencesFileName = SystemUtils.IS_OS_WINDOWS //
				? PREFS_FILE_WINDOWS //
				: PREFS_FILE_LINUX_MAC;
		return new File(workflowTestsConfigDirectory, preferencesFileName);
	}

	/**
	 * The path to the bundle root looks like this on a Linux build machine:
	 * <P>
	 * {@literal /home/jenkins/workspace/<repository-root>/<test-bundle-name>/target/work/plugins/<plugin-name>}
	 * <P>
	 * We want to extract the path to {@literal <repository-root>}.
	 */
	private static File extractRepositoryRootDirectory(final File bundleRoot, final String testBundleName) {
		File repositoryRoot = bundleRoot;
		while (!repositoryRoot.getName().equals(testBundleName)) {
			repositoryRoot = repositoryRoot.getParentFile();
		}
		repositoryRoot = repositoryRoot.getParentFile();
		return repositoryRoot;
	}
}
