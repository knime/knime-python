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
package org.knime.python2.nodes.conda;

import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

/**
 * Note that we do not test the filter lists exhaustively here but only do some spot checks on them. The reason is that
 * the filter lists are deliberately provided via JSON to avoid Java code changes when updating/expanding them. Still
 * having to update the tests each time to keep them complete would nullify this goal.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public final class PlatformCondaPackageFilterTest {

	private static final String[] ALL_PACKAGES = { //
			"appscript", // Mac only
			"asammdf", //
			"libxcb", // Linux only
			"mkl_fft", //
			"numpy", //
			"pandas", //
			"pyreadline", // Windows only
			"readline", // Linux & Mac only
			"tensorflow", //
			"xlwings" // Mac & Windows only
	};

	@Test
	public void testLinuxFilter() throws IOException {
		final String[] expectedFilteredPackages = { //
				"asammdf", "libxcb", "numpy", "pandas", "readline", "tensorflow" };
		testFilter(expectedFilteredPackages, PlatformCondaPackageFilter.createLinuxFilterList());
	}

	@Test
	public void testMacFilter() throws IOException {
		final String[] expectedFilteredPackages = { //
				"appscript", "asammdf", "numpy", "pandas", "readline", "tensorflow", "xlwings" };
		testFilter(expectedFilteredPackages, PlatformCondaPackageFilter.createMacFilterList());
	}

	@Test
	public void testWindowsFilter() throws IOException {
		final String[] expectedFilteredPackages = { //
				"asammdf", "numpy", "pandas", "pyreadline", "tensorflow", "xlwings" };
		testFilter(expectedFilteredPackages, PlatformCondaPackageFilter.createWindowsFilterList());
	}

	private static void testFilter(final String[] expectedFilteredPackages, final PlatformCondaPackageFilter filter) {
		final String[] actualFilteredPackages = Arrays.stream(ALL_PACKAGES) //
				.filter(pkg -> !filter.excludesPackage(pkg)) //
				.toArray(String[]::new);
		assertArrayEquals(expectedFilteredPackages, actualFilteredPackages);
	}
}
