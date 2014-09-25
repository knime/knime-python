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

/**
 * Results of a python test.
 * 
 * @author Patrick Winter, KNIME.com, Zurich, Switzerland
 */
public class PythonKernelTestResult {

	private String m_version;
	private String m_message;
	private boolean m_error;

	/**
	 * Creates a test result.
	 * 
	 * @param message
	 *            The result message containing detailed information
	 * @param error
	 *            true if the installation is not capable of running the python
	 *            kernel, false otherwise
	 */
	PythonKernelTestResult(final String message, final boolean error) {
		m_error = error;
		int endOfLineOne = message.indexOf(System.lineSeparator());
		if (endOfLineOne < 0) {
			endOfLineOne = message.length();
			m_message = message.trim();
		} else {
			m_message = message.substring(endOfLineOne, message.length()).trim();
		}
		String firstLine = message.substring(0, endOfLineOne);
		m_version = firstLine.matches("Python version: [0-9]+[.][0-9]+[.][0-9]+") ? firstLine : null;
		if (m_version == null) {
			m_error = true;
			m_message = "Could not detect python version";
		}
	}

	/**
	 * Returns the python version.
	 * 
	 * @return The version of the python installation
	 */
	public String getVersion() {
		return m_version;
	}

	/**
	 * Returns detailed information about the result of the test.
	 * 
	 * @return The result message containing detailed information
	 */
	public String getMessage() {
		return m_message;
	}

	/**
	 * Returns if the python installation is not capable of running the python
	 * kernel.
	 * 
	 * @return true if the installation is not capable of running the python
	 *         kernel, false otherwise
	 */
	public boolean hasError() {
		return m_error;
	}
}
