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

import java.util.Optional;

/**
 * Results of a python test.
 *
 * @author Patrick Winter, KNIME.com, Zurich, Switzerland
 */
public class PythonKernelTestResult {

    private final String m_version;

    private String m_message;

    /**
     * Creates a test result.
     *
     * @param scriptOutput The result message containing detailed information
     * @param config of python environment
     * @param potential errorMessage in case of failure
     */
    PythonKernelTestResult(final String scriptOutput, final String config, final Optional<String> errorMessage) {
        if (errorMessage.isPresent()) {
            m_message = errorMessage.get() + "\n";
            m_message += config;
            m_version = null;
        } else {
            final String[] lines = scriptOutput.split("\\r?\\n");
            String version = null;
            m_message = "";
            for (final String line : lines) {
                if (version != null) {
                    m_message += line;
                } else {
                    // Ignore everything before version, could be anaconda for example
                    final String trimmed = line.trim();
                    version = trimmed.matches("Python version: [0-9]+[.][0-9]+[.][0-9]+") ? trimmed : null;
                }
            }
            m_version = version;
            if (m_version == null) {
                m_message = "Python installation could not be determined with the following settings: \n" + config;
            }
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
     * Returns if the python installation is not capable of running the python kernel.
     *
     * @return true if the installation is not capable of running the python kernel, false otherwise
     */
    public boolean hasError() {
        return m_message != null && !m_message.isEmpty();
    }
}
