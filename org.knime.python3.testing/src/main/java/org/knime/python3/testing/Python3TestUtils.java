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
 *   May 31, 2022 (marcel): created
 */
package org.knime.python3.testing;

import java.io.IOException;

import org.apache.commons.lang3.SystemUtils;
import org.knime.externalprocessprovider.ExternalProcessProvider;
import org.knime.python3.SimplePythonCommand;

/**
 * Contains utilities shared by multiple test fragments in knime-python.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class Python3TestUtils {

    private static final String PYTHON_EXE_ENV_VAR = "PYTHON3_EXEC_PATH";

    private Python3TestUtils() {
    }

    /**
     * Creates a Python command from the path specified in environment variable PYTHON3_EXEC_PATH or, if the former is
     * not set, PYTHON3_EXEC_PATH_&lt;LINUX|MAC|WINDOWS&gt;, where the latter depends on the current operating system.
     *
     * @return The command created from environment variable.
     * @throws IOException If none of the environment variables is set.
     */
    public static ExternalProcessProvider getPythonCommand() throws IOException {
        final String osSuffix;
        if (SystemUtils.IS_OS_LINUX) {
            osSuffix = "LINUX";
        } else if (SystemUtils.IS_OS_MAC) {
            osSuffix = "MAC";
        } else {
            osSuffix = "WINDOWS";
        }
        final String python3PathEnvVarForOs = PYTHON_EXE_ENV_VAR + "_" + osSuffix;

        String python3Path = System.getenv(PYTHON_EXE_ENV_VAR);
        if (python3Path == null) {
            python3Path = System.getenv(python3PathEnvVarForOs);
        }
        if (python3Path != null) {
            // TODO: We should also support paths pointing to a conda environment and accordingly create a conda-based
            // command.
            return new SimplePythonCommand(python3Path);
        }
        throw new IOException("Please set environment variable '" + PYTHON_EXE_ENV_VAR + "' or '"
            + python3PathEnvVarForOs + "' to the path of the Python 3 executable.");
    }

}
