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
 *   May 18, 2022 (marcel): created
 */
package org.knime.python3;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

/**
 * Gateway to a Python process. Starts a Python process upon construction of an instance and destroys it when
 * {@link #close() closing} the instance. Python functionality can be accessed via a {@link #getEntryPoint() proxy}.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @param <T> the class of the proxy
 */
public interface PythonGateway<T extends PythonEntryPoint> extends Closeable {

    /**
     * @return The entry point into Python. Calling methods on this object will call Python functions.
     */
    T getEntryPoint();

    /**
     * The standard output stream of the Python process.
     * Must properly implement {@link InputStream#available()} i.e. return a value > 0 if there is input available.
     *
     * @return The Python process's {@code stdout}.
     */
    InputStream getStandardOutputStream();

    /**
     * The standard error stream of the Python process.
     * Must properly implement {@link InputStream#available()} i.e. return a value > 0 if there is input available.
     *
     * @return The Python process's {@code stderr}.
     */
    InputStream getStandardErrorStream();

    /**
     * @return If the process was terminated abnormally, returns a user friendly string explaining why that happened.
     *         null otherwise.
     */
    String getTerminationReason();

    @Override
    void close() throws IOException;
}
