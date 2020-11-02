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
 *   Feb 15, 2019 (marcel): created
 */
package org.knime.python2;

import org.knime.python2.kernel.PythonException;

/**
 * Describes an external Python process. The process can be started via the {@link ProcessBuilder} returned by
 * {@link #createProcessBuilder()}.
 * <P>
 * Implementation note: Implementors of this interface must override {@link #hashCode()}, {@link #equals(Object)}, and
 * {@link #toString()} in a value-based way.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public interface PythonCommand {

    /**
     * @return The (desired) version of Python environments launched by this command. Note that in general, the version
     *         returned by this method and the actual Python version of environments launched by this command can
     *         diverge since the command is usually selected by the user (via the Python preference page or flow
     *         variables). Erroneous user input may therefore lead to conflicting versions. Integrity checks on started
     *         environments should be performed to make sure that they match the version specified here.
     */
    PythonVersion getPythonVersion();

    /**
     * @return A {@link ProcessBuilder} that can be used to parameterize and start the Python process represented by
     *         this command instance.
     * @throws UnconfiguredEnvironmentException If no process can be created from this command because the underlying
     *             Python environment is not configured.
     */
    ProcessBuilder createProcessBuilder() throws UnconfiguredEnvironmentException;

    @Override
    int hashCode();

    @Override
    boolean equals(Object obj);

    @Override
    String toString();

    /**
     * Indicates that a Python environment is not yet configured and can therefore not be used to create a Python
     * process.
     */
    public final class UnconfiguredEnvironmentException extends Exception implements PythonException {

        private static final long serialVersionUID = 1L;

        /**
         * @param message A descriptive error message that instructs the user how to to configure the environment.
         */
        public UnconfiguredEnvironmentException(final String message) {
            super(message);
        }
    }
}
