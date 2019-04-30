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
 *   Jul 18, 2017 (clemens): created
 */
package org.knime.python2.kernel;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Optional;

import org.knime.python2.PythonFrameSummary;
import org.knime.python2.util.PythonUtils;

import com.google.common.base.Strings;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 */
public final class PythonIOException extends IOException implements PythonException {

    private static final long serialVersionUID = 1L;

    private static String amendMessage(final String message) {
        if (Strings.isNullOrEmpty(message)) {
            return "An exception occured while running the Python kernel. See log for details.";
        } else {
            return message;
        }
    }

    /**
     * May be {@code null}.
     */
    private final String m_shortMessage;

    /**
     * May be {@code null}.
     */
    private final String m_formattedPythonTraceback;

    /**
     * May be {@code null}.
     */
    private final PythonFrameSummary[] m_pythonTraceBack;

    /**
     * @param message a message indicating the cause of the exception
     */
    public PythonIOException(final String message) {
        super(amendMessage(message));
        m_formattedPythonTraceback = null;
        m_pythonTraceBack = null;
        m_shortMessage = null;
    }

    /**
     * @param message the message
     * @param formattedPythonTraceback The formatted string representation of the trace back of the error on Python
     *            side. Together with the frame summaries, this basically corresponds to the {@code cause} argument in
     *            {@link #PythonIOException(String, Throwable)} but for a cause on Python side.
     * @param pythonTraceback The individual frames of the trace back of the error on Python side. Together with the
     *            formatted trace back, this basically corresponds to the {@code cause} argument in
     *            {@link #PythonIOException(String, Throwable)} but for a cause on Python side.
     */
    public PythonIOException(final String message, final String formattedPythonTraceback,
        final PythonFrameSummary[] pythonTraceback) {
        super(amendMessage(message));
        m_formattedPythonTraceback = formattedPythonTraceback;
        m_pythonTraceBack = pythonTraceback;
        m_shortMessage = null;
    }

    /**
     * @param message the message
     * @param shortMessage a short high level error message
     * @param formattedPythonTraceback The formatted string representation of the trace back of the error on Python
     *            side. Together with the frame summaries, this basically corresponds to the {@code cause} argument in
     *            {@link #PythonIOException(String, Throwable)} but for a cause on Python side.
     * @param pythonTraceback The individual frames of the trace back of the error on Python side. Together with the
     *            formatted trace back, this basically corresponds to the {@code cause} argument in
     *            {@link #PythonIOException(String, Throwable)} but for a cause on Python side.
     */
    public PythonIOException(final String message, final String shortMessage, final String formattedPythonTraceback,
        final PythonFrameSummary[] pythonTraceback) {
        super(amendMessage(message));
        m_formattedPythonTraceback = formattedPythonTraceback;
        m_pythonTraceBack = pythonTraceback;
        m_shortMessage = shortMessage;
    }

    /**
     * @param cause the cause of the problem
     */
    public PythonIOException(final Throwable cause) {
        super(amendMessage(null), cause);
        m_formattedPythonTraceback = PythonUtils.Misc.extractFormattedPythonTraceback(cause).orElse(null);
        m_pythonTraceBack = PythonUtils.Misc.extractPythonTraceback(cause).orElse(null);
        m_shortMessage = PythonUtils.Misc.extractPythonShortMessage(cause).orElse(null);
    }

    /**
     * @param message a message indicating the cause of the exception
     * @param cause the cause of the problem
     */
    public PythonIOException(final String message, final Throwable cause) {
        super(amendMessage(message), cause);
        m_formattedPythonTraceback = PythonUtils.Misc.extractFormattedPythonTraceback(cause).orElse(null);
        m_pythonTraceBack = PythonUtils.Misc.extractPythonTraceback(cause).orElse(null);
        m_shortMessage = PythonUtils.Misc.extractPythonShortMessage(cause).orElse(null);
    }

    @Override
    public Optional<String> getFormattedPythonTraceback() {
        return Optional.ofNullable(m_formattedPythonTraceback);
    }

    @Override
    public Optional<PythonFrameSummary[]> getPythonTraceback() {
        return Optional.ofNullable(m_pythonTraceBack);
    }

    @Override
    public Optional<String> getShortMessage() {
        return Optional.ofNullable(m_shortMessage);
    }

    @Override
    public void printStackTrace(final PrintStream s) {
        super.printStackTrace(s);
        // Also print Python "stack trace".
        if (m_formattedPythonTraceback != null) {
            s.print("Caused by: ");
            s.println(m_formattedPythonTraceback);
        }
    }

    @Override
    public void printStackTrace(final PrintWriter s) {
        super.printStackTrace(s);
        // Also print Python "stack trace".
        if (m_formattedPythonTraceback != null) {
            s.print("Caused by: ");
            s.println(m_formattedPythonTraceback);
        }
    }
}
