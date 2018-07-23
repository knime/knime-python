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
 *   Jul 23, 2018 (marcel): created
 */
package org.knime.python2.util;

import org.knime.core.node.NodeLogger;

/**
 * A logger whose debug log level can be enabled/disabled via a {@link #DEBUG_ENABLED global flag}. Method calls
 * delegate to {@link NodeLogger}.<br>
 * This class was introduced because (debug) logging in {@code org.knime.python2} is pretty scattered and we don't want
 * all those debug entries pollute our test logs but still need them for actual debugging.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class PythonNodeLogger {

    /**
     * Determines if the debug log level is enabled. This should only be set to {@code true} during actual debugging.
     * Note that logging is also governed by {@link NodeLogger#isDebugEnabled()}.
     */
    public static final boolean DEBUG_ENABLED = false;

    /**
     * @see PythonNodeLogger#getLogger(Class)
     */
    public static PythonNodeLogger getLogger(final Class<?> c) {
        return new PythonNodeLogger(NodeLogger.getLogger(c));
    }

    /**
     * @see PythonNodeLogger#getLogger(String)
     */
    public static PythonNodeLogger getLogger(final String s) {
        return new PythonNodeLogger(NodeLogger.getLogger(s));
    }

    private final NodeLogger m_logger;

    /**
     * Creates a new {@link PythonNodeLogger} that wraps the given {@link NodeLogger}.
     */
    public PythonNodeLogger(final NodeLogger logger) {
        m_logger = logger;
    }

    public void debug(final Object o) {
        if (DEBUG_ENABLED) {
            m_logger.debug(o);
        }
    }

    public void info(final Object o) {
        m_logger.info(o);
    }

    public void warn(final Object o) {
        m_logger.warn(o);
    }

    public void error(final Object o) {
        m_logger.error(o);
    }

    public void fatal(final Object o) {
        m_logger.fatal(o);
    }

    public void debug(final Object o, final Throwable t) {
        if (DEBUG_ENABLED) {
            m_logger.debug(o, t);
        }
    }

    public void info(final Object o, final Throwable t) {
        m_logger.info(o, t);
    }

    public void warn(final Object o, final Throwable t) {
        m_logger.warn(o, t);
    }

    public void error(final Object o, final Throwable t) {
        m_logger.error(o, t);
    }

    public void fatal(final Object o, final Throwable t) {
        m_logger.fatal(o, t);
    }
}
