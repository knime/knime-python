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
 *   Jul 28, 2021 (marcel): created
 */
package org.knime.python2.kernel;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.NodeContext;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class PythonOutputListeners implements AutoCloseable {

    // Do not change. Used on Python side.
    private static final String WARNING_MESSAGE_PREFIX = "[WARN]";

    private final List<PythonOutputListener> m_stdoutListeners = Collections.synchronizedList(new ArrayList<>());

    private final List<PythonOutputListener> m_stderrListeners = Collections.synchronizedList(new ArrayList<>());

    private final Thread m_stdoutDistributor;

    private final Thread m_stderrDistributor;

    /**
     * @param stdoutStream The Python process's standard output stream.
     * @param stderrStream The Python process's standard error stream.
     * @param nodeContextManager Can be used to associate outputs of the Python process with a specific KNIME node. That
     *            is, calling {@link NodeContext#getContext()} in
     *            {@link PythonOutputListener#messageReceived(String, boolean)} of the registered listeners will return
     *            the node context managed by the argument.
     */
    public PythonOutputListeners(final InputStream stdoutStream, final InputStream stderrStream,
        final NodeContextManager nodeContextManager) {
        m_stdoutDistributor =
            new Thread(new PythonOutputDistributor(stdoutStream, m_stdoutListeners, nodeContextManager));
        m_stderrDistributor =
            new Thread(new PythonOutputDistributor(stderrStream, m_stderrListeners, nodeContextManager));
    }

    /**
     * Starts listening to the streams provided upon construction and distributing their outputs to the registered
     * listeners.
     */
    public void startListening() {
        m_stdoutDistributor.start();
        m_stderrDistributor.start();
    }

    /**
     * @param listener The listener intended for monitoring standard output to add to this collection.
     */
    public void addStdoutListener(final PythonOutputListener listener) {
        m_stdoutListeners.add(listener);
    }

    /**
     * @param listener The listener intended for monitoring standard error to add to this collection.
     */
    public void addStderrorListener(final PythonOutputListener listener) {
        m_stderrListeners.add(listener);
    }

    /**
     * @param listener The listener monitoring standard output to remove from this collection.
     */
    public void removeStdoutListener(final PythonOutputListener listener) {
        m_stdoutListeners.remove(listener);
    }

    /**
     * @param listener The listener monitoring standard error to remove from this collection.
     */
    public void removeStderrorListener(final PythonOutputListener listener) {
        m_stderrListeners.remove(listener);
    }

    @Override
    public void close() throws Exception {
        synchronized (m_stderrListeners) {
            synchronized (m_stdoutListeners) {
                m_stderrListeners.forEach(l -> l.setDisabled(true));
                m_stdoutListeners.forEach(l -> l.setDisabled(true));
                m_stderrDistributor.interrupt();
                m_stdoutDistributor.interrupt();
                m_stderrListeners.clear();
                m_stdoutListeners.clear();
            }
        }
    }

    private static final class PythonOutputDistributor implements Runnable {

        private final InputStream m_stream;

        private final List<PythonOutputListener> m_listeners;

        private final NodeContextManager m_nodeContextManager;

        public PythonOutputDistributor(final InputStream stream, final List<PythonOutputListener> listeners,
            final NodeContextManager nodeContextManager) {
            m_stream = stream;
            m_listeners = listeners;
            m_nodeContextManager = nodeContextManager;
        }

        @Override
        public void run() {
            final BufferedReader reader = new BufferedReader(new InputStreamReader(m_stream));
            String message;
            try {
                while ((message = reader.readLine()) != null && !Thread.interrupted()) {
                    distributeToListeners(message);
                }
            } catch (final IOException ex) {
                NodeLogger.getLogger(getClass()).debug("Exception during interactive logging: " + ex.getMessage(), ex);
            }
        }

        private void distributeToListeners(final String msg) {
            final boolean isWarningMessage = isWarningMessage(msg);
            final String message = isWarningMessage //
                ? stripWarningMessagePrefix(msg) //
                : msg;
            synchronized (m_listeners) {
                // Associate log messages with current node.
                try (final NodeContextManager nodeContextManager = m_nodeContextManager.pushNodeContext()) {
                    for (final PythonOutputListener listener : m_listeners) {
                        listener.messageReceived(message, isWarningMessage);
                    }
                }
            }
        }

        private static boolean isWarningMessage(final String message) {
            return message.startsWith(WARNING_MESSAGE_PREFIX);
        }

        private static String stripWarningMessagePrefix(final String message) {
            return message.substring(WARNING_MESSAGE_PREFIX.length(), message.length());
        }
    }
}
