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
 *   May 10, 2018 (marcel): created
 */
package org.knime.python2.kernel.messaging;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.knime.core.node.NodeLogger;
import org.knime.python2.kernel.PythonExecutionMonitor;
import org.knime.python2.util.PythonUtils;

/**
 * Facade class that delegates to internal workers.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class PythonMessaging implements MessageSender, MessageHandlerCollection, AutoCloseable {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonMessaging.class);

    private static final int SEND_QUEUE_LENGTH = 10;

    private static final int RECEIVE_QUEUE_LENGTH = 10;

    private final AtomicBoolean m_isRunning = new AtomicBoolean(false);

    private final AtomicInteger m_messageIdCounter = new AtomicInteger(0);

    // Send:

    private final OutputStream m_outToPython;

    private final DefaultMessageSenderLoop m_sendLoop;

    // Receive:

    private final BlockingQueue<Message> m_receiveQueue;

    private final DefaultMessageReceiverLoop m_receiveLoop;

    // Distribute:

    private final MessageDistributorLoop m_distributeLoop;

    public PythonMessaging(final OutputStream outToPython, final InputStream inFromPython,
        final PythonExecutionMonitor monitor) {
        m_outToPython = outToPython;
        m_sendLoop = new DefaultMessageSenderLoop(new DefaultMessageSender(outToPython),
            new ArrayBlockingQueue<>(SEND_QUEUE_LENGTH), monitor);

        m_receiveQueue = new ArrayBlockingQueue<>(RECEIVE_QUEUE_LENGTH);
        m_receiveLoop =
            new DefaultMessageReceiverLoop(new DefaultMessageReceiver(inFromPython), m_receiveQueue, monitor);

        m_distributeLoop = new MessageDistributorLoop(m_receiveLoop, monitor);
    }

    public boolean isRunning() {
        return m_isRunning.get();
    }

    public int createNextMessageId() {
        return m_messageIdCounter.getAndIncrement();
    }

    @Override
    public boolean registerMessageHandler(final String messageCategory, final MessageHandler handler) {
        return m_distributeLoop.registerMessageHandler(messageCategory, handler);
    }

    @Override
    public boolean unregisterMessageHandler(final String messageCategory) {
        return m_distributeLoop.unregisterMessageHandler(messageCategory);
    }

    public void start() {
        if (m_isRunning.compareAndSet(false, true)) {
            // Order is intended.
            m_distributeLoop.start();
            m_receiveLoop.start();
            m_sendLoop.start();
        }
    }

    @Override
    public void send(final Message message) throws IOException, InterruptedException {
        m_sendLoop.send(message);
    }

    @Override
    public boolean handle(final Message message) throws Exception {
        if (m_distributeLoop.canHandle(message.getCategory())) {
            m_receiveQueue.put(message);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close() {
        if (m_isRunning.compareAndSet(true, false)) {
            sendShutdownMessage();
            // Order is intended.
            final Error error =
                PythonUtils.Misc.closeSafely(LOGGER::debug, m_sendLoop, m_receiveLoop, m_distributeLoop);
            if (!isClosed()) {
                LOGGER.debug("Python messaging system could not be shut down gracefully. Process will be killed.");
            }
            if (error != null) {
                throw error;
            }
        }
    }

    private void sendShutdownMessage() {
        try {
            final Message message = new DefaultMessage(createNextMessageId(), "shutdown", null, null);
            send(message);
        } catch (final IOException ex) {
            LOGGER.debug("Exception occurred while shutting down Python messaging system. Cause: " + ex.getMessage(),
                ex);
        } catch (final InterruptedException ex) {
            // Closing the messaging system should not be interrupted.
            Thread.currentThread().interrupt();
        }
        try {
            // Give Python some time to shutdown.
            Thread.sleep(1000);
        } catch (final InterruptedException ex) {
            // Closing the messaging system should not be interrupted.
            Thread.currentThread().interrupt();
        }
    }

    private boolean isClosed() {
        boolean closed = true;
        try {
            m_outToPython.write(0);
            closed = false;
        } catch (final IOException e) {
            // Stream is closed. Python shut down properly.
        }
        return closed;
    }
}
