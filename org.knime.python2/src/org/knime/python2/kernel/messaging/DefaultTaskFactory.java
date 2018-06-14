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
 *   May 13, 2018 (marcel): created
 */
package org.knime.python2.kernel.messaging;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;

import org.knime.core.node.NodeLogger;
import org.knime.python2.kernel.PythonExecutionMonitor;

public final class DefaultTaskFactory<T> implements MessageHandler {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DefaultTaskFactory.class);

    private final TaskHandler<T> m_delegateTaskHandler;

    private final MessageSender m_messageSender;

    private final MessageHandlerCollection m_messageHandlers;

    private final IntSupplier m_messageIdSupplier;

    private final ExecutorService m_executor;

    private final PythonExecutionMonitor m_monitor;

    public DefaultTaskFactory(final TaskHandler<T> taskHandler, final MessageSender sender,
        final MessageHandlerCollection messageHandlers, final IntSupplier messageIdSupplier,
        final ExecutorService executor, final PythonExecutionMonitor monitor) {
        m_delegateTaskHandler = taskHandler;
        m_messageSender = sender;
        m_messageHandlers = messageHandlers;
        m_messageIdSupplier = messageIdSupplier;
        m_executor = executor;
        m_monitor = monitor;
    }

    public DefaultTask<T> createTask() {
        return new DefaultTask<>(null, m_delegateTaskHandler, m_messageSender, m_messageHandlers, m_messageIdSupplier,
            m_executor, m_monitor);
    }

    @Override
    public boolean handle(final Message message) throws Exception {
        if (message != m_monitor.getPoisonPill()) {
            return createTask().handle(message);
        } else {
            return true;
        }
    }

    /**
     * {@link RunnableFuture} implementation that maintains an internal message send/receive loop which is run until a
     * result is obtained or a failure occurs. The execution of the loop is blocking. It is the client's responsibility
     * to run the task concurrently if that is intended.
     *
     * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
     * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
     */
    public static final class DefaultTask<T> implements RunnableFuture<T>, MessageHandler {

        private static final int RECEIVE_QUEUE_LENGTH = 10;

        private final Message m_initiatingMessage;

        private final FutureTask<T> m_delegateTask = new FutureTask<>(this::runInternal);

        private final TaskHandler<T> m_delegateTaskHandler;

        private final MessageSender m_messageSender;

        private final MessageHandlerCollection m_messageHandlers;

        private final IntSupplier m_messageIdSupplier;

        private final ExecutorService m_executor;

        private final PythonExecutionMonitor m_monitor;

        private final BlockingQueue<Message> m_receivedMessages = new ArrayBlockingQueue<>(RECEIVE_QUEUE_LENGTH);

        private final AtomicBoolean m_isRunningOrDone = new AtomicBoolean(false);

        private boolean m_isDone = false;

        private T m_result = null;

        private String m_taskCategory = null;

        /**
         * @param message the message that initiates the task, is sent when the task starts running. May be
         *            <code>null</code>, in which case the task is initiated by an external trigger.
         * @param taskHandler the task handler that handles all subsequent messages that address the initial message of
         *            this task
         * @param sender the message sender that is used to send the initial message and all subsequent responses
         * @param messageHandlers the message handlers to which incoming messages are forwarded if they cannot be
         *            handled by {@code taskHandler}
         * @param messageIdSupplier used to create new message ids for responses
         */
        public DefaultTask(final Message message, final TaskHandler<T> taskHandler, final MessageSender sender,
            final MessageHandlerCollection messageHandlers, final IntSupplier messageIdSupplier,
            final ExecutorService executor, final PythonExecutionMonitor monitor) {
            m_initiatingMessage = message;
            m_delegateTaskHandler = taskHandler;
            m_messageSender = sender;
            m_messageHandlers = messageHandlers;
            m_messageIdSupplier = messageIdSupplier;
            m_executor = executor;
            m_monitor = monitor;
        }

        @Override
        public boolean isCancelled() {
            return m_delegateTask.isCancelled();
        }

        @Override
        public boolean isDone() {
            return m_delegateTask.isDone();
        }

        @Override
        public void run() {
            if (m_isRunningOrDone.compareAndSet(false, true)) {
                m_executor.submit(m_delegateTask);
            }
        }

        @Override
        public boolean cancel(final boolean mayInterruptIfRunning) {
            return m_delegateTask.cancel(mayInterruptIfRunning);
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            run(); // Start task if not already running.
            return m_delegateTask.get();
        }

        @Override
        public T get(final long timeout, final TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
            return m_delegateTask.get(timeout, unit);
        }

        @Override
        public boolean handle(final Message message) throws ExecutionException {
            try {
                LOGGER.debug("Java - Enqueue message for task, message: " + message + ", initiating message: "
                    + m_initiatingMessage);
                if (message == m_monitor.getPoisonPill()) {
                    do {
                        m_receivedMessages.clear();
                    } while (!m_receivedMessages.offer(message));
                } else {
                    m_receivedMessages.put(message);
                }
                run(); // Start task if not already running.
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new ExecutionException(
                    "Message handler was interrupted while handling message '" + message + "'.", ex);
            }
            return true;
        }

        private T runInternal() throws Exception {
            LOGGER.debug("Java - Run task, initiating message: " + m_initiatingMessage);
            Message toSend = m_initiatingMessage;
            while (!m_isDone && !Thread.interrupted()) {
                if (toSend != null) {
                    if (m_taskCategory == null) {
                        m_taskCategory = Integer.toString(toSend.getId());
                        m_messageHandlers.registerMessageHandler(m_taskCategory, this);
                    } else {
                        // TODO: Remove and replace by reply-to pattern.
                        m_messageHandlers.registerMessageHandler(Integer.toString(toSend.getId()), this);
                    }
                    m_messageSender.send(toSend);
                }
                LOGGER.debug("Java - Wait for message in task, initiating message: " + m_initiatingMessage);
                final Message received = m_receivedMessages.take();
                if (received == m_monitor.getPoisonPill()) {
                    LOGGER.debug("Java - Received poison pill in task, initiating message: " + m_initiatingMessage);
                    m_monitor.checkExceptions();
                    throw new IllegalStateException("Java - Task terminated due to an unknown error.");
                } else {
                    LOGGER.debug("Java - Received message in task, message: " + received + ", initiating message: "
                        + m_initiatingMessage);
                    toSend =
                        m_delegateTaskHandler.handle(received, m_messageHandlers, m_messageIdSupplier, this::setResult);
                }
            }
            // Send pending message if any.
            // This may happen if the act of responding to a message also marks (successful) termination of the task.
            if (toSend != null) {
                m_messageSender.send(toSend);
            }
            return m_result;
        }

        private void setResult(final T result) {
            m_result = result;
            m_isDone = true;
        }
    }
}
