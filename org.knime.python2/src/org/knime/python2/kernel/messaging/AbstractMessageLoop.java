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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.knime.python2.kernel.PythonExecutionMonitor;
import org.knime.python2.kernel.PythonKernel;
import org.knime.python2.util.PythonNodeLogger;
import org.knime.python2.util.PythonUtils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
abstract class AbstractMessageLoop implements AutoCloseable {

    protected static final PythonNodeLogger LOGGER = PythonNodeLogger.getLogger(PythonKernel.class);

    protected static void throwExceptionInLoop(final String errorMessage, final Exception cause) throws Exception {
        final String fullErrorMessage =
            errorMessage + (cause.getMessage() != null ? " Cause: " + cause.getMessage() : "");
        throw cause instanceof InterruptedException ? new InterruptedException(fullErrorMessage)
            : new Exception(fullErrorMessage, cause);
    }

    protected static void clearQueueAndPutMessage(final BlockingQueue<Message> queue, final Message message) {
        do {
            queue.clear();
        } while (!queue.offer(message));
    }

    protected final PythonExecutionMonitor m_monitor;

    private final ExecutorService m_executor;

    /**
     * <code>true</code> if the loop is running, i.e. if {@link #start()} was called and {@link #close()} was not yet
     * called, <code>false</code> otherwise.
     */
    private final AtomicBoolean m_isRunning = new AtomicBoolean(false);

    /**
     * <code>true</code> if {@link #close()} was called, <code>false</code> otherwise.
     */
    private final AtomicBoolean m_isClosedOrClosing = new AtomicBoolean(false);

    private Exception m_exceptionDuringClose = null;

    public AbstractMessageLoop(final PythonExecutionMonitor monitor, final String loopThreadName) {
        m_monitor = monitor;
        m_executor =
            Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(loopThreadName + "-%d").build());
    }

    /**
     * This method is called once in the course of {@link #start()} to initiate the message loop. Looping has to be
     * implemented within the method itself. The method is executed in its own dedicated thread.
     *
     * @throws Exception any exception that occurs while running the loop
     */
    protected abstract void loop() throws Exception;

    /**
     * @return <code>true</code> if the message loop is up and running, <code>false</code> otherwise
     */
    public boolean isRunning() {
        return m_isRunning.get();
    }

    /**
     * Starts the message loop if it is not already {@link #isRunning() running}. Does nothing otherwise.<br>
     * The loop can be shutdown via {@link #close()}.
     *
     * @throws IllegalStateException if the loop is in the process of closing or is already closed
     */
    public void start() {
        if (m_isRunning.compareAndSet(false, true)) {
            if (m_isClosedOrClosing.get()) {
                m_isRunning.set(false);
                throw new IllegalStateException("Cannot restart a closed message loop.");
            } else {
                m_executor.execute(this::doLoop);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (m_isRunning.compareAndSet(true, false)) {
            try {
                m_isClosedOrClosing.set(true);
                PythonUtils.Misc.invokeSafely(LOGGER::debug, ExecutorService::shutdown, m_executor);
                try {
                    if (!(m_executor.awaitTermination(1, TimeUnit.SECONDS))) {
                        PythonUtils.Misc.invokeSafely(LOGGER::debug, ExecutorService::shutdownNow, m_executor);
                    }
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    PythonUtils.Misc.invokeSafely(LOGGER::debug, ExecutorService::shutdownNow, m_executor);
                }
                closeInternal();
            } catch (final Exception ex) {
                LOGGER.debug(getClass().getName() + ": Failed to shutdown properly. Cause: " + ex.getMessage(), ex);
                throw ex;
            }
        } else if (m_exceptionDuringClose != null) {
            throw m_exceptionDuringClose;
        }
    }

    /**
     * Called at the end of {@link #close()}.
     *
     * @throws Exception if any exception occurs while closing
     */
    protected void closeInternal() throws Exception {
        // No-op by default.
    }

    private void doLoop() {
        try {
            loop();
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            LOGGER.debug(getClass().getName() + ": Interrupt occurred. Loop terminated.");
        } catch (final Exception ex) {
            LOGGER.debug(ex.getMessage(), ex);
            LOGGER.debug(getClass().getName() + ": Loop terminated due to above exception.");
            if (m_isRunning.get() && !m_isClosedOrClosing.get()) {
                m_monitor.reportException(ex);
            }
        } finally {
            try {
                close();
            } catch (final Exception ex) {
                m_exceptionDuringClose = ex;
            }
        }
    }
}
