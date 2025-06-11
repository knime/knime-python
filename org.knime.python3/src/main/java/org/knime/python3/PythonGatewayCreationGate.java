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
 *   24 Nov 2022 (chaubold): created
 */
package org.knime.python3;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.knime.core.node.NodeLogger;

/**
 * Gate that controls Python gateway creation. Allows to block gateway creation and to kill all Python processes if
 * needed.
 *
 * The keeps a count of how often it was closed, state changes only occur if it is opened enough to bring this
 * count down to zero.
 *
 * Registered listeners will be notified if the overall state of the gate changes between opened and closed.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction")
public final class PythonGatewayCreationGate {

    /**
     * Interface for listeners to the {@link PythonGatewayCreationGate} opening and closing
     */
    public interface PythonGatewayCreationGateListener {
        /**
         * Called as soon as creating gateways becomes possible
         */
        void onPythonGatewayCreationGateOpen();

        /**
         * Called as soon as creating gateways becomes blocked
         */
        void onPythonGatewayCreationGateClose();
    }

    /**
     * The singleton instance of the Gate
     */
    public static final PythonGatewayCreationGate INSTANCE = new PythonGatewayCreationGate();

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonGatewayCreationGate.class);

    private ReentrantReadWriteLock m_gatewayLock = new ReentrantReadWriteLock();

    private AtomicInteger m_blockCount = new AtomicInteger(0);

    private final List<PythonGatewayCreationGateListener> m_listeners = new ArrayList<>();

    private PythonGatewayCreationGate() {
        // hidden constructor
    }

    /**
     * Close the gate, block creation of Python gateways. If the gate was already closed before, it stays closed but now
     * has to be opened once more to be open again.
     *
     * If the state changes from open to closed, listeners will be notified synchronously of this event.
     */
    void blockPythonCreation() {
        LOGGER.info("Blocking Python process startup");
        if (m_blockCount.getAndIncrement() == 0) {
            m_gatewayLock.writeLock().lock();

            synchronized (m_listeners) {
                m_listeners.forEach(PythonGatewayCreationGateListener::onPythonGatewayCreationGateClose);
            }
        }
    }

    /**
     * Open the gate = allow creation of Python gateways. If the gate was closed multiple times, opening it once could
     * mean that it is still closed. Only if it was opened as often as it was closed the gate will really open.
     *
     * If the state changes from closed to open, listeners will be notified synchronously of this event.
     */
    void allowPythonCreation() {
        if (m_blockCount.getAndDecrement() == 1) {
            LOGGER.info("Allowing Python process startup again after installation failed");
            m_gatewayLock.writeLock().unlock();

            synchronized (m_listeners) {
                m_listeners.forEach(PythonGatewayCreationGateListener::onPythonGatewayCreationGateOpen);
            }
        }
    }

    /**
     * @return true if the gate is open.
     */
    public boolean isPythonGatewayCreationAllowed() {
        return !m_gatewayLock.isWriteLocked();
    }

    /**
     * Wait for Python gateway creation to be allowed again, allowing for interruptions.
     * @throws InterruptedException
     */
    public void awaitPythonGatewayCreationAllowedInterruptibly() throws InterruptedException {
        m_gatewayLock.readLock().lockInterruptibly();
        m_gatewayLock.readLock().unlock();
    }

    /**
     * Register a listener that is notified whenever the gate opens and closes
     * @param listener
     */
    public void registerListener(final PythonGatewayCreationGateListener listener) {
        synchronized (m_listeners) {
            m_listeners.add(listener);
        }
    }

    /**
     * Remove a listener that was notified whenever the gate opens and closes
     * @param listener
     */
    public void deregisterListener(final PythonGatewayCreationGateListener listener) {
        synchronized (m_listeners) {
            m_listeners.remove(listener);
        }
    }

}
