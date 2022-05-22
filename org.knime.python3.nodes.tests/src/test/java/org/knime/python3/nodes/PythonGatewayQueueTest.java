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
 *   May 17, 2022 (marcel): created
 */
package org.knime.python3.nodes;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.After;
import org.junit.Test;
import org.knime.python3.PythonEntryPoint;
import org.knime.python3.PythonGateway;
import org.knime.python3.PythonGatewayFactory;
import org.knime.python3.PythonGatewayFactory.PythonGatewayDescription;
import org.knime.python3.QueuedPythonGatewayFactory;
import org.knime.python3.QueuedPythonGatewayFactory.PythonGatewayQueue;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public final class PythonGatewayQueueTest {

    private static final PythonGatewayDescription<?>[] DESCRIPTIONS = new PythonGatewayDescription[10];

    private static final PythonGatewayDescription<?> DESCRIPTION_0;

    private static final int MAX_NUMBER_OF_IDLING_KERNELS =
        QueuedPythonGatewayFactory.DEFAULT_MAX_NUMBER_OF_IDLING_GATEWAYS;

    static {
        for (int i = 0; i < DESCRIPTIONS.length; i++) {
            DESCRIPTIONS[i] = PythonGatewayDescription.builder(null, Paths.get(Integer.toString(i)), null).build();
        }
        DESCRIPTION_0 = DESCRIPTIONS[0];
    }

    private final TestPythonGatewayFactory m_gatewayFactory = new TestPythonGatewayFactory();

    private final PythonGatewayQueue m_queue = new PythonGatewayQueue(MAX_NUMBER_OF_IDLING_KERNELS,
        QueuedPythonGatewayFactory.DEFAULT_EXPIRATION_DURATION_IN_MINUTES, m_gatewayFactory);

    @After
    public void shutdown() {
        m_queue.close();
    }

    /**
     * Taking a gateway from an unpopulated queue should result in the ad hoc creation of the requested gateway and an
     * additional provisional creation of a gateway for the same description.
     */
    @Test
    public void testTakeGatewayFromQueueOnce() throws IOException, InterruptedException {
        assertEquals(0, m_queue.getNumQueuedGateways(DESCRIPTION_0));
        takeGatewayAndClose(DESCRIPTION_0);
        m_gatewayFactory.waitForNumCreatedGateways(2);
        assertNumQueuedGateways(1, DESCRIPTION_0);
        closeQueue(2);
    }

    /**
     * Taking two gateways with the same description from an unpopulated queue in succession should result in the ad hoc
     * creation of the two requested gateways and -- given that the second request happens before the queue has the
     * chance to populate itself after handling the first request -- should result in two additional provisional
     * creations of gateways for the same description (one per "queue miss").
     */
    @Test
    public void testTakeGatewayFromQueueTwiceWithoutWaiting() throws IOException, InterruptedException {
        assertEquals(0, m_queue.getNumQueuedGateways(DESCRIPTION_0));
        takeGatewayAndClose(DESCRIPTION_0);
        takeGatewayAndClose(DESCRIPTION_0);
        m_gatewayFactory.waitForNumCreatedGateways(4);
        assertNumQueuedGateways(2, DESCRIPTION_0);
        closeQueue(4);
    }

    /**
     * Taking two gateways with the same description from an initially unpopulated queue in succession -- but with
     * enough time between the first and the second request for the queue to create a provisional entry -- should result
     * in the ad hoc creation of the first entry, the provisional creation of the second entry, and a third provisional
     * creation for potential future use.
     */
    @Test
    public void testTakeGatewayFromQueueTwiceWithWaiting() throws IOException, InterruptedException {
        assertEquals(0, m_queue.getNumQueuedGateways(DESCRIPTION_0));
        takeGatewayAndClose(DESCRIPTION_0);
        m_gatewayFactory.waitForNumCreatedGateways(2);
        assertNumQueuedGateways(1, DESCRIPTION_0);
        takeGatewayAndClose(DESCRIPTION_0);
        m_gatewayFactory.waitForNumCreatedGateways(3);
        assertNumQueuedGateways(1, DESCRIPTION_0);
        closeQueue(3);
    }

    /**
     * Taking a gateway from an unpopulated queue should result in the ad hoc creation of the requested gateway and an
     * additional provisional creation of a gateway for the same description. Doing this for different descriptions
     * should therefore yield 2 * number of unique descriptions many gateway creations.
     */
    @Test
    public void testTakeDifferentGatewaysFromQueueOnce() throws IOException, InterruptedException {
        for (int i = 0; i < MAX_NUMBER_OF_IDLING_KERNELS; i++) {
            assertEquals(0, m_queue.getNumQueuedGateways(DESCRIPTIONS[i]));
        }
        for (int i = 0; i < MAX_NUMBER_OF_IDLING_KERNELS; i++) {
            takeGatewayAndClose(DESCRIPTIONS[i]);
        }
        m_gatewayFactory.waitForNumCreatedGateways(MAX_NUMBER_OF_IDLING_KERNELS * 2);
        for (int i = 0; i < MAX_NUMBER_OF_IDLING_KERNELS; i++) {
            assertNumQueuedGateways(1, DESCRIPTIONS[i]);
        }
        closeQueue(MAX_NUMBER_OF_IDLING_KERNELS * 2);
    }

    /**
     * After the queue has been saturated, requesting gateways for which there is no entry queued evicts the least
     * recently used entries.
     */
    @Test
    public void testLRUEviction() throws IOException, InterruptedException {
        for (final var description : DESCRIPTIONS) {
            assertEquals(0, m_queue.getNumQueuedGateways(description));
        }
        // Saturate the queue.
        for (int i = 0; i < MAX_NUMBER_OF_IDLING_KERNELS; i++) {
            takeGatewayAndClose(DESCRIPTIONS[i]);
            Thread.sleep(1); // Make sure the timestamps of the queued gateways are different.
        }
        m_gatewayFactory.waitForNumCreatedGateways(MAX_NUMBER_OF_IDLING_KERNELS * 2);
        for (int i = 0; i < MAX_NUMBER_OF_IDLING_KERNELS; i++) {
            assertNumQueuedGateways(1, DESCRIPTIONS[i]);
        }
        // Trigger the creation of more gateways. These will replace the oldest entries in the queue.
        takeGatewayAndClose(DESCRIPTIONS[MAX_NUMBER_OF_IDLING_KERNELS]);
        takeGatewayAndClose(DESCRIPTIONS[MAX_NUMBER_OF_IDLING_KERNELS + 1]);
        takeGatewayAndClose(DESCRIPTIONS[MAX_NUMBER_OF_IDLING_KERNELS + 2]);
        m_gatewayFactory.waitForNumCreatedGateways(MAX_NUMBER_OF_IDLING_KERNELS * 2 + 3 * 2);
        m_gatewayFactory.waitForNumActiveGateways(MAX_NUMBER_OF_IDLING_KERNELS);

        assertNumQueuedGateways(0, DESCRIPTIONS[0]);
        assertNumQueuedGateways(0, DESCRIPTIONS[1]);
        assertNumQueuedGateways(0, DESCRIPTIONS[2]);
        for (int i = 3; i < MAX_NUMBER_OF_IDLING_KERNELS + 3; i++) {
            assertNumQueuedGateways(1, DESCRIPTIONS[i]);
        }

        closeQueue(MAX_NUMBER_OF_IDLING_KERNELS * 2 + 3 * 2);
    }

    private void takeGatewayAndClose(final PythonGatewayDescription<?> description)
        throws IOException, InterruptedException {
        try (final PythonGateway<?> gateway = m_queue.getNextGateway(description)) {
            assertEquals(description, ((TestPythonGateway)gateway).m_description);
        }
    }

    private void assertNumQueuedGateways(final int expectedNumQueuedGateways,
        final PythonGatewayDescription<?> description) throws InterruptedException {
        if (m_queue.getNumQueuedGateways(description) != expectedNumQueuedGateways) {
            // Gateway creation is reported by the test factory before the gateway is actually enqueued.
            Thread.sleep(100);
            assertEquals(expectedNumQueuedGateways, m_queue.getNumQueuedGateways(description));
        }
    }

    /**
     * Make sure that after closing: 1. all queues are cleared, 2. all gateways have actually been
     * {@link AutoCloseable#close() closed}, and 3. the total number of created gateways has not increased compared to
     * when the caller checked last.
     */
    private void closeQueue(final int expectedTotalNumCreatedGateways) {
        m_queue.close();
        for (final var description : DESCRIPTIONS) {
            assertEquals(0, m_queue.getNumQueuedGateways(description));
        }
        assertEquals(0, m_gatewayFactory.getNumActiveGateways());
        assertEquals(expectedTotalNumCreatedGateways, m_gatewayFactory.getNumCreatedGateways());
    }

    private static final class TestPythonGatewayFactory implements PythonGatewayFactory {

        private int m_numActiveGateways = 0;

        private int m_numCreatedGateways = 0;

        private final Lock m_numGatewaysLock = new ReentrantLock();

        private final Condition m_numGatewaysChanged = m_numGatewaysLock.newCondition();

        @Override
        public <E extends PythonEntryPoint> PythonGateway<E> create(final PythonGatewayDescription<E> description)
            throws IOException, InterruptedException {
            @SuppressWarnings({"resource", "unchecked"})
            final PythonGateway<E> casted =
                (PythonGateway<E>)new TestPythonGateway((PythonGatewayDescription<PythonEntryPoint>)description, this);
            gatewayCreated();
            return casted;
        }

        public int getNumActiveGateways() {
            m_numGatewaysLock.lock();
            try {
                return m_numActiveGateways;
            } finally {
                m_numGatewaysLock.unlock();
            }
        }

        public int getNumCreatedGateways() {
            m_numGatewaysLock.lock();
            try {
                return m_numCreatedGateways;
            } finally {
                m_numGatewaysLock.unlock();
            }
        }

        private void gatewayCreated() {
            m_numGatewaysLock.lock();
            try {
                m_numActiveGateways++;
                m_numCreatedGateways++;
                m_numGatewaysChanged.signalAll();
            } finally {
                m_numGatewaysLock.unlock();
            }
        }

        public void gatewayClosed() {
            m_numGatewaysLock.lock();
            try {
                m_numActiveGateways--;
                m_numGatewaysChanged.signalAll();
            } finally {
                m_numGatewaysLock.unlock();
            }
        }

        public void waitForNumActiveGateways(final int numActiveGateways) throws InterruptedException {
            m_numGatewaysLock.lock();
            try {
                while (m_numActiveGateways != numActiveGateways) {
                    m_numGatewaysChanged.await();
                }
            } finally {
                m_numGatewaysLock.unlock();
            }
        }

        public void waitForNumCreatedGateways(final int numCreatedGateways) throws InterruptedException {
            m_numGatewaysLock.lock();
            try {
                if (numCreatedGateways < m_numCreatedGateways) {
                    throw new IllegalArgumentException(numCreatedGateways + " vs " + m_numActiveGateways);
                }
                while (m_numCreatedGateways != numCreatedGateways) {
                    if (!m_numGatewaysChanged.await(4l * TestPythonGateway.CREATION_DELAY_IN_MILLIS,
                        TimeUnit.MILLISECONDS)) {
                        throw new AssertionError("Queue failed to produce gateway in time: " + numCreatedGateways
                            + " vs " + m_numActiveGateways);
                    }
                }
            } finally {
                m_numGatewaysLock.unlock();
            }
        }
    }

    private static final class TestPythonGateway implements PythonGateway<PythonEntryPoint> {

        private static final int CREATION_DELAY_IN_MILLIS = 500;

        private final PythonGatewayDescription<PythonEntryPoint> m_description;

        private final TestPythonGatewayFactory m_factory;

        public TestPythonGateway(final PythonGatewayDescription<PythonEntryPoint> description,
            final TestPythonGatewayFactory factory) throws InterruptedException {
            m_description = description;
            m_factory = factory;
            Thread.sleep(CREATION_DELAY_IN_MILLIS);
        }

        @Override
        public PythonEntryPoint getEntryPoint() {
            return null;
        }

        @Override
        public InputStream getStandardOutputStream() {
            return null;
        }

        @Override
        public InputStream getStandardErrorStream() {
            return null;
        }

        @Override
        public void close() throws IOException {
            m_factory.gatewayClosed();
        }
    }
}
