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
 *   May 4, 2022 (marcel): created
 */
package org.knime.python3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.knime.core.node.NodeLogger;
import org.knime.python3.PythonGatewayCreationGate.PythonGatewayCreationGateListener;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Creates, holds, and provides {@link PythonGateway} instances matching specific
 * {@link PythonGatewayFactory.PythonGatewayDescription descriptions}. Clients can retrieve these instances, one at a
 * time, via {@link #create(PythonGatewayDescription)}. Upon retrieval of an instance from the queue, a new gateway is
 * automatically and asynchronously created and enqueued to keep the queue evenly populated.
 * <P>
 * The queue only holds a limited number of gateways. It evicts and {@link PythonGateway#close() closes} inactive
 * gateways (i.e., gateways that have been idling for a particular time) in case the number of entries reaches this
 * limit. It also regularly evicts and closes inactive gateway instances independent of the current number of entries.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class QueuedPythonGatewayFactory implements PythonGatewayFactory {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(QueuedPythonGatewayFactory.class);

    /**
     * The default maximum number of idling gateways that are held by the queue at any time, that is, the default
     * capacity of the queue.
     */
    public static final int DEFAULT_MAX_NUMBER_OF_IDLING_GATEWAYS = 3;

    /**
     * The default duration after which unused idling gateways are marked as expired. The default duration until expired
     * entries are actually evicted is generally longer than this value because the underlying pool performs clean-ups
     * in a timer-based manner. The clean-up interval of the timer is governed by
     * {@code EVICTION_CHECK_INTERVAL_IN_MILLISECONDS}.
     */
    public static final int DEFAULT_EXPIRATION_DURATION_IN_MINUTES = 5;

    private AbstractPythonGatewayQueue m_queue;

    @Override
    public <E extends PythonEntryPoint> PythonGateway<E> create(final PythonGatewayDescription<E> description)
        throws IOException, InterruptedException {
        synchronized (this) {
            if (m_queue == null) {
                reconfigureQueue(DEFAULT_MAX_NUMBER_OF_IDLING_GATEWAYS, DEFAULT_EXPIRATION_DURATION_IN_MINUTES);
            }
        }
        PythonGatewayCreationGate.INSTANCE.awaitPythonGatewayCreationAllowedInterruptibly();
        var gateway = m_queue.getNextGateway(description);
        if (gateway.getEntryPoint() != null) {
            LOGGER.debug("Reusing Python process with PID " + gateway.getEntryPoint().getPid());
        }
        return gateway;
    }

    /**
     * Reconfigures the queue according to the given arguments.
     * <P>
     * Implementation note: we do not expect the queue to be reconfigured regularly. Therefore we do not reconfigure it
     * while it is being in use but simply {@link #close() close} it and reinstantiate it using the provided arguments.
     *
     * @param maxNumberOfIdlingGateways The maximum number of idling gateways that are held by the queue at any time,
     *            that is, the capacity of the queue.
     * @param expirationDurationInMinutes The duration in minutes after which unused idling gateways are marked as
     *            expired. The duration until expired entries are actually evicted is generally longer than this value
     *            because the underlying queue performs clean-ups in a timer-based manner. The clean-up interval of the
     *            timer is governed by {@code EVICTION_CHECK_INTERVAL_IN_MILLISECONDS}.
     */
    public synchronized void reconfigureQueue(final int maxNumberOfIdlingGateways,
        final int expirationDurationInMinutes) {
        final boolean sameConfiguration = m_queue != null //
            && m_queue.m_maxNumberOfIdlingGateways == maxNumberOfIdlingGateways
            && m_queue.m_expirationDurationInMinutes == expirationDurationInMinutes;
        if (!sameConfiguration) {
            close();
            final var actualFactory = new FreshPythonGatewayFactory();
            if (maxNumberOfIdlingGateways != 0 && expirationDurationInMinutes != 0) {
                m_queue = new PythonGatewayQueue(maxNumberOfIdlingGateways, expirationDurationInMinutes, actualFactory);
            } else {
                m_queue =
                    new PythonGatewayDummyQueue(maxNumberOfIdlingGateways, expirationDurationInMinutes, actualFactory);
            }
        }
    }

    /**
     * Removes all {@link PythonGateway Python gateways} employing the given command from the queue.
     *
     * @param command The Python command whose corresponding gateways to remove from the queue
     */
    public synchronized void clearQueuedGateways(final PythonCommand command) {
        if (m_queue != null) {
            m_queue.clearQueuedGateways(command);
        }
    }

    /**
     * Closes all contained {@link PythonGateway Python gateways} and clears the queue. Calling
     * {@link #create(PythonGatewayDescription)} without calling {@link #reconfigureQueue(int, int)} first is not
     * allowed.
     */
    public synchronized void close() {
        if (m_queue != null) {
            m_queue.close();
        }
    }

    /**
     * The actual queue implementation of the enclosing gateway factory.
     */
    static final class PythonGatewayQueue extends AbstractPythonGatewayQueue {

        private static final int EVICTION_CHECK_INTERVAL_IN_MILLISECONDS = 60 * 1000;

        private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonGatewayQueue.class);

        private final Map<PythonGatewayDescription<?>, BlockingQueue<GatewayHolder>> m_gateways = new HashMap<>();

        private final ExecutorService m_gatewayCreators;

        private final ScheduledExecutorService m_gatewayEvictor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("python-gateway-evictor-%d").build());

        private final ExecutorService m_gatewayClosers;

        private final AtomicBoolean m_closed = new AtomicBoolean();

        /**
         * @param maxNumberOfIdlingGateways The maximum number of idling gateways that are held by the queue at any
         *            time, that is, the capacity of the queue.
         * @param expirationDurationInMinutes The duration in minutes after which unused idling gateways are marked as
         *            expired. The duration until expired entries are actually evicted is generally longer than this
         *            value because the underlying queue performs clean-ups in a timer-based manner. The clean-up
         *            interval of the timer is governed by {@code EVICTION_CHECK_INTERVAL_IN_MILLISECONDS}.
         * @param actualFactory The factory actually creating the gateways.
         */
        public PythonGatewayQueue(final int maxNumberOfIdlingGateways, final int expirationDurationInMinutes,
            final PythonGatewayFactory actualFactory) {
            super(maxNumberOfIdlingGateways, expirationDurationInMinutes, actualFactory);
            m_gatewayCreators = createThreadPoolExecutor("python-gateway-creator", false);
            m_gatewayClosers = createThreadPoolExecutor("python-gateway-closer", true);
            m_gatewayEvictor.scheduleAtFixedRate(this::evictExpiredGateways, 0l,
                EVICTION_CHECK_INTERVAL_IN_MILLISECONDS, TimeUnit.MILLISECONDS);

            PythonGatewayCreationGate.INSTANCE.registerListener(new PythonGatewayCreationGateListener() {
                @Override
                public void onPythonGatewayCreationGateOpen() {
                    // Nothing to do here.
                    // Queue is blocked anyways in QueuedPythonGatewayQueue.create() while gate is closed.
                }

                @Override
                public void onPythonGatewayCreationGateClose() {
                    evictGateways(//
                        m_gateways.values().stream()//
                            .flatMap(Collection::stream)//
                            .collect(Collectors.toList())//
                    );
                }
            });
        }

        private ExecutorService createThreadPoolExecutor(final String threadNamePrefix,
            final boolean blockIfPoolSaturated) {
            return new ThreadPoolExecutor(0, m_maxNumberOfIdlingGateways, m_expirationDurationInMinutes,
                TimeUnit.MINUTES, new SynchronousQueue<>(),
                new ThreadFactoryBuilder().setNameFormat(threadNamePrefix + "-%d").build(),
                blockIfPoolSaturated ? new CallerRunsPolicy() : new AbortPolicy());

        }

        /**
         * @param description The description of the gateways whose current number in the queue to return.
         * @return The number of gateways currently held by the queue matching the given description.
         */
        public synchronized int getNumQueuedGateways(final PythonGatewayDescription<?> description) {
            final BlockingQueue<GatewayHolder> queue = m_gateways.get(description);
            return queue != null ? queue.size() : 0;
        }

        @Override
        public <E extends PythonEntryPoint> PythonGateway<E>
            getNextGateway(final PythonGatewayDescription<E> description) throws IOException, InterruptedException {
            if (m_closed.get()) {
                throw new IllegalStateException("Queue has been closed.");
            }
            @SuppressWarnings({"resource", "unchecked"})
            final PythonGateway<E> gateway = (PythonGateway<E>)takeOrCreateGatewayAndEnqueue(description);
            return gateway;
        }

        @SuppressWarnings("resource")
        private PythonGateway<?> takeOrCreateGatewayAndEnqueue(final PythonGatewayDescription<?> description)
            throws IOException, InterruptedException {
            PythonGateway<?> gateway = null;
            try {
                synchronized (this) {
                    gateway = takeGatewayIfPresent(description);
                }
            } finally {
                try {
                    m_gatewayCreators.execute(() -> enqueueGateway(description));
                } catch (final RejectedExecutionException ex) { // NOSONAR
                    // Do not attempt to provision gateway if thread pool is saturated.
                    // TODO: Ideally, we would interrupt and discard the oldest running creation thread LRU-style and
                    // then reattempt the provisioning of the current gateway. This should, however, not block since the
                    // worst-case runtime of this method should be similar to creating a gateway without the queue.
                }
            }
            if (gateway == null) {
                gateway = m_actualFactory.create(description);
            }
            return gateway;
        }

        private PythonGateway<?> takeGatewayIfPresent(final PythonGatewayDescription<?> description)
            throws InterruptedException, IOException {
            PythonGateway<?> gateway = null;
            final BlockingQueue<GatewayHolder> queue = getGatewayQueue(description);
            if (!queue.isEmpty()) {
                final GatewayHolder holder = queue.take();
                try {
                    gateway = holder.getGatewayOrThrow();
                } catch (final InterruptedException ex) {
                    closeGateway(holder);
                    throw ex;
                }
            }
            return gateway;
        }

        @SuppressWarnings("resource")
        private void enqueueGateway(final PythonGatewayDescription<?> description) {
            GatewayHolder holder = null;
            try {
                holder = new GatewayHolder(m_actualFactory.create(description));
            } catch (final Exception ex) { // NOSONAR Exceptions not caught here would get lost.
                holder = new GatewayHolder(ex);
            }
            synchronized (this) {
                final BlockingQueue<GatewayHolder> queue = getGatewayQueue(description);
                queue.add(holder);
                final int numToEvict =
                    m_gateways.values().stream().mapToInt(BlockingQueue::size).sum() - m_maxNumberOfIdlingGateways;
                evictLRUGateways(numToEvict);
            }
        }

        private BlockingQueue<GatewayHolder> getGatewayQueue(final PythonGatewayDescription<?> description) {
            return m_gateways.computeIfAbsent(description, d -> new LinkedBlockingQueue<>());
        }

        @Override
        public synchronized void clearQueuedGateways(final PythonCommand command) {
            final List<GatewayHolder> gatewaysToEvict = new ArrayList<>();
            for (final var entry : m_gateways.entrySet()) {
                if (entry.getKey().getCommand().equals(command)) {
                    gatewaysToEvict.addAll(entry.getValue());
                }
            }
            evictGateways(gatewaysToEvict);
        }

        @Override
        public void close() {
            if (m_closed.compareAndSet(false, true)) {
                m_gatewayCreators.shutdownNow();
                m_gatewayEvictor.shutdown();
                try {
                    m_gatewayCreators.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                    m_gatewayEvictor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
                final List<GatewayHolder> allGateways = m_gateways.values().stream() //
                    .flatMap(Queue::stream) //
                    .collect(Collectors.toList());
                evictGateways(allGateways);
                m_gatewayClosers.shutdown();
                try {
                    m_gatewayClosers.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        private synchronized void evictExpiredGateways() {
            final long currentTimestamp = System.currentTimeMillis();
            final long expirationDurationInMillis = m_expirationDurationInMinutes * 60l * 1000l;
            final List<GatewayHolder> expiredGateways = m_gateways.values().stream() //
                .flatMap(Queue::stream) //
                .filter(h -> (currentTimestamp - h.m_timestamp) >= expirationDurationInMillis) //
                .collect(Collectors.toList());
            evictGateways(expiredGateways);
        }

        private void evictLRUGateways(final int numToEvict) {
            if (numToEvict > 0) {
                final Set<GatewayHolder> gatewaysToEvict = m_gateways.values().stream() //
                    .flatMap(Queue::stream) //
                    .sorted(Comparator.comparingLong((final GatewayHolder h) -> h.m_timestamp)) //
                    .limit(numToEvict) //
                    .collect(Collectors.toSet());
                evictGateways(gatewaysToEvict);
            }
        }

        private void evictGateways(final Collection<GatewayHolder> gatewaysToEvict) {
            if (!gatewaysToEvict.isEmpty()) {
                for (final var it = m_gateways.entrySet().iterator(); it.hasNext();) {
                    final var entry = it.next();
                    final BlockingQueue<GatewayHolder> queue = entry.getValue();
                    while (!queue.isEmpty() && gatewaysToEvict.contains(queue.peek())) {
                        queue.remove(); // NOSONAR We already keep track of the returned gateway.
                    }
                    if (queue.isEmpty()) {
                        it.remove();
                    }
                }
                for (final var gateway : gatewaysToEvict) {
                    closeGateway(gateway);
                }
            }
        }

        private void closeGateway(final GatewayHolder gateway) {
            m_gatewayClosers.execute(() -> {
                try {
                    gateway.closeGateway();
                } catch (final IOException ex) {
                    LOGGER.error(ex);
                }
            });
        }

        private static final class GatewayHolder {

            private final PythonGateway<?> m_gateway;

            private final Exception m_exception;

            private final long m_timestamp = System.currentTimeMillis();

            public GatewayHolder(final PythonGateway<?> gateway) {
                this(gateway, null);
            }

            public GatewayHolder(final Exception exception) {
                this(null, exception);
            }

            private GatewayHolder(final PythonGateway<?> gateway, final Exception exception) {
                m_gateway = gateway;
                m_exception = exception;
            }

            public PythonGateway<?> getGatewayOrThrow() throws IOException, InterruptedException { // NOSONAR Private API
                if (m_exception != null) {
                    if (m_exception instanceof IOException) {
                        throw (IOException)m_exception;
                    } else if (m_exception instanceof InterruptedException) {
                        throw (InterruptedException)m_exception;
                    } else {
                        throw new IllegalStateException(m_exception);
                    }
                } else {
                    return m_gateway;
                }
            }

            // No try-with-resource since we want to distinguish between failures during setup (ignored) and closing
            // (propagated).
            @SuppressWarnings("resource")
            public void closeGateway() throws IOException {
                PythonGateway<?> gateway = null;
                try {
                    gateway = getGatewayOrThrow();
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                } catch (final Exception ex) { // NOSONAR We're closing anyway, failures during setup are irrelevant.
                    LOGGER.debug(ex);
                }
                if (gateway != null) {
                    gateway.close();
                }
            }
        }
    }

    private static final class PythonGatewayDummyQueue extends AbstractPythonGatewayQueue {

        public PythonGatewayDummyQueue(final int maxNumberOfIdlingGateways, final int expirationDurationInMinutes,
            final PythonGatewayFactory actualFactory) {
            super(maxNumberOfIdlingGateways, expirationDurationInMinutes, actualFactory);
        }

        @Override
        public <E extends PythonEntryPoint> PythonGateway<E>
            getNextGateway(final PythonGatewayDescription<E> description) throws IOException, InterruptedException {
            return m_actualFactory.create(description);
        }

        @Override
        public void clearQueuedGateways(final PythonCommand command) {
            // Nothing to do.
        }

        @Override
        public void close() {
            // Nothing to do.
        }
    }

    private abstract static class AbstractPythonGatewayQueue implements AutoCloseable {

        protected final int m_maxNumberOfIdlingGateways;

        protected final int m_expirationDurationInMinutes;

        protected final PythonGatewayFactory m_actualFactory;

        public AbstractPythonGatewayQueue(final int maxNumberOfIdlingGateways, final int expirationDurationInMinutes,
            final PythonGatewayFactory actualFactory) {
            m_maxNumberOfIdlingGateways = maxNumberOfIdlingGateways;
            m_expirationDurationInMinutes = expirationDurationInMinutes;
            m_actualFactory = actualFactory;
        }

        public abstract <E extends PythonEntryPoint> PythonGateway<E>
            getNextGateway(PythonGatewayDescription<E> description) throws IOException, InterruptedException;

        /**
         * Clears all queued gateways that were created with the specified {@link PythonCommand}.
         *
         * @param command The {@link PythonCommand}
         */
        public abstract void clearQueuedGateways(PythonCommand command);

        @Override
        public abstract void close();
    }
}
