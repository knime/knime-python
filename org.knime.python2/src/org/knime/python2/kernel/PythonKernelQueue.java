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
 *   Mar 17, 2020 (marcel): created
 */
package org.knime.python2.kernel;

import java.util.LinkedHashSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.knime.python2.PythonCommand;
import org.knime.python2.PythonModuleSpec;
import org.knime.python2.util.PythonUtils;

import com.google.common.collect.Iterables;

/**
 * Creates, holds, and provides {@link PythonKernel} instances for specific combinations of {@link PythonCommand Python
 * commands} and (preloaded) additional Python modules. Clients can retrieve these instances, one at a time, via
 * {@link #getNextKernel(PythonCommand, Set, Set, PythonKernelOptions, PythonCancelable) getNextKernel}. Upon retrieval
 * of an instance from the queue, a new kernel is automatically and asynchronously created and enqueued to keep the
 * queue evenly populated.
 * <P>
 * The queue only holds a limited number of kernels. It evicts and {@link PythonKernelQueue#close() closes} inactive
 * kernels (i.e., kernels that have been idling for a specific time) in case the number of entries reaches this limit.
 * It also regularly evicts and closes inactive kernel instances independent of the current number of entries.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class PythonKernelQueue {

    // Class:

    /**
     * The default maximum number of idling kernels that are held by the queue at any time, that is, the default
     * capacity of the queue.
     */
    public static final int DEFAULT_MAX_NUMBER_OF_IDLING_KERNELS = 3;

    /**
     * The default duration after which unused idling kernels are marked as expired. The default duration until expired
     * entries are actually evicted is generally longer than this value because the underlying pool performs clean-ups
     * in a timer-based manner. The clean-up interval of the timer is governed by
     * {@code EVICTION_CHECK_INTERVAL_IN_MILLISECONDS}.
     */
    public static final int DEFAULT_EXPIRATION_DURATION_IN_MINUTES = 5;

    private static final int EVICTION_CHECK_INTERVAL_IN_MILLISECONDS = 60 * 1000;

    private static final int CANCELLATION_CHECK_INTERVAL_IN_MILLISECONDS = 1000;

    /**
     * The singleton instance.
     */
    private static final PythonKernelQueue INSTANCE = new PythonKernelQueue();

    /**
     * Takes the next {@link PythonKernel} from the queue that was launched using the given {@link PythonCommand} and
     * has the given modules preloaded. Configures it according to the given {@link PythonKernelOptions} and returns it.
     * The caller is responsible for {@link PythonKernel#close() closing} the kernel.<br>
     * This method blocks until a kernel is present in the queue.
     * <P>
     * Note that specifying additional modules should only be done if loading these modules is time-consuming since,
     * internally, an own queue will be registered for each combination of Python command and modules. Having too many
     * of these combinations could defeat the purpose of queuing since all these queues will compete for the limited
     * number of available slots in the overall queue.
     *
     * @param command The {@link PythonCommand}.
     * @param requiredAdditionalModules The modules that must already be loaded in the returned kernel. May not be
     *            {@code null}, but empty.
     * @param optionalAdditionalModules The modules that should already -- but do not need to -- be loaded in the
     *            returned kernel. May not be {@code null}, but empty.
     * @param options The {@link PythonKernelOptions} according to which the returned {@link PythonKernel} is
     *            configured.
     * @param cancelable The {@link PythonCancelable} that is used to check whether retrieving the kernel from the queue
     *            (i.e., waiting until a kernel is present in the queue) should be canceled.
     * @return The next appropriate {@link PythonKernel} in the queue, configured according to the given
     *         {@link PythonKernelOptions}.
     * @throws PythonCanceledExecutionException If retrieving the kernel has been canceled or
     *             {@link InterruptedException interrupted}.
     * @throws PythonIOException The exception that was thrown while originally constructing the kernel that is next in
     *             the queue, if any. Such exceptions are preserved and rethrown by this method in order to make calling
     *             this method equivalent to constructing the kernel directly, from an exception-delivery point of view.
     */
    public static PythonKernel getNextKernel(final PythonCommand command,
        final Set<PythonModuleSpec> requiredAdditionalModules, final Set<PythonModuleSpec> optionalAdditionalModules,
        final PythonKernelOptions options, final PythonCancelable cancelable)
        throws PythonCanceledExecutionException, PythonIOException {
        return INSTANCE.getNextKernelInternal(command, requiredAdditionalModules, optionalAdditionalModules, options,
            cancelable);
    }

    /**
     * Closes all contained {@link PythonKernel Python kernels} and clears the queue. Calling
     * {@link #getNextKernel(PythonCommand, Set, Set, PythonKernelOptions, PythonCancelable) getNextKernel} is not
     * allowed once the queue is closed.
     */
    public static void close() {
        INSTANCE.m_pool.close();
    }

    // Instance:

    /**
     * An object pool that is used as a collection of queues of {@link PythonKernel Python kernels}, each queue indexed
     * by the command and sets of preloaded Python modules for which it holds kernel instances.
     * <P>
     * The semantic differences between queues (where clients take items without returning them) and pools (borrowing
     * and returning items) is bridged by not pooling kernels directly but rather wrapping them in reusable containers,
     * extracting them upon borrowing, and returning and repopulating the containers.
     */
    private final GenericKeyedObjectPool<PythonCommandAndModules, PythonKernelOrExceptionHolder> m_pool;

    private PythonKernelQueue() {
        final GenericKeyedObjectPoolConfig<PythonKernelOrExceptionHolder> config = new GenericKeyedObjectPoolConfig<>();
        config.setEvictorShutdownTimeoutMillis(0);
        config.setFairness(true);
        config.setJmxEnabled(false);
        config.setLifo(false);
        config.setMaxIdlePerKey(-1);
        config.setMaxTotal(DEFAULT_MAX_NUMBER_OF_IDLING_KERNELS);
        config.setMaxTotalPerKey(-1);
        config.setMaxWaitMillis(CANCELLATION_CHECK_INTERVAL_IN_MILLISECONDS);
        config.setMinEvictableIdleTimeMillis(DEFAULT_EXPIRATION_DURATION_IN_MINUTES * 60l * 1000l);
        config.setNumTestsPerEvictionRun(-1);
        config.setTimeBetweenEvictionRunsMillis(EVICTION_CHECK_INTERVAL_IN_MILLISECONDS);
        m_pool = new GenericKeyedObjectPool<>(new KeyedPooledPythonKernelFactory(), config);
    }

    @SuppressWarnings("resource") // Kernel is closed by the client.
    private PythonKernel getNextKernelInternal(final PythonCommand command,
        final Set<PythonModuleSpec> requiredAdditionalModules, final Set<PythonModuleSpec> optionalAdditionalModules,
        final PythonKernelOptions options, final PythonCancelable cancelable)
        throws PythonCanceledExecutionException, PythonIOException {
        final PythonCommandAndModules key =
            new PythonCommandAndModules(command, requiredAdditionalModules, optionalAdditionalModules);
        final PythonKernelOrExceptionHolder holder = dequeueHolder(key, cancelable);
        PythonKernel kernel = extractKernelAndEnqueueNewOne(key, holder);
        kernel = configureOrRecreateKernel(key, kernel, options);
        return kernel;
    }

    @SuppressWarnings("resource") // Holder was not taken from pool when this method throws.
    private PythonKernelOrExceptionHolder dequeueHolder(final PythonCommandAndModules key,
        final PythonCancelable cancelable) throws PythonCanceledExecutionException {
        PythonKernelOrExceptionHolder holder = null;
        do {
            try {
                holder = m_pool.borrowObject(key);
            } catch (final NoSuchElementException ex) {
                cancelable.checkCanceled();
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new PythonCanceledExecutionException(ex);
            } catch (final Exception ex) {
                // Should not happen. The only significant source of further checked exceptions is the object-pool
                // factory. And our implementation of the factory (see below) does not throw any checked exceptions.
                throw new IllegalStateException(ex);
            }
        } while (holder == null);
        return holder;
    }

    private PythonKernel extractKernelAndEnqueueNewOne(final PythonCommandAndModules key,
        final PythonKernelOrExceptionHolder holder) throws PythonIOException {
        try {
            return holder.clearFieldsAndReturnKernelOrThrow();
        } finally {
            new Thread(() -> m_pool.returnObject(key, holder), "python-kernel-creator").start();
        }
    }

    /**
     * Setting options may fail if the Python process crashed between adding it to the queue and now taking it from the
     * queue. We try to recover from such a situation by opening and configuring a new kernel (once). Note that we
     * naturally do not try to recover from a failed installation test since this requires user action. However, it is
     * our responsibility to close the kernel in any exceptional situation here since the client will not have a handle
     * to the kernel.
     */
    private static PythonKernel configureOrRecreateKernel(final PythonCommandAndModules key, PythonKernel kernel,
        final PythonKernelOptions options) throws PythonIOException {
        try {
            kernel.setOptions(options);
        } catch (final PythonInstallationTestException ex) {
            PythonUtils.Misc.closeSafelyThrowErrors(null, kernel);
            throw ex;
        } catch (final PythonIOException ex) {
            PythonUtils.Misc.closeSafelyThrowErrors(null, kernel);
            kernel = KeyedPooledPythonKernelFactory.createKernel(key);
            try {
                kernel.setOptions(options);
            } catch (final Throwable t) {
                PythonUtils.Misc.closeSafelyThrowErrors(null, kernel);
                t.addSuppressed(ex);
                throw t;
            }
        } catch (final Throwable t) {
            PythonUtils.Misc.closeSafelyThrowErrors(null, kernel);
            throw t;
        }
        return kernel;
    }

    /**
     * Manages the life-cycle of pooled kernel holders. In particular:
     * <ul>
     * <li>creates and populates new holders when the pool contains no idling instances and
     * {@link PythonKernelQueue#getNextKernelInternal(PythonCommand, Set, Set, PythonKernelOptions, PythonCancelable)}
     * is called</li>
     * <li>repopulates holders when they are returned to the pool after extracting their kernel in
     * {@link PythonKernelQueue#extractKernelAndEnqueueNewOne(PythonCommandAndModules, PythonKernelOrExceptionHolder)}</li>
     * <li>closes kernels if their holders are evicted</li
     * </ul>
     */
    private static final class KeyedPooledPythonKernelFactory
        implements KeyedPooledObjectFactory<PythonCommandAndModules, PythonKernelOrExceptionHolder> {

        @Override
        @SuppressWarnings("resource") // No kernel is held yet.
        public PooledObject<PythonKernelOrExceptionHolder> makeObject(final PythonCommandAndModules key) {
            final PythonKernelOrExceptionHolder holder = new PythonKernelOrExceptionHolder();
            populateHolder(key, holder);
            return new DefaultPooledObject<>(holder);
        }

        @Override
        public void passivateObject(final PythonCommandAndModules key,
            final PooledObject<PythonKernelOrExceptionHolder> p) {
            @SuppressWarnings("resource") // No kernel is held yet or any more.
            final PythonKernelOrExceptionHolder holder = p.getObject();
            if (holder.m_kernel == null && holder.m_exception == null) {
                populateHolder(key, holder);
            }
        }

        private static void populateHolder(final PythonCommandAndModules key,
            final PythonKernelOrExceptionHolder holder) {
            try {
                holder.m_kernel = createKernel(key);
            } catch (final PythonIOException ex) {
                holder.m_exception = ex;
            }
        }

        private static PythonKernel createKernel(final PythonCommandAndModules key) throws PythonIOException {
            final PythonKernel kernel = new PythonKernel(key.m_command);
            try {
                loadAdditionalModules(key, kernel);
            } catch (final PythonIOException ex) {
                PythonUtils.Misc.closeSafelyThrowErrors(null, kernel);
                throw ex;
            } catch (final Exception ex) {
                PythonUtils.Misc.closeSafelyThrowErrors(null, kernel);
                throw new PythonIOException(ex);
            } catch (final Throwable t) {
                PythonUtils.Misc.closeSafelyThrowErrors(null, kernel);
                throw t;
            }
            return kernel;
        }

        private static void loadAdditionalModules(final PythonCommandAndModules key, final PythonKernel kernel)
            throws PythonIOException {
            final Set<PythonModuleSpec> requiredModules = key.m_requiredAdditionalModules;
            if (!requiredModules.isEmpty()) {
                PythonKernel.testInstallation(key.m_command, requiredModules);
                final String requiredModulesImportCode =
                    String.join("\n", Iterables.transform(requiredModules, m -> "import " + m.getName()));
                kernel.execute(requiredModulesImportCode);
            }
            final Set<PythonModuleSpec> optionalModules = key.m_optionalAdditionalModules;
            if (!optionalModules.isEmpty()) {
                final String optionalModulesImportCode = String.join("\n", Iterables.transform(optionalModules, m -> //
                /* */ "try:\n" //
                    + "\timport " + m.getName() + "\n" //
                    + "except Exception:\n" //
                    + "\tpass" //
                ));
                kernel.execute(optionalModulesImportCode);
            }
        }

        @Override
        public void destroyObject(final PythonCommandAndModules key,
            final PooledObject<PythonKernelOrExceptionHolder> p) {
            PythonUtils.Misc.closeSafelyThrowErrors(null, p.getObject());
        }

        @Override
        public boolean validateObject(final PythonCommandAndModules key,
            final PooledObject<PythonKernelOrExceptionHolder> p) {
            // Nothing to do.
            return true;
        }

        @Override
        public void activateObject(final PythonCommandAndModules key,
            final PooledObject<PythonKernelOrExceptionHolder> p) {
            // Nothing to do.
        }
    }

    private static final class PythonCommandAndModules {

        private final PythonCommand m_command;

        private final Set<PythonModuleSpec> m_requiredAdditionalModules;

        private final Set<PythonModuleSpec> m_optionalAdditionalModules;

        private final int m_hashCode;

        public PythonCommandAndModules(final PythonCommand command,
            final Set<PythonModuleSpec> requiredAdditionalModules,
            final Set<PythonModuleSpec> optionalAdditionalModules) {
            m_command = command;
            // Make defensive copies since instances of this class are used as keys in the underlying pool. Preserve
            // order of modules if this matters when loading them.
            m_requiredAdditionalModules = new LinkedHashSet<>(requiredAdditionalModules);
            m_optionalAdditionalModules = new LinkedHashSet<>(optionalAdditionalModules);
            m_hashCode = Objects.hash(command, requiredAdditionalModules, optionalAdditionalModules);
        }

        @Override
        public int hashCode() {
            return m_hashCode;
        }

        @Override
        public boolean equals(final Object obj) {
            if (!(obj instanceof PythonCommandAndModules)) {
                return false;
            }
            final PythonCommandAndModules other = (PythonCommandAndModules)obj;
            return other.m_command.equals(m_command) //
                && other.m_requiredAdditionalModules.equals(m_requiredAdditionalModules) //
                && other.m_optionalAdditionalModules.equals(m_optionalAdditionalModules);
        }
    }

    private static final class PythonKernelOrExceptionHolder implements AutoCloseable {

        private PythonKernel m_kernel;

        private PythonIOException m_exception;

        public PythonKernel clearFieldsAndReturnKernelOrThrow() throws PythonIOException {
            final PythonKernel kernel = m_kernel;
            m_kernel = null;
            final PythonIOException exception = m_exception;
            m_exception = null;
            if (exception != null) {
                throw exception;
            } else {
                return kernel;
            }
        }

        @Override
        public void close() throws PythonKernelCleanupException {
            if (m_kernel != null) {
                m_kernel.close();
            }
        }
    }
}
