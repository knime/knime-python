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
 *   May 24, 2018 (marcel): created
 */
package org.knime.python2.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.knime.core.util.ThreadUtils;
import org.knime.python2.kernel.PythonCancelable;
import org.knime.python2.kernel.PythonCanceledExecutionException;
import org.knime.python2.kernel.PythonExecutionException;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class PythonUtils {

    private PythonUtils() {
        // utility class
    }

    /**
     * Utility class that helps checking whether the preconditions of a method or constructor invocation have been met
     * by the caller. <br>
     * This class complements the functionality of {@link com.google.common.base.Preconditions}.
     */
    public static class Preconditions {

        private Preconditions() {
        }

        /**
         * Ensures that a string passed as a parameter to the calling method is not null or empty.
         *
         * @param string a string
         * @return the non-null and non-empty reference that was validated
         * @throws NullPointerException if the input is null
         * @throws IllegalArgumentException if the input is empty
         * @see com.google.common.base.Preconditions#checkNotNull(Object)
         */
        public static String checkNotNullOrEmpty(final String string) {
            if (string == null) {
                throw new NullPointerException();
            }
            if (string.isEmpty()) {
                throw new IllegalArgumentException();
            }
            return string;
        }

        /**
         * Ensures that a string passed as a parameter to the calling method is not null or empty.
         *
         * @param string a string
         * @param errorMessage the exception message to use if the check fails; will be converted to a string using
         *            {@link String#valueOf(Object)}
         * @return the non-null and non-empty reference that was validated
         * @throws NullPointerException if the input is null
         * @throws IllegalArgumentException if the input is empty
         * @see com.google.common.base.Preconditions#checkNotNull(Object, Object)
         */
        public static String checkNotNullOrEmpty(final String string, final Object errorMessage) {
            if (string == null) {
                throw new NullPointerException(String.valueOf(errorMessage));
            }
            if (string.isEmpty()) {
                throw new IllegalArgumentException(String.valueOf(errorMessage));
            }
            return string;
        }

        /**
         * Ensures that a collection passed as a parameter to the calling method is not null or empty.
         *
         * @param collection a collection
         * @return the non-null and non-empty reference that was validated
         * @throws NullPointerException if the input is null
         * @throws IllegalArgumentException if the input is empty
         * @see com.google.common.base.Preconditions#checkNotNull(Object)
         */
        public static <C extends Collection<?>> C checkNotNullOrEmpty(final C collection) {
            if (collection == null) {
                throw new NullPointerException();
            }
            if (collection.isEmpty()) {
                throw new IllegalArgumentException();
            }
            return collection;
        }

        /**
         * Ensures that a collection passed as a parameter to the calling method is not null or empty.
         *
         * @param collection a collection
         * @param errorMessage the exception message to use if the check fails; will be converted to a string using
         *            {@link String#valueOf(Object)}
         * @return the non-null and non-empty reference that was validated
         * @throws NullPointerException if the input is null
         * @throws IllegalArgumentException if the input is empty
         * @see com.google.common.base.Preconditions#checkNotNull(Object, Object)
         */
        public static <C extends Collection<?>> C checkNotNullOrEmpty(final C collection, final Object errorMessage) {
            if (collection == null) {
                throw new NullPointerException(String.valueOf(errorMessage));
            }
            if (collection.isEmpty()) {
                throw new IllegalArgumentException(String.valueOf(errorMessage));
            }
            return collection;
        }
    }

    public static class Misc {

        private static final AtomicLong UNIQUE_THREAD_ID = new AtomicLong();

        private Misc() {
        }

        /**
         * @param exceptionConsumer may be <code>null</code>. If non-<code>null</code>, is used to report exceptions
         *            that occur while closing the individual given closeables.
         * @param closeables the resources to {@link AutoCloseable#close() close}. May be <code>null</code>, individual
         *            closeables may be <code>null</code>.
         * @return an error if any occurred, <code>null</code> otherwise. If non-<code>null</code>, it is recommended to
         *         rethrow that error. If multiple errors occurred, the first one is returned and the others are added
         *         as {@link Error#addSuppressed(Throwable) suppressed} if possible.
         *
         */
        public static Error closeSafely(final BiConsumer<String, Exception> exceptionConsumer,
            final AutoCloseable... closeables) {
            if (closeables == null) {
                return null;
            }
            return closeSafely(exceptionConsumer, Arrays.asList(closeables));
        }

        /**
         * @param exceptionConsumer may be <code>null</code>. If non-<code>null</code>, is used to report exceptions
         *            that occur while closing the individual given closeables.
         * @param closeables the resources to {@link AutoCloseable#close() close}. May be <code>null</code>, individual
         *            closeables may be <code>null</code>.
         * @return an error if any occurred, <code>null</code> otherwise. If non-<code>null</code>, it is recommended to
         *         rethrow that error. If multiple errors occurred, the first one is returned and the others are added
         *         as {@link Error#addSuppressed(Throwable) suppressed} if possible.
         *
         */
        public static Error closeSafely(final BiConsumer<String, Exception> exceptionConsumer,
            final Iterable<? extends AutoCloseable> closeables) {
            return invokeSafely(exceptionConsumer, closeable -> {
                try {
                    closeable.close();
                } catch (final Exception ex) {
                    throw new RuntimeException(ex.getMessage(), ex);
                }
            }, closeables);
        }

        /**
         * Runs a task in a worker thread and blocks until the task is finished, an exception occurs, or execution is
         * canceled. The worker thread is {@link Thread#interrupt() interrupted} if a cancellation occurs.
         *
         * @param callable the task to run cancelable
         * @param cancelable the cancelable to check for cancellation
         * @return the result of the task, if any
         * @throws PythonExecutionException if any exception occurred during execution
         * @throws PythonCanceledExecutionException if canceled
         */
        public static <T> T executeCancelable(final Callable<T> callable, final PythonCancelable cancelable)
            throws PythonExecutionException, PythonCanceledExecutionException {
            final Thread clientThread = Thread.currentThread();
            final AtomicBoolean done = new AtomicBoolean(false);
            final AtomicReference<T> output = new AtomicReference<>();
            final AtomicReference<Exception> exception = new AtomicReference<>(null);
            // Worker thread.
            Thread worker = ThreadUtils.threadWithContext(() -> {
                try {
                    output.set(callable.call());
                } catch (final Exception ex) {
                    exception.set(ex);
                }
                done.set(true);
                // Wake up the waiting client thread.
                clientThread.interrupt();
            }, "KNIME-Python-Worker-" + UNIQUE_THREAD_ID.incrementAndGet());
            worker.start();
            // Wait until worker thread is done or execution is cancelled.
            while (!done.get()) {
                try {
                    Thread.sleep(1000);
                } catch (final InterruptedException ex) {
                    if (!done.get()) {
                        Thread.currentThread().interrupt();
                    }
                    // Else we were most likely interrupted by the worker thread.
                }
                try {
                    cancelable.checkCanceled();
                } catch (PythonCanceledExecutionException ex) {
                    worker.interrupt();
                    throw ex;
                }
            }
            // If there was an exception in the worker thread, throw it here.
            Exception ex = exception.get();
            if (ex != null) {
                if (ex instanceof PythonCanceledExecutionException) {
                    // May happen if the executed task checks for cancellation itself.
                    throw (PythonCanceledExecutionException)ex;
                } else {
                    throw new PythonExecutionException(ex.getMessage(), ex);
                }
            }
            return output.get();
        }

        /**
         * @param exceptionConsumer may be <code>null</code>. If non-<code>null</code>, is used to report exceptions
         *            that occur while invoking the given method on the individual given invokees.
         * @param method the method to invoke, may be <code>null</code>
         * @param invokees the objects on which to invoke the given method. May be <code>null</code>, individual objects
         *            may be <code>null</code>.
         * @return an error if any occurred, <code>null</code> otherwise. If non-<code>null</code>, it is recommended to
         *         rethrow that error. If multiple errors occurred, the first one is returned and the others are added
         *         as {@link Error#addSuppressed(Throwable) suppressed} if possible.
         */
        @SafeVarargs
        public static <T> Error invokeSafely(final BiConsumer<String, Exception> exceptionConsumer,
            final Consumer<? super T> method, final T... invokees) {
            if (invokees == null) {
                return null;
            }
            return invokeSafely(exceptionConsumer, method, Arrays.asList(invokees));
        }

        /**
         * @param exceptionConsumer may be <code>null</code>. If non-<code>null</code>, is used to report exceptions
         *            that occur while invoking the given method on the individual given invokees.
         * @param method the method to invoke, may be <code>null</code>
         * @param invokees the objects on which to invoke the given method. May be <code>null</code>, individual objects
         *            may be <code>null</code>.
         * @return an error if any occurred, <code>null</code> otherwise. If non-<code>null</code>, it is recommended to
         *         rethrow that error. If multiple errors occurred, the first one is returned and the others are added
         *         as {@link Error#addSuppressed(Throwable) suppressed} if possible.
         */
        public static <T> Error invokeSafely(final BiConsumer<String, Exception> exceptionConsumer,
            final Consumer<? super T> method, final Iterable<? extends T> invokees) {
            if (method == null || invokees == null) {
                return null;
            }
            Error error = null;
            for (final T i : invokees) {
                try {
                    if (i != null) {
                        method.accept(i);
                    }
                } catch (final Exception ex) {
                    if (exceptionConsumer != null) {
                        try {
                            exceptionConsumer.accept("An exception occurred while safely invoking a method."
                                + (ex.getMessage() != null ? " Exception: " + ex.getMessage() : ""), ex);
                        } catch (final Exception ignore) {
                            // ignore
                        } catch (final Error e) {
                            // Error will be handled by caller.
                            if (error == null) {
                                error = e;
                            } else {
                                error.addSuppressed(e);
                            }
                        }
                    }
                } catch (final Error e) {
                    // Error will be handled by caller.
                    if (error == null) {
                        error = e;
                    } else {
                        error.addSuppressed(e);
                    }
                }
            }
            return error;
        }
    }
}
