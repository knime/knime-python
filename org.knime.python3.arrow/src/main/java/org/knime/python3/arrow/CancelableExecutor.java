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
 *   May 4, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.arrow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.NodeLogger;

/**
 * Allows to execute tasks in a cancelable manner.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class CancelableExecutor {

    /**
     * Allows to check if the execution is canceled.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public interface Cancelable {

        /**
         * Checks if the execution has been canceled.
         *
         * @throws CanceledExecutionException if the execution has been canceled
         */
        void checkCanceled() throws CanceledExecutionException;
    }

    private final ExecutorService m_executorService;

    /**
     * Constructs a {@link CancelableExecutor} that uses the provided {@link ExecutorService} for scheduling of tasks.
     * The life-cycle of the ExecutorService needs to be handled from the outside of this class.
     *
     * @param executorService for scheduling tasks
     */
    public CancelableExecutor(final ExecutorService executorService) {
        m_executorService = executorService;
    }

    /**
     * Performs all tasks in parallel and checks for cancellation while the tasks are completed.
     *
     * @param <T> the type of results
     * @param tasks to perform in parallel
     * @param cancelable used to check for cancellation of tasks
     * @return the results of all tasks
     * @throws CanceledExecutionException if execution is canceled
     * @throws ExecutionException if there occurs an exception during execution
     */
    public <T> List<T> performCancelable(final Collection<Callable<T>> tasks, final Cancelable cancelable)
        throws CanceledExecutionException, ExecutionException {
        var results = new ArrayList<T>(tasks.size());
        List<Future<T>> futures = tasks.stream()//
                .map(m_executorService::submit)//
                .collect(Collectors.toList());
        for (var future : futures) {
            try {
                results.add(getCancelable(future, cancelable));
            } catch (CanceledExecutionException | ExecutionException ex) {
                futures.forEach(f -> f.cancel(true));
                throw ex;
            }
        }
        return results;
    }

    private static <T> T getCancelable(final Future<T> future, final Cancelable cancelable) throws CanceledExecutionException, ExecutionException {
        while (true) {
            try {
                return future.get(1000, TimeUnit.MILLISECONDS);
            } catch (final TimeoutException ex) { // NOSONAR: Timeout is expected and part of control flow.
                cancelable.checkCanceled();
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                cancelable.checkCanceled();
            } catch (final CancellationException ex) {
                // Should not happen since the handle to the future is local to this method.
                NodeLogger.getLogger(CancelableExecutor.class).debug(ex);
                throw new CanceledExecutionException();
            }
        }
    }

    /**
     * Performs the task and checks for cancellation while the task is performed.
     *
     * @param <T> the type of result
     * @param task to perform
     * @param cancelable used to check for cancellation
     * @return the result of the task
     * @throws CanceledExecutionException if execution is cancelled
     * @throws ExecutionException if an exception occurs during the execution of the task
     */
    public <T> T performCancelable(final Callable<T> task, final Cancelable cancelable)
        throws CanceledExecutionException, ExecutionException {
        var future = m_executorService.submit(task);
        try {
            return getCancelable(future, cancelable);
        } catch (ExecutionException | CanceledExecutionException ex) {
            future.cancel(true);
            throw ex;
        }
    }

}
