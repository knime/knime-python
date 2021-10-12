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
 *   Sep 29, 2021 (benjamin): created
 */
package org.knime.python3.arrow;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.SequentialBatchReadable;
import org.knime.core.columnar.batch.SequentialBatchReader;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.filter.FilteredColumnSelection;
import org.knime.core.util.DuplicateChecker;
import org.knime.core.util.DuplicateKeyException;

/**
 * A checker that can check if keys are unique in a {@link RandomAccessBatchReadable} or
 * {@link SequentialBatchReadable}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class RowKeyChecker implements AutoCloseable {

    private final Supplier<SequentialBatchReadable> m_batchReadableSupplier;

    private final DuplicateChecker m_duplicateChecker;

    private final ExecutorService m_threadPool;

    private final Phaser m_phaser;

    private AtomicBoolean m_stillRunning;

    private final AtomicReference<Exception> m_invalidCause;

    private SequentialBatchReadable m_readable;

    private SequentialBatchReader m_reader;

    /**
     * Create a new {@link RowKeyChecker} that will get the batches from the {@link SequentialBatchReadable} supplied by
     * the given {@link Supplier}. The {@link Supplier} is called when the first batch is checked.
     *
     * @param batchReadableSupplier a {@link Supplier} for the {@link SequentialBatchReadable}. Only called once and the
     *            {@link SequentialBatchReadable} is closed with {@link RowKeyChecker#close()}.
     * @return A {@link RowKeyChecker} that checks the keys
     */
    public static RowKeyChecker fromSequentialReadable(final Supplier<SequentialBatchReadable> batchReadableSupplier) {
        return new RowKeyChecker(batchReadableSupplier);
    }

    /**
     * Create a new {@link RowKeyChecker} that will get the batches from the {@link RandomAccessBatchReadable} supplied
     * by the given {@link Supplier}. The {@link Supplier} is called when the first batch is checked. The batches are
     * accessed in sequential order starting with batch 0.
     *
     * @param batchReadableSupplier a {@link Supplier} for the {@link RandomAccessBatchReadable}. Only called once and
     *            the {@link RandomAccessBatchReadable} is closed with {@link RowKeyChecker#close()}.
     * @return A {@link RowKeyChecker} that checks the keys
     */
    public static RowKeyChecker
        fromRandomAccessReadable(final Supplier<RandomAccessBatchReadable> batchReadableSupplier) {
        return fromSequentialReadable(() -> new RandomAccessAsSequentialBatchReadable(batchReadableSupplier.get()));
    }

    private RowKeyChecker(final Supplier<SequentialBatchReadable> batchReadableSupplier) {
        m_batchReadableSupplier = batchReadableSupplier;
        m_duplicateChecker = new DuplicateChecker();

        m_invalidCause = new AtomicReference<>(null);

        m_threadPool = Executors.newFixedThreadPool(2);
        m_phaser = new Phaser(1);
        m_stillRunning = new AtomicBoolean(true);
    }

    private synchronized void initReader() {
        if (m_reader == null) {
            m_readable = m_batchReadableSupplier.get();
            m_reader =
                m_readable.createSequentialReader(new FilteredColumnSelection(m_readable.getSchema().numColumns(), 0));
        }
    }

    /**
     * Check for duplicate keys in the next batch. This starts the check asynchronously. The result can be accessed via
     * {@link #isValid()} and {@link #allUnique()}.
     *
     * @throws DuplicateKeyException if this {@link RowKeyChecker} already encountered duplicate keys
     * @throws IOException if checking the batch failed
     */
    public void checkNextBatch() throws DuplicateKeyException, IOException {
        final Exception invalidCause = m_invalidCause.get();
        if (invalidCause != null) {
            // Already invalid we do not need to check anymore
            if (invalidCause instanceof DuplicateKeyException) {
                throw (DuplicateKeyException)invalidCause;
            } else {
                throw (IOException)invalidCause;
            }
        }
        m_threadPool.execute(this::checkNextBatchRunner);
        m_phaser.register();
    }

    private void checkNextBatchRunner() {
        // Init the reader if it is not yet initialized
        if (m_reader == null) {
            initReader();
        }

        ReadBatch readBatch = null;
        try {
            // Read the next batch
            readBatch = m_reader.forward();
            final StringReadData rowKeys = (StringReadData)readBatch.get(0);

            // Loop over rows and add them to the duplicate checker
            for (int i = 0; // NOSONAR
                    i < rowKeys.length() //
                        && m_stillRunning.get() // Not stopped by close
                        && m_invalidCause.get() == null // Not already invalid
                    ; i++) {
                m_duplicateChecker.addKey(rowKeys.getString(i));
            }
        } catch (final DuplicateKeyException | IOException e) {
            m_invalidCause.set(e);
        } finally {
            if (readBatch != null) {
                readBatch.release();
            }
            m_phaser.arriveAndDeregister();
        }
    }

    /**
     * @return true if no duplicate keys were found until now. Note that duplicate keys could exist.
     */
    public boolean isValid() {
        return m_invalidCause.get() == null;
    }

    /**
     * Waits until all checks are finished and returns if all keys are still unique. Must be called after the checker
     * has been notified about all batches. Notifying the checker about new batches after this method is called will
     * lead to unexpected results.
     *
     * @return true if all keys are unique
     * @throws InterruptedException if checking the keys got interrupted
     */
    public boolean allUnique() throws InterruptedException {
        // Wait until all threads are done
        if (!m_phaser.isTerminated()) {
            final var phase = m_phaser.getPhase();
            m_phaser.arriveAndDeregister();
            m_phaser.awaitAdvanceInterruptibly(phase);
        }

        // Shutdown the thread pool
        m_threadPool.shutdown();

        // Do the final duplicate checking
        if (isValid()) {
            try {
                m_duplicateChecker.checkForDuplicates();
                return true;
            } catch (final DuplicateKeyException | IOException e) {
                m_invalidCause.set(e);
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    public void close() throws Exception {
        // Stop threads
        m_stillRunning.set(false);

        // Wait until the threads are all done
        if (!m_phaser.isTerminated()) {
            final var phase = m_phaser.getPhase();
            m_phaser.arriveAndDeregister();
            m_phaser.awaitAdvance(phase);
        }

        // Shutdown the thread pool
        m_threadPool.shutdown();

        // Close the reader and readable
        if (m_reader != null) {
            m_reader.close();
            m_readable.close();
        }
    }
}
