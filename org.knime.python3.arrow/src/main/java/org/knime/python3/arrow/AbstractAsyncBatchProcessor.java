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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.SequentialBatchReadable;
import org.knime.core.columnar.batch.SequentialBatchReader;
import org.knime.core.columnar.cache.object.CountUpDownLatch;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Abstract baseclass that can process batches asynhronously as they come in.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
abstract class AbstractAsyncBatchProcessor implements AutoCloseable {

    private final Supplier<SequentialBatchReadable> m_batchReadableSupplier;

    private final ExecutorService m_threadPool;

    private final CountUpDownLatch m_countUpDownLatch = new CountUpDownLatch(1);

    protected AtomicBoolean m_stillRunning;

    protected final AtomicReference<IOException> m_invalidCause;

    private SequentialBatchReadable m_readable;

    private SequentialBatchReader m_reader;

    private Object m_terminationLock = new Object();

    protected AbstractAsyncBatchProcessor(final Supplier<SequentialBatchReadable> batchReadableSupplier,
        final int numThreads, final String threadPrefix) {
        m_batchReadableSupplier = batchReadableSupplier;
        m_invalidCause = new AtomicReference<>(null);
        m_threadPool = Executors.newFixedThreadPool(numThreads,
            new ThreadFactoryBuilder().setNameFormat(threadPrefix + "-%d").build());
        m_stillRunning = new AtomicBoolean(true);
    }

    /**
     * Called when the reader is lazily initialized when the first batch has arrived.
     */
    protected void lazyInit() {
    }

    protected SequentialBatchReader initReaderFromReadable(final SequentialBatchReadable readable) {
        return readable.createSequentialReader();
    }

    /**
     * This method is invoked for each batch, potentially in parallel for multiple batches. Should be implemented by
     * derived classes to perform the actual work.
     *
     * If an exception is thrown, this causes the processing of all other batches to stop as soon as possible.
     *
     * To support early termination, check m_stillRunning and m_invalidCause from time to time.
     *
     * @param batch The batch to process
     * @throws IOException If an exception occurred during processing the batch.
     */
    protected abstract void processNextBatchImpl(final ReadBatch batch) throws IOException;

    private synchronized void initReader() {
        if (m_reader == null) {
            m_readable = m_batchReadableSupplier.get();
            m_reader = initReaderFromReadable(m_readable);

            lazyInit();
        }
    }

    /**
     * Process the next batch. This starts the computation asynchronously.
     *
     * @throws IOException if checking the batch failed
     */
    public final synchronized void processNextBatch() throws IOException {
        final IOException invalidCause = m_invalidCause.get();
        if (invalidCause != null) {
            // Already invalid we do not need to check anymore
            throw invalidCause;
        }
        m_countUpDownLatch.countUp();
        m_threadPool.execute(this::asyncProcessNextBatch);
    }

    private void asyncProcessNextBatch() {
        ReadBatch readBatch = null;
        try {
            // Lazy reader initialization
            if (m_reader == null) {
                initReader();
            }

            // Read the next batch in a synchronized block, otherwise two threads might actually do that at the
            // same time and both receive the same batch.
            synchronized (m_reader) {
                readBatch = m_reader.forward();
            }
            processNextBatchImpl(readBatch);
        } catch (IOException ex) {
            m_invalidCause.set(ex);
        } catch (Exception ex) {
            m_invalidCause.set(new IOException("Error when processing batch", ex));
        } finally {
            if (readBatch != null) {
                readBatch.release();
            }
            m_countUpDownLatch.countDown();
        }
    }

    protected final void waitForTermination() throws InterruptedException {
        synchronized (m_terminationLock) {
            // Wait until all threads are done
            m_countUpDownLatch.countDown();
            m_countUpDownLatch.awaitInterrputibly();

            // Shutdown the thread pool
            m_threadPool.shutdown();
        }
    }

    @Override
    public final void close() throws IOException {
        // Stop threads
        m_stillRunning.set(false);

        synchronized (m_terminationLock) {
            // Wait until the threads are all done -> uninterruptible!
            m_countUpDownLatch.countDown();
            m_countUpDownLatch.await();

            // Shutdown the thread pool
            m_threadPool.shutdown();

            // Close the reader and readable
            if (m_reader != null) {
                m_reader.close();
                m_readable.close();
            }
        }
    }
}
