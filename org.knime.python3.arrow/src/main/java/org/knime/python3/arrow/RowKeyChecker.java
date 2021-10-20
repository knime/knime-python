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
public final class RowKeyChecker extends AbstractAsyncBatchProcessor {

    private final DuplicateChecker m_duplicateChecker;

    private static final int NUM_THREADS = 2;

    private static final int ROW_KEY_COL_IDX = 0;

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
        super(batchReadableSupplier, NUM_THREADS);
        m_duplicateChecker = new DuplicateChecker();
    }

    @Override
    protected SequentialBatchReader initReaderFromReadable(final SequentialBatchReadable readable) {
        return readable.createSequentialReader(new FilteredColumnSelection(readable.getSchema().numColumns(), ROW_KEY_COL_IDX));
    }

    @Override
    protected void processNextBatchImpl(final ReadBatch batch) throws IOException {
        final StringReadData rowKeys = (StringReadData)batch.get(0);

        try {
            // Loop over rows and add them to the duplicate checker
            for (int i = 0; // NOSONAR
                    i < rowKeys.length() //
                        && m_stillRunning.get() // Not stopped by close
                        && m_invalidCause.get() == null // Not already invalid
                    ; i++) {
                m_duplicateChecker.addKey(rowKeys.getString(i));
            }
        } catch (DuplicateKeyException e) {
            throw new IOException(e);
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
        waitForTermination();

        // Do the final duplicate checking
        if (isValid()) {
            try {
                m_duplicateChecker.checkForDuplicates();
                return true;
            } catch (final IOException e) {
                m_invalidCause.set(e);
                return false;
            } catch (final DuplicateKeyException e) {
                m_invalidCause.set(new IOException(e));
                return false;
            }
        } else {
            return false;
        }
    }
}
