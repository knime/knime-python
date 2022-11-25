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

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.SequentialBatchReadable;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.columnar.domain.ColumnarDomainCalculator;
import org.knime.core.data.columnar.domain.DomainWritableConfig;
import org.knime.core.data.meta.DataColumnMetaData;
import org.knime.python3.arrow.PythonArrowDataUtils.TableDomainAndMetadata;

/**
 * Perform batch-wise domain calculation on a {@link RandomAccessBatchReadable} or {@link SequentialBatchReadable}.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
final class DomainCalculator extends AbstractAsyncBatchProcessor {

    private final Supplier<DomainWritableConfig> m_configSupplier;

    // we cannot use more than one thread for updating the domains as the update() method of
    // the domain calculators is not thread safe.
    private static final int NUM_THREADS = 1;

    private Map<Integer, ColumnarDomainCalculator<? extends NullableReadData, DataColumnDomain>> m_domainCalculators;

    private Map<Integer, ColumnarDomainCalculator<? extends NullableReadData, DataColumnMetaData[]>> m_metadataCalculators;

    /**
     * Create a new DomainCalculator that will get the batches from the {@link SequentialBatchReadable} supplied
     * by the given {@link Supplier}. The {@link Supplier} is called when the first batch is checked.
     *
     * @param batchReadableSupplier a {@link Supplier} for the {@link SequentialBatchReadable}. Only called once and the
     *            {@link SequentialBatchReadable} is closed with DomainCalculator#close().
     * @param configSupplier A {@link Supplier} for the {@link DomainWritableConfig} specifying which domains should be
     *            calculated and how
     * @return A DomainCalculator that calculates domains of the columns in the {@link SequentialBatchReadable}.
     */
    public static DomainCalculator fromSequentialReadable(final Supplier<SequentialBatchReadable> batchReadableSupplier,
        final Supplier<DomainWritableConfig> configSupplier) {
        return new DomainCalculator(batchReadableSupplier, configSupplier);
    }

    /**
     * Create a new DomainCalculator that will get the batches from the {@link RandomAccessBatchReadable}
     * supplied by the given {@link Supplier}. The {@link Supplier} is called when the first batch is checked. The
     * batches are accessed in sequential order starting with batch 0.
     *
     * @param batchReadableSupplier a {@link Supplier} for the {@link RandomAccessBatchReadable}. Only called once and
     *            the {@link RandomAccessBatchReadable} is closed with DomainCalculator#close().
     * @param configSupplier A {@link Supplier} for the link DomainWritableConfig} specifying which domains should be
     *            calculated and how
     * @return A DomainCalculator that calculates domains of the columns in the
     *         {@link RandomAccessBatchReadable}.
     */
    public static DomainCalculator fromRandomAccessReadable(
        final Supplier<RandomAccessBatchReadable> batchReadableSupplier,
        final Supplier<DomainWritableConfig> configSupplier) {
        return fromSequentialReadable(() -> new RandomAccessAsSequentialBatchReadable(batchReadableSupplier.get()),
            configSupplier);
    }

    private DomainCalculator(final Supplier<SequentialBatchReadable> batchReadableSupplier,
        final Supplier<DomainWritableConfig> configSupplier) {
        super(batchReadableSupplier, NUM_THREADS, "python-domain-batch-processor");
        m_configSupplier = configSupplier;
    }

    @Override
    protected void lazyInit() {
        final var config = m_configSupplier.get();
        m_domainCalculators = config.createDomainCalculators();
        m_metadataCalculators = config.createMetadataCalculators();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void processNextBatchImpl(final ReadBatch batch) {
        Stream.concat(m_domainCalculators.entrySet().stream(), m_metadataCalculators.entrySet().stream()).forEach(e -> {
            if (!m_stillRunning.get() || m_invalidCause.get() != null) {
                return; // stopped by close or encountered error before, so don't compute further domains
            }
            final NullableReadData data = batch.get(e.getKey());
            data.retain();
            ((ColumnarDomainCalculator<NullableReadData, ?>)e.getValue()).update(data);
            data.release();
        });
    }

    /**
     * Get the resulting {@link DataColumnDomain}.
     *
     * @param colIndex the columnIndex
     * @return the resulting domain, if present, otherwise null
     * @throws InterruptedException
     */
    public final DataColumnDomain getDomain(final int colIndex) throws InterruptedException {
        waitForTermination();
        return getDomainDontWait(colIndex);
    }

    /**
     * Get the resulting {@link DataColumnMetaData}.
     *
     * @param colIndex the columnIndex
     * @return the resulting metadata
     * @throws InterruptedException
     */
    public final DataColumnMetaData[] getMetadata(final int colIndex) throws InterruptedException {
        waitForTermination();
        return getMetadataDontWait(colIndex);
    }

    /**
     * Get the combined results as a {@link TableDomainAndMetadata} object.
     *
     * @return the resulting domain and metadata for the complete table
     * @throws InterruptedException
     */
    public final TableDomainAndMetadata getTableDomainAndMetadata() throws InterruptedException {
        waitForTermination();
        if (m_domainCalculators == null) {
            lazyInit();
        }
        return new TableDomainAndMetadata() {

            @Override
            public DataColumnDomain getDomain(final int colIndex) {
                return getDomainDontWait(colIndex);
            }

            @Override
            public DataColumnMetaData[] getMetadata(final int colIndex) {
                return getMetadataDontWait(colIndex);
            }
        };
    }

    private DataColumnDomain getDomainDontWait(final int colIndex) {
        if (m_domainCalculators != null) {
            final ColumnarDomainCalculator<?, DataColumnDomain> calculator = m_domainCalculators.get(colIndex);
            if (calculator != null) {
                return calculator.createDomain();
            }
        }
        return null;
    }

    private DataColumnMetaData[] getMetadataDontWait(final int colIndex) {
        if (m_metadataCalculators != null) {
            final ColumnarDomainCalculator<?, DataColumnMetaData[]> calculator = m_metadataCalculators.get(colIndex);
            if (calculator != null) {
                return calculator.createDomain();
            }
        }
        return new DataColumnMetaData[0];
    }
}
