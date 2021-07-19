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
 *   May 6, 2021 (benjamin): created
 */
package org.knime.python3.arrow;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.knime.core.columnar.arrow.ArrowBatchReadStore;
import org.knime.core.columnar.arrow.ArrowBatchStore;
import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.columnar.arrow.ArrowReaderWriterUtils.OffsetProvider;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.python3.PythonDataCallback;
import org.knime.python3.PythonDataProvider;
import org.knime.python3.PythonEntryPoint;

/**
 * Utilities for handling {@link PythonDataProvider} and {@link PythonDataCallback} for Arrow data that needs to be
 * transfered to and from Python.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class PythonArrowDataUtils {

    private PythonArrowDataUtils() {
    }

    /**
     * Create a {@link PythonArrowDataProvider} that provides the data from the given {@link ArrowBatchStore}.
     *
     * @param store the store which holds the data
     * @param numBatches the total number of batches that are available at the store
     * @return the {@link PythonArrowDataProvider} that can be given to a {@link PythonEntryPoint} and will be wrapped
     *         into a Python object for easy access to the data
     */
    public static PythonArrowDataProvider createProvider(final ArrowBatchStore store, final int numBatches) {
        return new PythonArrowBatchStoreDataProvider(store.getPath().toAbsolutePath().toString(),
            store.getOffsetProvider(), numBatches);
    }

    /**
     * Create a {@link PythonArrowDataProvider} that provides the data from the given {@link ArrowBatchReadStore}
     *
     * @param store the store which holds the data
     * @return the {@link PythonArrowDataProvider} that can be given to a {@link PythonEntryPoint} and will be wrapped
     *         into a Python object for easy access to the data
     */
    public static PythonArrowDataProvider createProvider(final ArrowBatchReadStore store) {
        return new PythonArrowBatchStoreDataProvider(store.getPath().toAbsolutePath().toString(), null,
            store.numBatches());
    }

    /**
     * Create an {@link PythonArrowDataCallback} that writes an Arrow file to the given path.
     *
     * @param targetPath the path to write the Arrow file to
     * @return a {@link PythonArrowDataCallback} that can be given to a {@link PythonEntryPoint} and will be wrapped
     *         into a Python object for easy access for setting the data
     */
    public static DefaultPythonArrowDataCallback createCallback(final Path targetPath) {
        return new DefaultPythonArrowDataCallback(targetPath);
    }

    /**
     * Create a {@link RandomAccessBatchReadable} that provides batches from the data written by the Python process to
     * the given {@link PythonArrowDataCallback}.
     *
     * @param callback the callback which was given to the Python process
     * @param storeFactory an {@link ArrowColumnStoreFactory} to create the readable
     * @return the {@link RandomAccessBatchReadable} with the data
     */
    public static RandomAccessBatchReadable createReadable(final DefaultPythonArrowDataCallback callback,
        final ArrowColumnStoreFactory storeFactory) {
        // TODO Do not require DefaultPythonArrowDataCallback but an interface
        return storeFactory.createPartialFileReadable(callback.getSchema(), callback.getPath(), getOffsetProvider(callback));
    }

    /**
     * Create a {@link RandomAccessBatchReadable} that provides batches from the data written by the Python process to
     * the given {@link PythonArrowDataCallback} and check that the data has a specific schema.
     *
     * @param callback the callback which was given to the Python process
     * @param expectedSchema the expected schema
     * @param storeFactory an {@link ArrowColumnStoreFactory} to create the readable
     * @return the {@link RandomAccessBatchReadable} with the data
     * @throws IllegalStateException if Python did not report data with the expected schema to the callback
     */
    public static RandomAccessBatchReadable createReadable(final DefaultPythonArrowDataCallback callback,
        final ColumnarSchema expectedSchema, final ArrowColumnStoreFactory storeFactory) {
        // TODO Do not require DefaultPythonArrowDataCallback but an interface
        // TODO(extensiontypes) we need an expected schema with virtual types/extension types
        checkSchema(callback.getSchema(), expectedSchema);
        return createReadable(callback, storeFactory);
    }

    private static OffsetProvider getOffsetProvider(final DefaultPythonArrowDataCallback callback) {
        return new OffsetProvider() {

            @Override
            public long getRecordBatchOffset(final int index) {
                return callback.getRecordBatchOffsets().get(index);
            }

            @Override
            public long[] getDictionaryBatchOffsets(final int index) {
                // TODO support dictionary batches
                return new long[0];
            }
        };
    }

    /** Check if the given schema fits the expected schema. Throws an {@link IllegalStateException} if not. */
    private static void checkSchema(final ColumnarSchema schema, final ColumnarSchema expectedSchema) {
        if (schema.numColumns() != expectedSchema.numColumns()) {
            // TODO test this
            throw new IllegalStateException("The schema by Python does not have the same amount of columns. "
                + "Expected " + expectedSchema.numColumns() + ", got " + schema.numColumns() + ".");
        }
        for (int i = 0; i < schema.numColumns(); i++) {
            final DataSpec expectedSpec = expectedSchema.getSpec(i);
            final DataSpec spec = schema.getSpec(i);
            if (!spec.equals(expectedSpec)) {
                // TODO test this
                throw new IllegalStateException("The schema by Python does not have the specs in column " + i + ". "
                    + "Expected " + expectedSpec + ", got " + spec + ".");
            }
        }
    }

    private static final class PythonArrowBatchStoreDataProvider implements PythonArrowDataProvider {

        private final String m_path;

        private final OffsetProvider m_offsetProvider;

        private final int m_numBatches;

        public PythonArrowBatchStoreDataProvider(final String path, final OffsetProvider offsetProvider,
            final int numBatches) {
            m_path = path;
            m_offsetProvider = offsetProvider;
            m_numBatches = numBatches;
        }

        @Override
        public String getAbsolutePath() {
            return m_path;
        }

        @Override
        public int numBatches() {
            return m_numBatches;
        }

        @Override
        public boolean isFooterWritten() {
            return m_offsetProvider == null;
        }

        @Override
        public long getRecordBatchOffset(final int index) {
            return m_offsetProvider.getRecordBatchOffset(index);
        }

        @Override
        public List<Long> getDictionaryBatchOffsets(final int index) {
            return Arrays.stream(m_offsetProvider.getDictionaryBatchOffsets(index)).boxed()
                .collect(Collectors.toList());
        }
    }
}