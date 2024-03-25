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
 *   Nov 5, 2021 (Carsten Haubold, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.arrow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.knime.core.table.schema.DataSpecs.DOUBLE;
import static org.knime.core.table.schema.DataSpecs.LONG;
import static org.knime.core.table.schema.DataSpecs.STRING;

import java.io.IOException;
import java.nio.file.Files;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Before;
import org.junit.Test;
import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.columnar.arrow.compress.ArrowCompressionUtil;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.SequentialBatchReader;
import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.DoubleData.DoubleWriteData;
import org.knime.core.columnar.data.LongData.LongReadData;
import org.knime.core.columnar.data.LongData.LongWriteData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.data.columnar.table.DefaultColumnarBatchStore.ColumnarBatchStoreBuilder;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DoubleDataSpec;
import org.knime.core.table.schema.LongDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.python3.arrow.TestUtils.SinkCreator;
import org.knime.python3.testing.Python3ArrowTestUtils;

/**
 * Tests sending data to a KNIME Table and receiving data from it.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public class KnimeTableTest {
    private ArrowColumnStoreFactory m_storeFactory;

    private BufferAllocator m_allocator; // NOSONAR

    /** Create allocator and storeFactory */
    @Before
    public void before() {
        m_allocator = new RootAllocator();
        m_storeFactory = new ArrowColumnStoreFactory(m_allocator, ArrowCompressionUtil.ARROW_NO_COMPRESSION);
    }

    /**
     * Test copying a table through the Python KNIME DataFrame API
     *
     * @throws IOException
     * @throws InterruptedException
     **/
    @Test
    public void test() throws IOException, InterruptedException {

        final var schema = ColumnarSchema.of(STRING, LONG, DOUBLE);

        final var numBatches = 50;
        final var numRowsPerBatch = 200_000;
        final var modes = new String[]{"arrow-sentinel", "arrow", "pandas", "dict"};

        for (final var mode : modes) {
            final var sourcePath = Python3ArrowTestUtils.createTmpKNIMEArrowFileHandle();
            final var sinkPath = Python3ArrowTestUtils.createTmpKNIMEArrowFileHandle();

            final SinkCreator sinkCreator = () -> PythonArrowDataUtils.createSink(sinkPath.asPath());

            // Open connection to Python
            try (final var pythonGateway = TestUtils.openPythonGateway()) {
                final var entryPoint = pythonGateway.getEntryPoint();

                // Test using a write store -> footer not yet written
                // We need to wrap the store in a columnar store to enable dictionary encoding.
                try (final var arrowStore = m_storeFactory.createStore(schema, sourcePath);
                        final var store = new ColumnarBatchStoreBuilder(arrowStore)//
                            .enableDictEncoding(true)//
                            .build()) {

                    final var dataSource = PythonArrowDataUtils.createSource(arrowStore, numBatches);

                    // Write some batches
                    try (final BatchWriter writer = store.getWriter()) {
                        for (int b = 0; b < numBatches; b++) { // NOSONAR
                            writeBatch(schema, b, numRowsPerBatch, writer, mode.equals("arrow-sentinel"));
                            // TODO: call Python here and make sure it blocks until all batches are written?!
                        }
                    } // <- Footer is written here

                    long startTime = System.nanoTime();

                    final var dataSink = (DefaultPythonArrowDataSink)entryPoint.testKnimeTable(dataSource, sinkCreator,
                        (long)numBatches * numRowsPerBatch, schema.numColumns(), mode);

                    long endTime = System.nanoTime();
                    long duration = (endTime - startTime);
                    System.out.println("Copying " + numBatches + " batches of " + numRowsPerBatch + " each took "
                        + (duration / 1_000_000) + "ms using mode '" + mode + "'");

                    try (final var readable = PythonArrowDataUtils.createReadable(dataSink, m_storeFactory);
                            final var reader = readable.createSequentialReader()) {
                        assertEquals(schema, readable.getSchema());

                        for (int b = 0; b < numBatches; b++) { // NOSONAR
                            checkNextBatch(schema, b, numRowsPerBatch, reader, mode.equals("arrow-sentinel"));
                        }
                    }
                }
            } finally {
                Files.deleteIfExists(sourcePath.asPath());
                Files.deleteIfExists(sinkPath.asPath());
            }
        }
    }

    private static void writeBatch(final ColumnarSchema schema, final int batchIdx, final int numRows,
        final BatchWriter writer, final boolean missingLongs) throws IOException {
        final var batch = writer.create(numRows);

        for (int col = 0; col < schema.numColumns(); col++) { // NOSONAR
            final var data = batch.get(col);

            if (schema.getSpec(col) == LongDataSpec.INSTANCE) {
                fillLongData((LongWriteData)data, batchIdx, numRows, missingLongs);
            } else if (schema.getSpec(col) == DoubleDataSpec.INSTANCE) {
                fillDoubleData((DoubleWriteData)data, batchIdx, numRows);
            } else if (schema.getSpec(col) == StringDataSpec.INSTANCE) {
                fillStringData((StringWriteData)data, batchIdx, numRows);
            }
        }

        // Write data
        final var readBatch = batch.close(numRows);
        writer.write(readBatch);
        readBatch.release();
    }

    private static void fillLongData(final LongWriteData data, final int batchIdx, final int numRows,
        final boolean missingLongs) {
        for (int r = 0; r < numRows; r++) { // NOSONAR
            if (missingLongs && (batchIdx * numRows + r) % 13 == 0) {
                data.setMissing(r);
            } else {
                data.setLong(r, batchIdx * numRows + r);
            }
        }
    }

    private static void fillDoubleData(final DoubleWriteData data, final int batchIdx, final int numRows) {
        for (int r = 0; r < numRows; r++) { // NOSONAR
            data.setDouble(r, batchIdx * numRows + r); // NOSONAR
        }
    }

    private static void fillStringData(final StringWriteData data, final int batchIdx, final int numRows) {
        for (int r = 0; r < numRows; r++) { // NOSONAR
            data.setString(r, String.valueOf(batchIdx * numRows + r));
        }
    }

    private static void checkNextBatch(final ColumnarSchema schema,
        final int batchIdx, final int numRows,
        final SequentialBatchReader reader, final boolean missingLongs) throws IOException {
        final var batch = reader.forward();

        for (int col = 0; col < schema.numColumns(); col++) { // NOSONAR
            final var data = batch.get(col);

            if (schema.getSpec(col) == LongDataSpec.INSTANCE) {
                checkLongData((LongReadData)data, batchIdx, numRows, missingLongs);
            } else if (schema.getSpec(col) == DoubleDataSpec.INSTANCE) {
                checkDoubleData((DoubleReadData)data, batchIdx, numRows);
            } else if (schema.getSpec(col) == StringDataSpec.INSTANCE) {
                checkStringData((StringReadData)data, batchIdx, numRows);
            }
        }

        batch.release();
    }

    private static void checkLongData(final LongReadData data, final int batchIdx, final int numRows,
        final boolean missingLongs) {
        for (int r = 0; r < numRows; r++) { // NOSONAR
            if (missingLongs && (batchIdx * numRows + r) % 13 == 0) {
                assertTrue(data.isMissing(r));
            } else {
                assertEquals(2 * (batchIdx * numRows + r), data.getLong(r));
            }

        }
    }

    private static void checkDoubleData(final DoubleReadData data, final int batchIdx, final int numRows) {
        for (int r = 0; r < numRows; r++) { // NOSONAR
            assertEquals(2 * (batchIdx * numRows + r), data.getDouble(r), 0.00001); // NOSONAR
        }
    }

    private static void checkStringData(final StringReadData data, final int batchIdx, final int numRows) {
        for (int r = 0; r < numRows; r++) { // NOSONAR
            assertEquals(String.valueOf(batchIdx * numRows + r), data.getString(r));
        }
    }
}
