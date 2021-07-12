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
 *   Apr 12, 2021 (benjamin): created
 */
package org.knime.python3.arrow;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.knime.core.columnar.arrow.ArrowBatchStore;
import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.columnar.arrow.compress.ArrowCompressionUtil;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.data.LocalDateData.LocalDateWriteData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.data.ZonedDateTimeData.ZonedDateTimeWriteData;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;

@SuppressWarnings("javadoc")
public class ExampleTests {

    private ArrowColumnStoreFactory m_storeFactory;

    private BufferAllocator m_allocator;

    @Before
    public void before() {
        m_allocator = new RootAllocator();
        m_storeFactory = new ArrowColumnStoreFactory(m_allocator, 0, m_allocator.getLimit(),
            ArrowCompressionUtil.ARROW_NO_COMPRESSION);
    }

    @After
    public void after() {
        m_allocator.close();
    }

    @Test
    public void testSimple() throws Exception {
        final var numRows = 100;
        final var numBatches = 5;

        try (final var store = createExampleStore(m_storeFactory, numRows, numBatches)) {

            // Open connection to Python
            try (final var pythonGateway = TestUtils.openPythonGateway()) {
                final var entryPoint = pythonGateway.getEntryPoint();

                // Define a PythonArrowDataProvider providing the data of the store
                final var dataProvider = PythonArrowDataUtils.createProvider(store, numBatches);

                // Define a PythonArrowDataCallback getting the data from Python
                final var outPath = TestUtils.createTmpKNIMEArrowPath();
                final var dataCallback = PythonArrowDataUtils.createCallback(outPath);

                // Call Python
                entryPoint.testSimpleComputation(dataProvider, dataCallback);

                // Read the data back
                try (final var readStore = PythonArrowDataUtils.createReadable(dataCallback, m_allocator)) {
                    checkReadable(readStore, numRows, numBatches);
                }
            }
        }
    }

    @Test
    public void testExpectedSchema() throws Exception {
        // TODO test failing
        final var numRows = 100;
        final var numBatches = 5;

        try (final var store = createExampleStore(m_storeFactory, numRows, numBatches)) {

            // Open connection to Python
            try (final var pythonGateway = TestUtils.openPythonGateway()) {
                final var entryPoint = pythonGateway.getEntryPoint();

                // Define a PythonArrowDataProvider providing the data of the store
                final var dataProvider = PythonArrowDataUtils.createProvider(store, numBatches);

                // Define a PythonArrowDataCallback getting the data from Python
                final var outPath = TestUtils.createTmpKNIMEArrowPath();
                final var expectedSchema =
                    new DefaultColumnarSchema(DataSpec.stringSpec(), DataSpec.intSpec(), DataSpec.intSpec());
                // final var expectedSchema =
                //    new DefaultColumnarSchema(DataSpec.stringSpec(), DataSpec.intSpec(), DataSpec.doubleSpec());
                final var dataCallback = PythonArrowDataUtils.createCallback(outPath);

                // Call Python
                entryPoint.testSimpleComputation(dataProvider, dataCallback);

                // Read the data back
                try (final var readStore =
                    PythonArrowDataUtils.createReadable(dataCallback, expectedSchema, m_allocator)) {
                    checkReadable(readStore, numRows, numBatches);
                }
            }
        }
    }

    @Test
    public void testProvideReadStore() throws Exception {
        final var numRows = 100;
        final var numBatches = 5;

        // Write some data using a WriteStore copy it to permanent storage to be
        // read using a ReadStore again
        final var path = TestUtils.createTmpKNIMEArrowPath();
        final ColumnarSchema schema;
        try (final var writeStore = createExampleStore(m_storeFactory, numRows, numBatches)) {
            // Save the file to "permanent" storage
            Files.copy(writeStore.getPath(), path, StandardCopyOption.REPLACE_EXISTING);
            schema = writeStore.getSchema();
        }

        // NOTE: store is now an ArrowBatchReadStore!
        try (final var store = m_storeFactory.createReadStore(schema, path)) {

            // Open connection to Python
            try (final var pythonGateway = TestUtils.openPythonGateway()) {
                final var entryPoint = pythonGateway.getEntryPoint();

                // Define a PythonArrowDataProvider providing the data of the store
                final var dataProvider = PythonArrowDataUtils.createProvider(store);

                // Define a PythonArrowDataCallback getting the data from Python
                final var outPath = TestUtils.createTmpKNIMEArrowPath();
                final var dataCallback = PythonArrowDataUtils.createCallback(outPath);

                // Call Python
                entryPoint.testSimpleComputation(dataProvider, dataCallback);

                // Read the data back
                try (final var readStore = PythonArrowDataUtils.createReadable(dataCallback, m_allocator)) {
                    checkReadable(readStore, numRows, numBatches);
                }
            }
        }
    }

    @Test
    @Ignore
    public void testBigTable() throws Exception {
        final var numRows = 100_000;
        final var numBatches = 200;

        long startTime = System.currentTimeMillis();
        try (final var store = createExampleStore(m_storeFactory, numRows, numBatches)) {
            System.out.println("Creating/Writing data took " + (System.currentTimeMillis() - startTime) + "ms");

            // Open connection to Python
            try (final var pythonGateway = TestUtils.openPythonGateway()) {
                final var entryPoint = pythonGateway.getEntryPoint();

                // Define a PythonArrowDataProvider providing the data of the store
                final var dataProvider = PythonArrowDataUtils.createProvider(store, numBatches);

                // Define a PythonArrowDataCallback getting the data from Python
                final var outPath = TestUtils.createTmpKNIMEArrowPath();
                final var dataCallback = PythonArrowDataUtils.createCallback(outPath);

                // Call Python
                startTime = System.currentTimeMillis();
                entryPoint.testSimpleComputation(dataProvider, dataCallback);
                System.out.println("Python computation took " + (System.currentTimeMillis() - startTime) + "ms");

                // Read the data back
                // try (final var readStore = dataCallback.createReadStore(
                //     m_allocator.newChildAllocator("ArrowColumnReadStore", 0, m_allocator.getLimit()))) {
                //     checkReadStore(readStore, numRows, numBatches);
                // }
            }
        }
    }

    @Test
    public void testMultipleInputsOutputs() throws Exception {
        final var numRows = 100;
        final var numBatches = 5;

        // Write some data using a WriteStore copy it to permanent storage to be
        // read using a ReadStore again
        final var path0 = TestUtils.createTmpKNIMEArrowPath();
        final ColumnarSchema schema;
        try (final var writeStore = createExampleStore(m_storeFactory, numRows, numBatches)) {
            // Save the file to "permanent" storage
            Files.copy(writeStore.getPath(), path0, StandardCopyOption.REPLACE_EXISTING);
            schema = writeStore.getSchema();
        }

        try (final var store0 = m_storeFactory.createReadStore(schema, path0);
                final var store1 = createExampleStore(m_storeFactory, numRows, numBatches);
                final var pythonGateway = TestUtils.openPythonGateway()) {

            final var entryPoint = pythonGateway.getEntryPoint();

            // Define a PythonArrowDataProvider providing the data of the store
            final var dataProvider0 = PythonArrowDataUtils.createProvider(store0);
            final var dataProvider1 = PythonArrowDataUtils.createProvider(store1, numBatches);

            // Define a PythonArrowDataCallback getting the data from Python
            final var outPath = TestUtils.createTmpKNIMEArrowPath();
            final var dataCallback = PythonArrowDataUtils.createCallback(outPath);

            // Call Python
            // TODO callbacks
            entryPoint.testMultipleInputsOutputs(List.of(dataProvider0, dataProvider1), null);

            // Read the data back
            //            try (final var readStore = dataCallback
            //                .createReadStore(m_allocator.newChildAllocator("ArrowColumnReadStore", 0, m_allocator.getLimit()))) {
            //                checkReadStore(readStore, numRows, numBatches);
            //            }
        }
    }

    @Test
    public void testDateTime() throws Exception {
        final var numRows = 100;
        final var numBatches = 5;

        final var path = TestUtils.createTmpKNIMEArrowPath();
        final var schema = new DefaultColumnarSchema(DataSpec.localDateSpec());
        try (final var store = m_storeFactory.createStore(schema, path)) {

            // Write some data to the store
            try (final BatchWriter writer = store.getWriter()) {
                for (int b = 0; b < numBatches; b++) {
                    final var batch = writer.create(numRows);
                    final var data = (LocalDateWriteData)batch.get(0);

                    // Fill data
                    for (int r = 0; r < numRows; r++) {
                        final var localDate = LocalDate.ofEpochDay(r + b);
                        data.setLocalDate(r, localDate);
                    }

                    // Write data
                    final var readBatch = batch.close(numRows);
                    writer.write(readBatch);
                    readBatch.release();
                }
            }

            // Open connection to Python
            try (final var pythonGateway = TestUtils.openPythonGateway()) {
                final var entryPoint = pythonGateway.getEntryPoint();

                // Define a PythonArrowDataProvider providing the data of the store
                final var dataProvider = PythonArrowDataUtils.createProvider(store, numBatches);

                // Call Python
                entryPoint.testLocalDate(dataProvider);
            }
        }
    }

    @Test
    @Ignore
    public void testZonedDateTime() throws Exception {
        // TODO
        // This requires dictionary encoding. In pyarrow we cannot read record batches which are dictionary encoded by offset
        // We should ask the Arrow Community for API:
        // pa.ipc.read_dictionary()
        final var numRows = 100;
        final var numBatches = 5;
        final List<String> availableZoneIds = new ArrayList<>(ZoneId.getAvailableZoneIds());

        final var path = TestUtils.createTmpKNIMEArrowPath();
        final var schema = new DefaultColumnarSchema(DataSpec.zonedDateTimeSpec());
        try (final var store = m_storeFactory.createStore(schema, path)) {

            // Write some data to the store
            try (final BatchWriter writer = store.getWriter()) {
                for (int b = 0; b < numBatches; b++) {
                    final var batch = writer.create(numRows);
                    final var data = (ZonedDateTimeWriteData)batch.get(0);

                    // Fill data
                    for (int r = 0; r < numRows; r++) {
                        final ZoneId zoneId = ZoneId.of(availableZoneIds.get(b % 3));
                        final LocalDate localDate = LocalDate.ofEpochDay(r + b);
                        final LocalTime localTime = LocalTime.of(r % 24, b % 60);
                        final LocalDateTime localDateTime = LocalDateTime.of(localDate, localTime);
                        final ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, zoneId);
                        data.setZonedDateTime(r, zonedDateTime);
                    }

                    // Write data
                    final var readBatch = batch.close(numRows);
                    writer.write(readBatch);
                    readBatch.release();
                }
            }

            // Open connection to Python
            try (final var pythonGateway = TestUtils.openPythonGateway()) {
                final var entryPoint = pythonGateway.getEntryPoint();

                // Define a PythonArrowDataProvider providing the data of the store
                final var dataProvider = PythonArrowDataUtils.createProvider(store, numBatches);

                // Call Python
                entryPoint.testZonedDateTime(dataProvider);
            }
        }
    }

    // ------------------- UTILITIES
    // TODO move into utils?

    static ArrowBatchStore createExampleStore(final ArrowColumnStoreFactory storeFactory, final int numRows,
        final int numBatches) throws IOException {
        final var path = TestUtils.createTmpKNIMEArrowPath();
        final var schema = new DefaultColumnarSchema(DataSpec.stringSpec(), DataSpec.intSpec());
        final var store = storeFactory.createStore(schema, path);

        // Write some data to the store
        try (final BatchWriter writer = store.getWriter()) {
            for (int b = 0; b < numBatches; b++) {
                final var batch = writer.create(numRows);
                final var stringData = (StringWriteData)batch.get(0);
                final var intData = (IntWriteData)batch.get(1);

                // Fill data
                for (int r = 0; r < numRows; r++) {
                    final int v = r + b;
                    stringData.setString(r, stringForRow(b, r, v));
                    intData.setInt(r, v);
                }

                // Write data
                final var readBatch = batch.close(numRows);
                writer.write(readBatch);
                readBatch.release();
            }
        }

        return store;
    }

    static void checkReadable(final RandomAccessBatchReadable store, final int numRows, final int numBatches)
        throws IOException {

        // Check for the expected schema
        final ColumnarSchema schema = store.getSchema();
        assertEquals(3, schema.numColumns());
        assertEquals(DataSpec.stringSpec(), schema.getSpec(0));
        assertEquals(DataSpec.intSpec(), schema.getSpec(1));
        assertEquals(DataSpec.intSpec(), schema.getSpec(2));

        try (final RandomAccessBatchReader reader = store.createRandomAccessReader()) {
            for (int b = 0; b < numBatches; b++) {
                final ReadBatch batch = reader.readRetained(b);
                final StringReadData stringData = (StringReadData)batch.get(0);
                final IntReadData intDataSum = (IntReadData)batch.get(1);
                final IntReadData intDataProduct = (IntReadData)batch.get(2);

                // Print data
                System.out.println("Batch " + b + " at index 0: [" //
                    + "'" + stringData.getString(0) + "', " //
                    + intDataSum.getInt(0) + ", " //
                    + intDataProduct.getInt(0) + "]" //
                );

                for (int r = 0; r < numRows; r++) {
                    var v = r + b;
                    assertEquals(stringForRow(b, r, v), stringData.getString(r));
                    assertEquals(v + v, intDataSum.getInt(r));
                    assertEquals(v * v, intDataProduct.getInt(r));
                }

                batch.release();
            }
        }
    }

    static String stringForRow(final int batch, final int row, final int value) {
        return "batch: " + batch + ", row: " + row + ", value: " + value;
    }
}
