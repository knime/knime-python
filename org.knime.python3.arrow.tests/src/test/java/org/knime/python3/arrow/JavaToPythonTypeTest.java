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
 *   Apr 19, 2021 (benjamin): created
 */
package org.knime.python3.arrow;

import static org.knime.core.table.schema.DataSpecs.BOOLEAN;
import static org.knime.core.table.schema.DataSpecs.BYTE;
import static org.knime.core.table.schema.DataSpecs.DICT_ENCODING;
import static org.knime.core.table.schema.DataSpecs.DOUBLE;
import static org.knime.core.table.schema.DataSpecs.DURATION;
import static org.knime.core.table.schema.DataSpecs.FLOAT;
import static org.knime.core.table.schema.DataSpecs.INT;
import static org.knime.core.table.schema.DataSpecs.LIST;
import static org.knime.core.table.schema.DataSpecs.LOCALDATE;
import static org.knime.core.table.schema.DataSpecs.LOCALDATETIME;
import static org.knime.core.table.schema.DataSpecs.LOCALTIME;
import static org.knime.core.table.schema.DataSpecs.LONG;
import static org.knime.core.table.schema.DataSpecs.PERIOD;
import static org.knime.core.table.schema.DataSpecs.STRING;
import static org.knime.core.table.schema.DataSpecs.STRUCT;
import static org.knime.core.table.schema.DataSpecs.VARBINARY;
import static org.knime.core.table.schema.DataSpecs.VOID;
import static org.knime.core.table.schema.DataSpecs.ZONEDDATETIME;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.knime.core.columnar.arrow.ArrowBatchReadStore;
import org.knime.core.columnar.arrow.ArrowBatchStore;
import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.columnar.arrow.compress.ArrowCompressionUtil;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.BooleanData.BooleanWriteData;
import org.knime.core.columnar.data.ByteData.ByteWriteData;
import org.knime.core.columnar.data.DoubleData.DoubleWriteData;
import org.knime.core.columnar.data.DurationData.DurationWriteData;
import org.knime.core.columnar.data.FloatData.FloatWriteData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.data.ListData.ListWriteData;
import org.knime.core.columnar.data.LocalDateData.LocalDateWriteData;
import org.knime.core.columnar.data.LocalDateTimeData.LocalDateTimeWriteData;
import org.knime.core.columnar.data.LocalTimeData.LocalTimeWriteData;
import org.knime.core.columnar.data.LongData.LongWriteData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.data.PeriodData.PeriodWriteData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.data.StructData.StructWriteData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.columnar.data.VoidData.VoidWriteData;
import org.knime.core.columnar.data.ZonedDateTimeData.ZonedDateTimeWriteData;
import org.knime.core.columnar.data.dictencoding.DictDecodedStringData.DictDecodedStringWriteData;
import org.knime.core.columnar.data.dictencoding.DictDecodedVarBinaryData.DictDecodedVarBinaryWriteData;
import org.knime.core.data.columnar.table.DefaultColumnarBatchReadStore.ColumnarBatchReadStoreBuilder;
import org.knime.core.data.columnar.table.DefaultColumnarBatchStore.ColumnarBatchStoreBuilder;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * Test transfer of different Arrow types to Python. Always transfers one table with one column of a specific type.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class JavaToPythonTypeTest {

    private static final int NUM_ROWS = 20;

    private static final int NUM_BATCHES = 3;

    private ArrowColumnStoreFactory m_storeFactory;

    private BufferAllocator m_allocator;

    /** Create allocator and storeFactory */
    @Before
    public void before() {
        m_allocator = new RootAllocator();
        m_storeFactory = new ArrowColumnStoreFactory(m_allocator, 0, m_allocator.getLimit(),
            ArrowCompressionUtil.ARROW_NO_COMPRESSION);
    }

    /** Close allocator */
    @After
    public void after() {
        m_allocator.close();
    }

    /**
     * Test transfer of a boolean column to Python.
     *
     * @throws Exception
     */
    @Test
    public void testBoolean() throws Exception {
        final ValueSetter<BooleanWriteData> valueSetter = (data, b, r) -> data.setBoolean(r, (r + b) % 5 == 0);
        test("boolean", ColumnarSchema.of(BOOLEAN), valueSetter);
    }

    /**
     * Test transfer of a byte column to Python.
     *
     * @throws Exception
     */
    @Test
    public void testByte() throws Exception {
        final ValueSetter<ByteWriteData> valueSetter = (data, b, r) -> data.setByte(r, (byte)((r + b) % 256 - 128));
        test("byte", ColumnarSchema.of(BYTE), valueSetter);
    }

    /**
     * Test transfer of a double column to Python.
     *
     * @throws Exception
     */
    @Test
    public void testDouble() throws Exception {
        final ValueSetter<DoubleWriteData> valueSetter = (data, b, r) -> data.setDouble(r, r / 10.0 + b);
        test("double", ColumnarSchema.of(DOUBLE), valueSetter);
    }

    /**
     * Test transfer of a float column to Python.
     *
     * @throws Exception
     */
    @Test
    public void testFloat() throws Exception {
        final ValueSetter<FloatWriteData> valueSetter = (data, b, r) -> data.setFloat(r, (float)(r / 10.0 + b));
        test("float", ColumnarSchema.of(FLOAT), valueSetter);
    }

    /**
     * Test transfer of a int column to Python.
     *
     * @throws Exception
     */
    @Test
    public void testInt() throws Exception {
        final ValueSetter<IntWriteData> valueSetter = (data, b, r) -> data.setInt(r, r + b);
        test("int", ColumnarSchema.of(INT), valueSetter);
    }

    /**
     * Test transfer of a long column to Python.
     *
     * @throws Exception
     */
    @Test
    public void testLong() throws Exception {
        final ValueSetter<LongWriteData> valueSetter = (data, b, r) -> data.setLong(r, r + b * 10_000_000_000L);
        test("long", ColumnarSchema.of(LONG), valueSetter);
    }

    /**
     * Test transfer of a var binary column to Python.
     *
     * @throws Exception
     */
    @Test
    public void testVarBinary() throws Exception {
        final ValueSetter<VarBinaryWriteData> valueSetter = (data, b, r) -> {
            final byte[] v = new byte[r % 10];
            for (int i = 0; i < v.length; i++) {
                v[i] = (byte)((b + i) % 128);
            }
            data.setBytes(r, v);
        };
        test("varbinary", ColumnarSchema.of(VARBINARY), valueSetter);
    }

    /**
     * Test transfer of a void column to Python.
     *
     * @throws Exception
     */
    @Test
    public void testVoid() throws Exception {
        final ValueSetter<VoidWriteData> valueSetter = (data, b, r) -> {};
        test("void", ColumnarSchema.of(VOID), valueSetter);
    }

    /**
     * Test transfer of a struct column to Python.
     *
     * @throws Exception
     */
    @Test
    public void testStruct() throws Exception {
        final ValueSetter<StructWriteData> valueSetter = (data, b, r) -> {
            ((IntWriteData)data.getWriteDataAt(0)).setInt(r, r + b);
            ((StringWriteData)data.getWriteDataAt(1)).setString(r, "Row: " + r + ", Batch: " + b);
        };
        test("struct", ColumnarSchema.of(STRUCT.of(INT, STRING)), valueSetter);
    }

    /**
     * Test transfer of a complex struct containing a list column to Python.
     *
     * @throws Exception
     */
    @Test
    public void testComplexStructList() throws Exception {
        test("structcomplex", ColumnarSchema.of(STRUCT.of(LIST.of(STRUCT.of(INT, STRING)), INT)),
            JavaToPythonTypeTest::setComplexStructList);
    }

    /** Set a struct in a list in a struct for {@link #testComplexStructList()} */
    private static void setComplexStructList(final StructWriteData data, final int b, final int r) {
        final int listLength = r % 10;
        final StructWriteData listData = ((ListWriteData)data.getWriteDataAt(0)).createWriteData(r, listLength);
        final IntWriteData intData = listData.getWriteDataAt(0);
        final StringWriteData stringData = listData.getWriteDataAt(1);
        for (int i = 0; i < listLength; i++) {
            intData.setInt(i, r + b + i);
            if (i % 7 == 0) {
                stringData.setMissing(i);
            } else {
                stringData.setString(i, "r:" + r + ",b:" + b + ",i:" + i);
            }
        }
        ((IntWriteData)data.getWriteDataAt(1)).setInt(r, r + b);
    }

    /**
     * Test transfer of a list column to Python.
     *
     * @throws Exception
     */
    @Test
    public void testList() throws Exception {
        final ValueSetter<ListWriteData> valueSetter = (data, b, r) -> {
            final var size = r % 10;
            final var intData = (IntWriteData)data.createWriteData(r, size);
            for (int i = 0; i < size; i++) {
                intData.setInt(i, r + b + i);
            }
        };
        test("list", ColumnarSchema.of(LIST.of(INT)), valueSetter);
    }

    /**
     * Test transfer of a String column to Python.
     *
     * @throws Exception
     */
    @Test
    public void testString() throws Exception {
        final ValueSetter<StringWriteData> valueSetter =
            (data, b, r) -> data.setString(r, "Row: " + r + ", Batch: " + b);
        test("string", ColumnarSchema.of(STRING), valueSetter);
    }

    /**
     * Test transfer of a duration column to Python.
     *
     * @throws Exception
     */
    @Test
    public void testDuration() throws Exception {
        final ValueSetter<DurationWriteData> valueSetter =
            (data, b, r) -> data.setDuration(r, Duration.ofSeconds(r, b));
        test("duration", ColumnarSchema.of(DURATION), valueSetter);
    }

    /**
     * Test transfer of a local date column to Python.
     *
     * @throws Exception
     */
    @Test
    public void testLocalDate() throws Exception {
        final ValueSetter<LocalDateWriteData> valueSetter =
            (data, b, r) -> data.setLocalDate(r, LocalDate.ofEpochDay(r + b * 100L));
        test("localdate", ColumnarSchema.of(LOCALDATE), valueSetter);
    }

    /**
     * Test transfer of a local date time column to Python.
     *
     * @throws Exception
     */
    @Test
    public void testLocalDateTime() throws Exception {
        final ValueSetter<LocalDateTimeWriteData> valueSetter = (data, b, r) -> data.setLocalDateTime(r,
            LocalDateTime.of(LocalDate.ofEpochDay(r + b * 100L), LocalTime.ofNanoOfDay(r * 500L + b)));
        test("localdatetime", ColumnarSchema.of(LOCALDATETIME), valueSetter);
    }

    /**
     * Test transfer of a local time column to Python.
     *
     * @throws Exception
     */
    @Test
    public void testLocalTime() throws Exception {
        final ValueSetter<LocalTimeWriteData> valueSetter =
            (data, b, r) -> data.setLocalTime(r, LocalTime.ofNanoOfDay(r * 500L + b));
        test("localtime", ColumnarSchema.of(LOCALTIME), valueSetter);
    }

    /**
     * Test transfer of a period column to Python.
     *
     * @throws Exception
     */
    @Test
    public void testPeriod() throws Exception {
        final ValueSetter<PeriodWriteData> valueSetter =
            (data, b, r) -> data.setPeriod(r, Period.of(r, b % 12, (r + b) % 28));
        test("period", ColumnarSchema.of(PERIOD), valueSetter);
    }

    /**
     * Test transfer of a zoned date time column to Python.
     *
     * @throws Exception
     */
    @Test
    public void testZonedDateTime() throws Exception {
        final List<String> availableZoneIds = List.of("Etc/Universal", "Asia/Hong_Kong", "America/Los_Angeles");

        final ValueSetter<ZonedDateTimeWriteData> valueSetter = (data, b, r) -> {
            final var zoneId = ZoneId.of(availableZoneIds.get(r % 3));
            final var localDateTime = LocalDateTime.of( //
                LocalDate.ofEpochDay(r + b * 100L), //
                LocalTime.ofNanoOfDay(r * 1000L) //
            );
            data.setZonedDateTime(r, ZonedDateTime.of(localDateTime, zoneId));
        };
        test("zoneddatetime", ColumnarSchema.of(ZONEDDATETIME), valueSetter);
    }

    /**
     * Test transfer of a dictionary encoded string column to Python.
     *
     * @throws Exception
     */
    @Test
    public void testDictEncodedString() throws Exception {
        final List<String> dict = List.of("foo", "bar", "car", "aaa");

        final ValueSetter<DictDecodedStringWriteData<Long>> valueSetter = (data, b, r) -> {
            if (b == 0) {
                data.setString(r, dict.get(r % (dict.size() - 1)));
            } else {
                data.setString(r, dict.get(r % dict.size()));
            }
        };
        test("dictstring", ColumnarSchema.of(STRING(DICT_ENCODING)), valueSetter);
    }

    /**
     * Test transfer of a dictionary encoded varbinary column to Python.
     *
     * @throws Exception
     */
    @Test
    public void testDictEncodedVarBinary() throws Exception {
        final List<byte[]> dict = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            final byte[] v = new byte[i + 1];
            for (int j = 0; j < v.length; j++) {
                v[j] = (byte)((j + 50) % 128);
            }
            dict.add(v);
        }

        final ValueSetter<DictDecodedVarBinaryWriteData<Long>> valueSetter = (data, b, r) -> {
            if (b == 0) {
                data.setBytes(r, dict.get(r % (dict.size() - 1)));
            } else {
                data.setBytes(r, dict.get(r % dict.size()));
            }
        };
        test("dictvarbinary", ColumnarSchema.of(VARBINARY(DICT_ENCODING)), valueSetter);
    }

    /** Test sending data to Python for the given type using the values from the valueSetter */
    private <T extends NullableWriteData> void test(final String type, final ColumnarSchema schema,
        final ValueSetter<T> valueSetter) throws Exception {
        final var writePath = TestUtils.createTmpKNIMEArrowPath();
        final var readPath = TestUtils.createTmpKNIMEArrowPath();

        // Open connection to Python
        try (final var pythonGateway = TestUtils.openPythonGateway()) {
            final var entryPoint = pythonGateway.getEntryPoint();

            // Test using a write store -> footer not yet written
            // We need to wrap the store in a columnar store to enable dictionary encoding.
            try (@SuppressWarnings("resource") // Arrow store will be closed along with columnar store.
            final var store = new ColumnarBatchStoreBuilder(m_storeFactory.createStore(schema, writePath))
                .enableDictEncoding(true).build()) {
                // Write some batches
                try (final BatchWriter writer = store.getWriter()) {
                    writeBatches(valueSetter, writer);

                    // Define a Python data source for the data of the store
                    @SuppressWarnings("resource") // Arrow store will be closed along with columnar store.
                    final var dataSource = PythonArrowDataUtils
                        .createSource((ArrowBatchStore)store.getDelegateBatchReadStore(), NUM_BATCHES);

                    // Call Python (footer not written)
                    entryPoint.testTypeToPython(type, dataSource);
                } // <- Footer is written here

                // Move the file to the read store location
                Files.copy(writePath, readPath, StandardCopyOption.REPLACE_EXISTING);
            }

            // Test using a read store -> footer written
            try (@SuppressWarnings("resource") // Arrow store will be closed along with columnar store.
            final var store = new ColumnarBatchReadStoreBuilder(m_storeFactory.createReadStore(schema, readPath))
                .enableDictEncoding(true).build()) {

                // Define a Python data source for the data of the store
                @SuppressWarnings("resource") // Arrow store will be closed along with columnar store.
                final var dataSource =
                    PythonArrowDataUtils.createSource((ArrowBatchReadStore)store.getDelegateBatchReadStore());

                // Call Python (footer written)
                entryPoint.testTypeToPython(type, dataSource);
            }
        }
    }

    /** Write some batches to the store */
    private static <T extends NullableWriteData> void writeBatches(final ValueSetter<T> valueSetter,
        final BatchWriter writer) throws IOException {
        for (int b = 0; b < NUM_BATCHES; b++) {
            final var batch = writer.create(NUM_ROWS);
            fillBatch(valueSetter, b, batch);

            // Write data
            final ReadBatch readBatch = batch.close(NUM_ROWS);
            writer.write(readBatch);
            readBatch.release();
        }
    }

    /** Fill the given batch with values using the valueSetter. Every 13th value is missing. */
    private static <T extends NullableWriteData> void fillBatch(final ValueSetter<T> valueSetter, final int b,
        final WriteBatch batch) {
        final var data = batch.get(0);

        for (int r = 0; r < NUM_ROWS; r++) {
            if (r % 13 == 0) {
                data.setMissing(r);
            } else {
                @SuppressWarnings("unchecked")
                final T dataT = (T)data;
                valueSetter.set(dataT, b, r);
            }
        }
    }

    /** Functional interface for setting a value in a data object */
    @FunctionalInterface
    static interface ValueSetter<T extends NullableWriteData> {
        void set(T data, int batch, int row);
    }
}
