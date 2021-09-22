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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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
import org.junit.Ignore;
import org.junit.Test;
import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.BooleanData.BooleanReadData;
import org.knime.core.columnar.data.ByteData.ByteReadData;
import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.DurationData.DurationReadData;
import org.knime.core.columnar.data.FloatData.FloatReadData;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.LocalDateData.LocalDateReadData;
import org.knime.core.columnar.data.LocalDateTimeData.LocalDateTimeReadData;
import org.knime.core.columnar.data.LocalTimeData.LocalTimeReadData;
import org.knime.core.columnar.data.LongData.LongReadData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.PeriodData.PeriodReadData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StructData.StructReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VoidData.VoidReadData;
import org.knime.core.columnar.data.ZonedDateTimeData.ZonedDateTimeReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedStringReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedVarBinaryReadData;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpecs;

/**
 * Test transfer of different Arrow types from Python. Always transfers one table with one column of a specific type.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class PythonToJavaTypeTest {

    private static final int NUM_ROWS = 20;

    private static final int NUM_BATCHES = 3;

    private static final double DOUBLE_COMPARISON_EPSILON = 1e-12;

    private static final double FLOAT_COMPARISON_EPSILON = 1e-5;

    private ArrowColumnStoreFactory m_storeFactory;

    private BufferAllocator m_allocator;

    /** Create allocator */
    @Before
    public void before() {
        m_allocator = new RootAllocator();
        m_storeFactory = new ArrowColumnStoreFactory(m_allocator);
    }

    /** Close allocator */
    @After
    public void after() {
        m_allocator.close();
    }

    /**
     * Test transfer of a boolean column from Python.
     *
     * @throws Exception
     */
    @Test
    public void testBoolean() throws Exception {
        final ValueChecker<BooleanReadData> valueChecker =
            (data, b, r) -> assertEquals((r + b) % 5 == 0, data.getBoolean(r));
        test("boolean", BOOLEAN, valueChecker);
    }

    /**
     * Test transfer of a byte column from Python.
     *
     * @throws Exception
     */
    @Test
    public void testByte() throws Exception {
        final ValueChecker<ByteReadData> valueChecker =
            (data, b, r) -> assertEquals((byte)((r + b) % 256 - 128), data.getByte(r));
        test("byte", BYTE, valueChecker);
    }

    /**
     * Test transfer of a double column from Python.
     *
     * @throws Exception
     */
    @Test
    public void testDouble() throws Exception {
        final ValueChecker<DoubleReadData> valueChecker =
            (data, b, r) -> assertEquals(r / 10.0 + b, data.getDouble(r), DOUBLE_COMPARISON_EPSILON);
        test("double", DOUBLE, valueChecker);
    }

    /**
     * Test transfer of a float column from Python.
     *
     * @throws Exception
     */
    @Test
    public void testFloat() throws Exception {
        final ValueChecker<FloatReadData> valueChecker =
            (data, b, r) -> assertEquals(r / 10.0 + b, data.getFloat(r), FLOAT_COMPARISON_EPSILON);
        test("float", FLOAT, valueChecker);
    }

    /**
     * Test transfer of a int column from Python.
     *
     * @throws Exception
     */
    @Test
    public void testInt() throws Exception {
        final ValueChecker<IntReadData> valueChecker = (data, b, r) -> assertEquals((long)r + b, data.getInt(r));
        test("int", INT, valueChecker);
    }

    /**
     * Test transfer of a long column from Python.
     *
     * @throws Exception
     */
    @Test
    public void testLong() throws Exception {
        final ValueChecker<LongReadData> valueChecker =
            (data, b, r) -> assertEquals(r + b * 10_000_000_000L, data.getLong(r));
        test("long", LONG, valueChecker);
    }

    /**
     * Test transfer of a var binary column from Python.
     *
     * @throws Exception
     */
    @Test
    public void testVarBinary() throws Exception {
        final ValueChecker<VarBinaryReadData> valueChecker = (data, b, r) -> {
            final byte[] v = new byte[r % 10];
            for (int i = 0; i < v.length; i++) {
                v[i] = (byte)((b + i) % 256);
            }
            assertArrayEquals(v, data.getBytes(r));
        };
        test("varbinary", VARBINARY, valueChecker);
    }

    /**
     * Test transfer of a long column from Python.
     *
     * @throws Exception
     */
    @Test
    public void testVoid() throws Exception {
        final ValueChecker<VoidReadData> valueChecker = (data, b, r) -> {
        };
        test("void", VOID, valueChecker);
    }

    /**
     * Test transfer of a var binary column from Python.
     *
     * @throws Exception
     */
    @Test
    public void testStruct() throws Exception {
        final ValueChecker<StructReadData> valueChecker = (data, b, r) -> {
            assertEquals((long)r + b, ((IntReadData)data.getReadDataAt(0)).getInt(r));
            assertEquals("Row: " + r + ", Batch: " + b, ((StringReadData)data.getReadDataAt(1)).getString(r));
        };
        test("struct", STRUCT.of(INT, STRING), valueChecker);
    }

    /**
     * Test transfer of a complex struct containing a list column from Python.
     *
     * @throws Exception
     */
    @Test
    public void testComplexStructList() throws Exception {
        test("structcomplex",
            STRUCT.of(LIST.of(STRUCT.of(INT, STRING)), INT),
            PythonToJavaTypeTest::checkComplexStructList);
    }

    /** Check a struct in a list in a struct for {@link #testComplexStructList()} */
    private static void checkComplexStructList(final StructReadData data, final int b, final int r) {
        final int listLength = r % 10;
        final StructReadData listData = ((ListReadData)data.getReadDataAt(0)).createReadData(r);
        final IntReadData intData = listData.getReadDataAt(0);
        final StringReadData stringData = listData.getReadDataAt(1);
        for (int i = 0; i < listLength; i++) {
            assertEquals(r + b + i, intData.getInt(i));
            if (i % 7 == 0) {
                assertTrue(stringData.isMissing(i));
            } else {
                assertFalse(stringData.isMissing(i));
                assertEquals("r:" + r + ",b:" + b + ",i:" + i, stringData.getString(i));
            }
        }
        assertEquals(r + b, ((IntReadData)data.getReadDataAt(1)).getInt(r));
    }

    /**
     * Test transfer of a list column from Python.
     *
     * @throws Exception
     */
    @Test
    public void testList() throws Exception {
        final ValueChecker<ListReadData> valueChecker = (data, b, r) -> {
            final IntReadData v = data.createReadData(r);
            final int expectedLength = r % 10;
            assertEquals(expectedLength, v.length());
            for (int i = 0; i < expectedLength; i++) {
                assertEquals((long)r + b + i, v.getInt(i));
            }
        };
        test("list", LIST.of(INT), valueChecker);
    }

    /**
     * Test transfer of a String column from Python.
     *
     * @throws Exception
     */
    @Test
    public void testString() throws Exception {
        final ValueChecker<StringReadData> valueChecker =
            (data, b, r) -> assertEquals("Row: " + r + ", Batch: " + b, data.getString(r));
        test("string", STRING, valueChecker);
    }

    /**
     * Test transfer of a duration column from Python.
     *
     * @throws Exception
     */
    @Test
    @Ignore
    public void testDuration() throws Exception {
        // TODO(extensiontypes) implement a way Python can tell java that the data is of this type
        final ValueChecker<DurationReadData> valueChecker =
            (data, b, r) -> assertEquals(Duration.ofSeconds(r, b), data.getDuration(r));
        test("duration", DURATION, valueChecker);
    }

    /**
     * Test transfer of a local date column from Python.
     *
     * @throws Exception
     */
    @Test
    @Ignore
    public void testLocalDate() throws Exception {
        // TODO(extensiontypes) implement a way Python can tell java that the data is of this type
        final ValueChecker<LocalDateReadData> valueChecker =
            (data, b, r) -> assertEquals(LocalDate.ofEpochDay(r + b * 100L), data.getLocalDate(r));
        test("localdate", LOCALDATE, valueChecker);
    }

    /**
     * Test transfer of a local date time column from Python.
     *
     * @throws Exception
     */
    @Test
    @Ignore
    public void testLocalDateTime() throws Exception {
        // TODO(extensiontypes) implement a way Python can tell java that the data is of this type
        final ValueChecker<LocalDateTimeReadData> valueChecker = (data, b, r) -> assertEquals(
            LocalDateTime.of(LocalDate.ofEpochDay(r + b * 100L), LocalTime.ofNanoOfDay(r * 500L + b)),
            data.getLocalDateTime(r));
        test("localdatetime", LOCALDATETIME, valueChecker);
    }

    /**
     * Test transfer of a local time column from Python.
     *
     * @throws Exception
     */
    @Test
    public void testLocalTime() throws Exception {
        final ValueChecker<LocalTimeReadData> valueChecker =
            (data, b, r) -> assertEquals(LocalTime.ofNanoOfDay(r * 500L + b), data.getLocalTime(r));
        test("localtime", LOCALTIME, valueChecker);
    }

    /**
     * Test transfer of a period column from Python.
     *
     * @throws Exception
     */
    @Test
    @Ignore
    public void testPeriod() throws Exception {
        // TODO(extensiontypes) implement a way Python can tell java that the data is of this type
        final ValueChecker<PeriodReadData> valueChecker =
            (data, b, r) -> assertEquals(Period.of(r, b % 12, (r + b) % 28), data.getPeriod(r));
        test("period", PERIOD, valueChecker);
    }

    /**
     * Test transfer of a zoned date time column from Python.
     *
     * @throws Exception
     */
    @Test
    @Ignore
    public void testZonedDateTime() throws Exception {
        // TODO(extensiontypes) implement a way Python can tell java that the data is of this type
        final List<String> availableZoneIds = new ArrayList<>(ZoneId.getAvailableZoneIds());

        final ValueChecker<ZonedDateTimeReadData> valueChecker = (data, b, r) -> {
            final ZoneId zoneId = ZoneId.of(availableZoneIds.get(b % 3));
            final LocalDateTime localDateTime = LocalDateTime.of( //
                LocalDate.ofEpochDay(r + b * 100L), //
                LocalTime.ofNanoOfDay(r * 500L + b) //
            );
            assertEquals(ZonedDateTime.of(localDateTime, zoneId), data.getZonedDateTime(r));
        };
        test("zoneddatetime", ZONEDDATETIME, valueChecker);
    }

    /**
     * Test transfer of a dictionary encoded string column from Python.
     *
     * @throws Exception
     */
    @Test
    @Ignore
    public void testDictEncodedString() throws Exception {
        // TODO(extensiontypes) implement a way Python can tell java that the data is of this type
        final List<String> dict = List.of("foo", "bar", "car", "aaa");

        final ValueChecker<DictEncodedStringReadData<Long>> valueChecker = (data, b, r) -> {
            if (b == 0) {
                assertEquals(dict.get(r % (dict.size() - 1)), data.getString(r));
            } else {
                assertEquals(dict.get(r % dict.size()), data.getString(r));
            }
        };
        test("dictstring", STRING(DICT_ENCODING), valueChecker);
    }

    /**
     * Test transfer of a dictionary encoded varbinary column from Python.
     *
     * @throws Exception
     */
    @Test
    @Ignore
    public void testDictEncodedVarBinary() throws Exception {
        // TODO(extensiontypes) implement a way Python can tell java that the data is of this type
        final List<byte[]> dict = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            final byte[] v = new byte[i + 1];
            for (int j = 0; j < v.length; j++) {
                v[j] = (byte)((j + 50) % 128);
            }
            dict.add(v);
        }

        final ValueChecker<DictEncodedVarBinaryReadData<Long>> valueChecker = (data, b, r) -> {
            if (b == 0) {
                assertArrayEquals(dict.get(r % (dict.size() - 1)), data.getBytes(r));
            } else {
                assertArrayEquals(dict.get(r % dict.size()), data.getBytes(r));
            }
        };
        test("dictvarbinary", VARBINARY(DICT_ENCODING), valueChecker);
    }

    private <T extends NullableReadData> void test(final String type, final DataSpecs.DataSpecWithTraits spec,
        final ValueChecker<T> valueChecker) throws Exception {
        try (final var pythonGateway = TestUtils.openPythonGateway()) {
            final var entryPoint = pythonGateway.getEntryPoint();

            // Define a Python data sink collecting the data
            final var outPath = TestUtils.createTmpKNIMEArrowPath();
            final var dataSink = PythonArrowDataUtils.createSink(outPath);

            // Call Python
            entryPoint.testTypeFromPython(type, dataSink);

            // Read the data back
            checkData(spec, valueChecker, dataSink);
        }
    }

    private <T extends NullableReadData> void checkData(final DataSpecs.DataSpecWithTraits spec, final ValueChecker<T> valueChecker,
        final DefaultPythonArrowDataSink dataSink) throws IOException, AssertionError {
        try (final var store = PythonArrowDataUtils.createReadable(dataSink, m_storeFactory)) {
            final ColumnarSchema schema = store.getSchema();
            assertEquals(1, schema.numColumns());
            assertEquals(spec.spec(), schema.getSpec(0));
            // TODO(AP-17518) compare traits? They don't have equals implemented
            // assertEquals(spec.traits(), schema.getTraits(0));

            try (final RandomAccessBatchReader reader = store.createRandomAccessReader()) {
                for (int b = 0; b < NUM_BATCHES; b++) {
                    final ReadBatch batch = reader.readRetained(b);
                    checkBatch(valueChecker, b, batch);
                    batch.release();
                }
            }
        }
    }

    private static <T extends NullableReadData> void checkBatch(final ValueChecker<T> valueChecker, final int b,
        final ReadBatch batch) throws AssertionError {
        final NullableReadData data = batch.get(0);

        for (int r = 0; r < NUM_ROWS; r++) {
            if (r % 13 == 0) {
                assertTrue(data.isMissing(r));
            } else {
                if (!(data instanceof VoidReadData)) {
                    assertFalse(data.isMissing(r));
                }
                @SuppressWarnings("unchecked")
                final T dataT = (T)data;
                valueChecker.check(dataT, b, r);
            }
        }
    }

    static interface ValueChecker<T extends NullableReadData> {
        void check(T data, int batch, int row) throws AssertionError;
    }
}
