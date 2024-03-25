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
 *   Aug 17, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.arrow.types;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.knime.core.table.schema.DataSpecs.STRING;
import static org.knime.python3.testing.Python3ArrowTestUtils.createTmpKNIMEArrowFileHandle;
import static org.knime.python3.testing.Python3ArrowTestUtils.createTmpKNIMEArrowPath;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.knime.core.columnar.access.ColumnDataIndex;
import org.knime.core.columnar.access.ColumnarAccessFactoryMapper;
import org.knime.core.columnar.access.ColumnarWriteAccess;
import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.columnar.arrow.compress.ArrowCompressionUtil;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.SequentialBatchReadable;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.data.ListData.ListWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.data.StructData.StructReadData;
import org.knime.core.columnar.data.StructData.StructWriteData;
import org.knime.core.data.BooleanValue;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DataValue;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.IntValue;
import org.knime.core.data.LongValue;
import org.knime.core.data.RowKeyValue;
import org.knime.core.data.StringValue;
import org.knime.core.data.collection.ListCell;
import org.knime.core.data.columnar.table.DefaultColumnarBatchStore.ColumnarBatchStoreBuilder;
import org.knime.core.data.columnar.table.UnsavedColumnarContainerTable;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.filestore.internal.NotInWorkflowDataRepository;
import org.knime.core.data.time.duration.DurationCellFactory;
import org.knime.core.data.time.duration.DurationValue;
import org.knime.core.data.time.localdate.LocalDateCellFactory;
import org.knime.core.data.time.localdate.LocalDateValue;
import org.knime.core.data.time.localdatetime.LocalDateTimeCellFactory;
import org.knime.core.data.time.localdatetime.LocalDateTimeValue;
import org.knime.core.data.time.localtime.LocalTimeCellFactory;
import org.knime.core.data.time.localtime.LocalTimeValue;
import org.knime.core.data.time.zoneddatetime.ZonedDateTimeCellFactory;
import org.knime.core.data.time.zoneddatetime.ZonedDateTimeValue;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.RowKeyWriteValue;
import org.knime.core.data.v2.RowRead;
import org.knime.core.data.v2.RowValueRead;
import org.knime.core.data.v2.RowValueWrite;
import org.knime.core.data.v2.RowWrite;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.ValueFactoryUtils;
import org.knime.core.data.v2.WriteValue;
import org.knime.core.data.v2.time.DateTimeValueInterfaces.DurationWriteValue;
import org.knime.core.data.v2.time.DateTimeValueInterfaces.LocalDateTimeWriteValue;
import org.knime.core.data.v2.time.DateTimeValueInterfaces.LocalDateWriteValue;
import org.knime.core.data.v2.time.DateTimeValueInterfaces.LocalTimeWriteValue;
import org.knime.core.data.v2.time.DateTimeValueInterfaces.ZonedDateTimeWriteValue;
import org.knime.core.data.v2.time.DurationValueFactory;
import org.knime.core.data.v2.time.LocalDateTimeValueFactory;
import org.knime.core.data.v2.time.LocalDateValueFactory;
import org.knime.core.data.v2.time.LocalTimeValueFactory;
import org.knime.core.data.v2.time.ZonedDateTimeValueFactory2;
import org.knime.core.data.v2.value.BooleanValueFactory;
import org.knime.core.data.v2.value.DefaultRowKeyValueFactory;
import org.knime.core.data.v2.value.DoubleValueFactory;
import org.knime.core.data.v2.value.IntListValueFactory;
import org.knime.core.data.v2.value.IntValueFactory;
import org.knime.core.data.v2.value.LongValueFactory;
import org.knime.core.data.v2.value.StringValueFactory;
import org.knime.core.data.v2.value.ValueInterfaces.BooleanWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.DoubleWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.IntListReadValue;
import org.knime.core.data.v2.value.ValueInterfaces.IntListWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.IntWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.LongWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.StringWriteValue;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DataSpecs;
import org.knime.core.table.schema.DataSpecs.DataSpecWithTraits;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.traits.LogicalTypeTrait;
import org.knime.filehandling.core.connections.FSCategory;
import org.knime.filehandling.core.connections.FSLocation;
import org.knime.filehandling.core.data.location.FSLocationValue;
import org.knime.filehandling.core.data.location.FSLocationValueFactory;
import org.knime.filehandling.core.data.location.FSLocationValueFactory.FSLocationWriteValue;
import org.knime.filehandling.core.data.location.cell.SimpleFSLocationCellFactory;
import org.knime.python3.DefaultPythonGateway;
import org.knime.python3.Python3SourceDirectory;
import org.knime.python3.PythonCommand;
import org.knime.python3.PythonDataSink;
import org.knime.python3.PythonDataSource;
import org.knime.python3.PythonEntryPoint;
import org.knime.python3.PythonExtension;
import org.knime.python3.PythonGateway;
import org.knime.python3.PythonPath;
import org.knime.python3.PythonPath.PythonPathBuilder;
import org.knime.python3.arrow.Python3ArrowSourceDirectory;
import org.knime.python3.arrow.PythonArrowDataSource;
import org.knime.python3.arrow.PythonArrowDataUtils;
import org.knime.python3.arrow.PythonArrowDataUtils.TableDomainAndMetadata;
import org.knime.python3.arrow.PythonArrowExtension;
import org.knime.python3.testing.Python3ArrowTestUtils;
import org.knime.python3.testing.Python3TestUtils;
import org.knime.python3.types.PythonModule;
import org.knime.python3.types.PythonValueFactoryModule;
import org.knime.python3.types.PythonValueFactoryRegistry;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public class KnimeArrowExtensionTypesTest {

	private static final TypeAndFactory<StringValue, StringWriteValue> STRING_TF = TypeAndFactory
			.create(StringCell.TYPE, new StringValueFactory(), StringWriteValue.class);

	private static final TypeAndFactory<DoubleValue, DoubleWriteValue> DOUBLE_TF = TypeAndFactory
			.create(DoubleCell.TYPE, new DoubleValueFactory(), DoubleWriteValue.class);

	private static final TypeAndFactory<LongValue, LongWriteValue> LONG_TF = TypeAndFactory.create(LongCell.TYPE,
			new LongValueFactory(), LongWriteValue.class);

	private static final TypeAndFactory<BooleanValue, BooleanWriteValue> BOOLEAN_TF = TypeAndFactory
			.create(BooleanCell.TYPE, new BooleanValueFactory(), BooleanWriteValue.class);

	private static final TypeAndFactory<IntValue, IntWriteValue> INT_TF = TypeAndFactory.create(IntCell.TYPE,
			new IntValueFactory(), IntWriteValue.class);

	private static final TypeAndFactory<ZonedDateTimeValue, ZonedDateTimeWriteValue> ZONED_DT_TF = TypeAndFactory
			.create(ZonedDateTimeCellFactory.TYPE, new ZonedDateTimeValueFactory2(), ZonedDateTimeWriteValue.class);

	private static final TypeAndFactory<LocalDateTimeValue, LocalDateTimeWriteValue> LOCAL_DT_TF = TypeAndFactory
			.create(LocalDateTimeCellFactory.TYPE, new LocalDateTimeValueFactory(), LocalDateTimeWriteValue.class);

	private static final TypeAndFactory<LocalTimeValue, LocalTimeWriteValue> LOCAL_TIME_TF = TypeAndFactory
			.create(LocalTimeCellFactory.TYPE, new LocalTimeValueFactory(), LocalTimeWriteValue.class);

	private static final TypeAndFactory<DurationValue, DurationWriteValue> DURATION_TF = TypeAndFactory
			.create(DurationCellFactory.TYPE, new DurationValueFactory(), DurationWriteValue.class);

	private static final TypeAndFactory<LocalDateValue, LocalDateWriteValue> LOCAL_DATE_TF = TypeAndFactory
			.create(LocalDateCellFactory.TYPE, new LocalDateValueFactory(), LocalDateWriteValue.class);

	private ArrowColumnStoreFactory m_storeFactory;

	private BufferAllocator m_allocator;

	@Before
	public void init() {
		m_allocator = new RootAllocator();
		m_storeFactory = new ArrowColumnStoreFactory(m_allocator, ArrowCompressionUtil.ARROW_NO_COMPRESSION);
	}

	private static void prepareFsLocationData(final BatchWriter writer, final String category, final String specifier,
			final String path) throws IOException {
		final var batch = writer.create(1);
		final StructWriteData data = (StructWriteData) batch.get(0);
		data.<StringWriteData>getWriteDataAt(0).setString(0, category);
		data.<StringWriteData>getWriteDataAt(1).setString(0, specifier);
		data.<StringWriteData>getWriteDataAt(2).setString(0, path);
		final ReadBatch readBatch = batch.close(1);
		writer.write(readBatch);
		readBatch.release();
	}

	private static ColumnarSchema createFsLocationSchema() {
		return ColumnarSchema.of(createFsLocationSpec());
	}

	private static DataSpecWithTraits createFsLocationSpec() {
		var traits = ValueFactoryUtils.getTraits(new FSLocationValueFactory());
		return DataSpecs.STRUCT(traits.get(LogicalTypeTrait.class)).of(STRING, STRING, STRING);
	}

	private static void prepareIntListData(final BatchWriter writer, final int... ints) throws IOException {
		final var batch = writer.create(ints.length);
		final ListWriteData data = (ListWriteData) batch.get(0);
		final var intData = (IntWriteData) data.createWriteData(0, ints.length);
		for (int idx = 0; idx < ints.length; idx++) {
			intData.setInt(idx, ints[idx]);
		}
		final ReadBatch readBatch = batch.close(1);
		writer.write(readBatch);
		readBatch.release();
	}

	private static ColumnarSchema createIntListSchema() {
		return ColumnarSchema.of(createIntListSpec());
	}

	private static DataSpecWithTraits createIntListSpec() {
		var valueFactory = IntListValueFactory.INSTANCE;
		return new DataSpecWithTraits(valueFactory.getSpec(), ValueFactoryUtils.getTraits(valueFactory));
	}

	private PythonArrowEntryPointTester<KnimeArrowExtensionTypeEntryPoint> createTester()
			throws IOException, InterruptedException {
		final List<PythonValueFactoryModule> modules = PythonValueFactoryRegistry.getModules();
		final var tester = new PythonArrowEntryPointTester<>(KnimeArrowExtensionTypeEntryPoint.class,
				"extension_tests_launcher.py", modules.toArray(PythonModule[]::new));
		registerPythonValueFactories(tester.getEntryPoint(), modules);
		return tester;
	}

	private static void registerPythonValueFactories(final KnimeArrowExtensionTypeEntryPoint entryPoint,
			final List<PythonValueFactoryModule> factoryModules) {
		for (final var module : factoryModules) {
			final var pythonModule = module.getModuleName();
			for (final var factory : module) {
				entryPoint.registerPythonValueFactory(pythonModule, factory.getPythonValueFactoryName(),
						factory.getDataSpecRepresentation(), factory.getDataTraitsJson(), factory.getValueTypeName(),
						factory.isDefaultPythonRepresentation());
			}
		}
	}

	@Test
	public void testFsLocation() throws Exception {
		try (var tester = createTester()) {
			tester.runJavaToPythonTest(//
					createFsLocationSchema(), //
					w -> prepareFsLocationData(w, "Python", "Value", "Factory"), //
					(e, s) -> e.assertFsLocationEquals(s, "Python", "Value", "Factory")//
			);
		}
	}

	@Test
	public void testIntListJavaToPython() throws Exception {
		try (var tester = createTester()) {
			tester.runJavaToPythonTest(//
					createIntListSchema(), //
					w -> prepareIntListData(w, 1, 2, 3, 4, 5), //
					(e, s) -> e.assertIntListEquals(s, 1, 2, 3, 4, 5)//
			);
		}
	}

	@Test
	@Ignore("Data traits (i.e. dict encoding) are not created on python side")
	public void testFsLocationFromPythonViaPandas() throws Exception {
		try (var tester = createTester()) {
			tester.runPythonToJavaTest(//
					(e, s) -> e.writeFsLocationViaPandas(s, "location", "from", "pandas"), //
					createSingleFsLocationTester("location", "from", "pandas"));
		}

	}

	private Consumer<SequentialBatchReadable> createSingleFsLocationTester(String category, String specifier,
			String path) {
		return createSingleDataTester(0, StructReadData.class, d -> {
			assertEquals(category, d.<StringReadData>getReadDataAt(0).getString(0));
			assertEquals(specifier, d.<StringReadData>getReadDataAt(1).getString(0));
			assertEquals(path, d.<StringReadData>getReadDataAt(2).getString(0));
		});
	}

	@Test
	@Ignore("Data traits (i.e. dict encoding) are not created on python side")
	public void testFsLocationFromPythonViaPyList() throws Exception {
		try (var tester = createTester()) {
			tester.runPythonToJavaTest(//
					(e, s) -> e.writeFsLocationViaPyList(s, "location", "from", "pylist"), //
					createSingleFsLocationTester("location", "from", "pylist"));
		}
	}

	@SuppressWarnings("unchecked")
	private static <W extends WriteAccess> WriteValue<?> createWriteValue(ValueFactory<?, W> valueFactory,
			WriteAccess access) {
		return valueFactory.createWriteValue((W) access);
	}

	private static RowFiller singleRowFiller(final String rowKey, final Consumer<RowValueWrite<?>> valueFiller) {
		return new RowFiller() {

			@Override
			public void accept(RowWrite r) {
				r.setRowKey(rowKey);
				valueFiller.accept(r);
			}

			@Override
			public int getNumRows() {
				return 1;
			}

		};
	}

	private static DataTableSpec dataTableSpec(String name, DataType type) {
		return new DataTableSpec(new DataColumnSpecCreator(name, type).createSpec());
	}

	@Test
	public void testCopyingSingleRowMultipleColumns() throws Exception {
		final var location = new FSLocation(FSCategory.CUSTOM_URL, "1000", "https://www.dummy.com");
		var rowFiller = singleRowFiller("foo", w -> {
			w.<FSLocationWriteValue>getWriteValue(0).setLocation(location);
			w.<IntWriteValue>getWriteValue(1).setIntValue(5);
		});
		var tableTester = singleRowTableTester(
				dataTableSpec(List.of("location", "int"), List.of(SimpleFSLocationCellFactory.TYPE, IntCell.TYPE)),
				"foo", r -> {
					assertEquals(location, r.<FSLocationValue>getValue(0).getFSLocation());
					assertEquals(5, r.<IntValue>getValue(1).getIntValue());
				});
		testCopyingData(() -> rowFiller, tableTester, List.of("location", "int"),
				List.of(new FSLocationValueFactory(), new IntValueFactory()), EnumSet.allOf(CopyPathway.class));
	}

	@Test
	public void testCopyingSingleIntCell() throws Exception {
		testCopySingleCell(INT_TF, w -> w.setIntValue(42), r -> assertEquals(42, r.getIntValue()),
				EnumSet.allOf(CopyPathway.class));
	}

	@Test
	public void testCopyingMissingIntCell() throws Exception {
		// TODO support pandas rountrip
		// Problem: Missing ints are represented as float NaN in pandas (as are missing
		// floats and missing longs)
		testCopySingleMissingCell(INT_TF, EnumSet.of(CopyPathway.JAVA_PYTHON_JAVA));
	}

	@Test
	public void testCopyingSingleBooleanCell() throws Exception {
		testCopySingleCell(BOOLEAN_TF, w -> w.setBooleanValue(true), r -> assertTrue(r.getBooleanValue()),
				EnumSet.allOf(CopyPathway.class));
	}

	@Test
	public void testCopyingMissingBooleanCell() throws Exception {
		// TODO support copying through pandas
		// Problem: Missing boolean in pandas is just None, which is the same as a
		// missing string and many other objects
		testCopySingleMissingCell(BOOLEAN_TF, EnumSet.of(CopyPathway.JAVA_PYTHON_JAVA));
	}

	@Test
	public void testCopyingSingleSmallLongCell() throws Exception {
		testCopySingleCell(LONG_TF, w -> w.setLongValue(1337L), r -> assertEquals(1337L, r.getLongValue()),
				EnumSet.allOf(CopyPathway.class));
	}

	@Test
	public void testCopyingSingleLargeLongCell() throws Exception {
		testCopySingleCell(LONG_TF, w -> w.setLongValue(Long.MAX_VALUE),
				r -> assertEquals(Long.MAX_VALUE, r.getLongValue()), EnumSet.allOf(CopyPathway.class));
	}

	@Test
	public void testCopyingMissingLongCell() throws Exception {
		// TODO support copying through pandas
		// Problem: A missing long in pandas is represented as float (as are floats and
		// ints)
		testCopySingleMissingCell(LONG_TF, EnumSet.of(CopyPathway.JAVA_PYTHON_JAVA));
	}

	@Test
	public void testCopyingSingleDoubleCell() throws Exception {
		testCopySingleCell(DOUBLE_TF, w -> w.setDoubleValue(13.37), r -> assertEquals(13.37, r.getDoubleValue(), 1e-5),
				EnumSet.allOf(CopyPathway.class));
	}

	@Test
	public void testCopyingMissingDoubleCell() throws Exception {
		testCopySingleMissingCell(DOUBLE_TF, EnumSet.allOf(CopyPathway.class));
	}

	@Test
	public void testCopyingSingleStringCell() throws Exception {
		testCopySingleCell(STRING_TF, w -> w.setStringValue("foobar"), r -> assertEquals("foobar", r.getStringValue()),
				EnumSet.of(CopyPathway.JAVA_PYTHON_JAVA));
	}

	@Test
	public void testCopyingMissingStringCell() throws Exception {
		// TODO enable transfer of single string cell via pandas
		// Problem: A missing string in pandas is just None, as is a missing boolean or
		// most other objects as well
		testCopySingleMissingCell(STRING_TF, EnumSet.of(CopyPathway.JAVA_PYTHON_JAVA));
	}

	@Test
	public void testCopyingSingleZonedDateTimeValue() throws Exception {
		final var zonedDt = ZonedDateTime.now();
		// TODO Python's builtin datetime object doesn't support nanoseconds, so we only
		// get microsecond precision
		// TODO Pandas has its own Timestamp data type that seems to provide nanosecond
		// precision
		testCopySingleCell(//
				ZONED_DT_TF, //
				w -> w.setZonedDateTime(zonedDt), //
//				r -> assertEquals(zonedDt, r.getZonedDateTime()), //
				r -> assertEquals(0, ChronoUnit.MICROS.between(zonedDt, r.getZonedDateTime())), //
				EnumSet.allOf(CopyPathway.class)//
		);
	}

	@Test
	public void testCopyingSingleLocalDateTimeValue() throws Exception {
		final var localDt = LocalDateTime.now();
		testCopySingleCell(//
				LOCAL_DT_TF, //
				w -> w.setLocalDateTime(localDt), //
//				r -> assertEquals(localDt, r.getLocalDateTime()), //
				r -> assertEquals(0, ChronoUnit.MICROS.between(localDt, r.getLocalDateTime())), //
				EnumSet.allOf(CopyPathway.class)//
		);
	}

	@Test
	public void testCopyingSingleLocalTimeValue() throws Exception {
		final var time = LocalTime.now();
		testCopySingleCell(//
				LOCAL_TIME_TF, //
				w -> w.setLocalTime(time), //
//				r -> assertEquals(time, r.getLocalTime()), //
				r -> assertEquals(0, ChronoUnit.MICROS.between(time, r.getLocalTime())),
				EnumSet.allOf(CopyPathway.class)//
		);
	}

	@Test
	public void testCopyingSingleDurationValue() throws Exception {
		final var duration = Duration.ofNanos(new Random().nextLong());
		testCopySingleCell(//
				DURATION_TF, //
				w -> w.setDuration(duration), //
//				r -> assertEquals(duration, r.getDuration()), //
				r -> assertTrue("Expected: " + duration + " Got: " + r.getDuration(),
						isWithinMicrosecond(duration, r.getDuration())),
				EnumSet.allOf(CopyPathway.class)//
		);
	}

	private static boolean isWithinMicrosecond(Duration first, Duration second) {
		var absDiff = first.minus(second).abs();
		return absDiff.getSeconds() == 0L && absDiff.getNano() < 1000;
	}

	@Test
	public void testCopyingSingleLocalDateValue() throws Exception {
		final var date = LocalDate.now();
		testCopySingleCell(//
				LOCAL_DATE_TF, //
				w -> w.setLocalDate(date), //
				r -> assertEquals(date, r.getLocalDate()), //
				EnumSet.allOf(CopyPathway.class)//
		);
	}

	private static class TypeAndFactory<D extends DataValue, W extends WriteValue<D>> {

		private final DataType m_type;

		private final ValueFactory<?, ?> m_valueFactory;

		private TypeAndFactory(DataType type, ValueFactory<?, ?> valueFactory) {
			m_type = type;
			m_valueFactory = valueFactory;
		}

		static <D extends DataValue, W extends WriteValue<D>> TypeAndFactory<D, W> create(final DataType type,
				final ValueFactory<?, ?> factory, final Class<W> writeValueClass) {// NOSONAR
			return new TypeAndFactory<>(type, factory);
		}

		private DataType getType() {
			return m_type;
		}

		private ValueFactory<?, ?> getValueFactory() {// NOSONAR
			return m_valueFactory;
		}
	}

	private enum CopyPathway {
		JAVA_PYTHON_JAVA((e, source, sink) -> e.copy(source, sink)),
		JAVA_PYTHON_PANDAS_PYTHON_JAVA((e, source, sink) -> e.copyThroughPandas(source, sink));

		private CopyPathway(
				TriConsumer<KnimeArrowExtensionTypeEntryPoint, PythonDataSource, PythonDataSink> entryPointSelector) {
			this.entryPointSelector = entryPointSelector;
		}

		private final TriConsumer<KnimeArrowExtensionTypeEntryPoint, PythonDataSource, PythonDataSink> entryPointSelector;
	}

	private <D extends DataValue, W extends WriteValue<D>> void testCopySingleCell(TypeAndFactory<D, W> typeAndFactory,
			Consumer<W> valueFiller, Consumer<D> valueTester, EnumSet<CopyPathway> pathways) throws Exception {
		var rowFiller = singleRowFiller("row key", w -> valueFiller.accept(w.getWriteValue(0)));
		var tableTester = singleRowTableTester(dataTableSpec("single column", typeAndFactory.getType()), "row key",
				r -> valueTester.accept(r.getValue(0)));
		testCopyingData(() -> rowFiller, tableTester, List.of("single column"),
				List.of(typeAndFactory.getValueFactory()), pathways);
	}

	private <D extends DataValue, W extends WriteValue<D>> void testCopySingleMissingCell(
			TypeAndFactory<D, W> typeAndFactory, EnumSet<CopyPathway> pathways) throws Exception {
		var rowFiller = singleRowFiller("row key", w -> w.setMissing(0));
		var tableTester = singleRowTableTester(dataTableSpec("single column", typeAndFactory.getType()), "row key",
				r -> assertTrue(r.isMissing(0)));
		testCopyingData(() -> rowFiller, tableTester, List.of("single column"),
				List.of(typeAndFactory.getValueFactory()), pathways);
	}

	private static DataTableSpec dataTableSpec(List<String> names, List<DataType> types) {
		return new DataTableSpec("default", names.toArray(String[]::new), types.toArray(DataType[]::new));
	}

	@Test
	public void testCopyingSingleFsLocationCellTable() throws Exception {
		final var location = new FSLocation(FSCategory.CUSTOM_URL, "1000", "https://www.knime.com");
		var rowFiller = singleRowFiller("Row0", r -> r.<FSLocationWriteValue>getWriteValue(0).setLocation(location));
		var tableTester = singleRowTableTester(dataTableSpec("my column", SimpleFSLocationCellFactory.TYPE), "Row0",
				r -> assertEquals(location, r.<FSLocationValue>getValue(0).getFSLocation()));
		testCopyingData(() -> rowFiller, tableTester, List.of("my column"), List.of(new FSLocationValueFactory()),
				EnumSet.allOf(CopyPathway.class));
	}

	@Test
	public void testCopyingSingleIntListCellTable() throws Exception {
		final var values = new int[] { 1, 2, 3, 4, 5 };
		var rowFiller = singleRowFiller("Row0", r -> {
			final var data = r.<IntListWriteValue>getWriteValue(0);
			data.setValue(values);
		});
		var tableTester = singleRowTableTester(dataTableSpec("my ints", DataType.getType(ListCell.class, IntCell.TYPE)),
				"Row0", r -> assertArrayEquals(values, r.<IntListReadValue>getValue(0).getIntArray()));
		testCopyingData(() -> rowFiller, tableTester, List.of("my ints"), List.of(new IntListValueFactory()),
				EnumSet.allOf(CopyPathway.class));
	}

	private void testCopyingData(Supplier<RowFiller> rowFillerSupplier,
			Consumer<UnsavedColumnarContainerTable> tableTester, List<String> columnNames,
			List<ValueFactory<?, ?>> valueFactories, EnumSet<CopyPathway> pathways) throws Exception {
		try (var tester = createTester()) {
			for (CopyPathway pathway : pathways) {
				tester.runJavaToPythonToJavaTest(pathway.entryPointSelector, rowFillerSupplier.get(), tableTester,
						columnNames, valueFactories);
			}
		}
	}

	private static Consumer<UnsavedColumnarContainerTable> singleRowTableTester(DataTableSpec expectedSpec,
			String expectedRowKey, final Consumer<RowValueRead> rowTester) {
		return table -> {
			DataTableSpec spec = table.getDataTableSpec();
			assertEquals(String.format("Expected: %s Got: %s", colsToString(expectedSpec), colsToString(spec)),
					expectedSpec, spec);
			try (RowCursor cursor = table.cursor()) {
				assertTrue(cursor.canForward());
				RowRead row = cursor.forward();
				assertEquals(expectedRowKey, row.getRowKey().getString());
				rowTester.accept(row);
				assertFalse(cursor.canForward());
			}
		};
	}

	private static String colsToString(final DataTableSpec spec) {
		return spec.stream()//
				.map(Object::toString)//
				.collect(Collectors.joining(",", "[", "]"));
	}

	@Test
	public void launchPythonTests() throws Exception {
		try (var tester = createTester()) {
			tester.getEntryPoint().launchPythonTests();
		}
	}

	@SuppressWarnings("unchecked")
	private static <D extends NullableReadData> Consumer<SequentialBatchReadable> createSingleDataTester(int colIdx,
			Class<D> dataClass, final Consumer<D> dataTester) {
		return createSingleBatchTester(b -> dataTester.accept((D) b.get(colIdx)));
	}

	private static Consumer<SequentialBatchReadable> createSingleBatchTester(final Consumer<ReadBatch> batchTester) {
		return s -> {
			try (var reader = s.createSequentialReader()) {
				final ReadBatch batch = reader.forward();
				batchTester.accept(batch);
				batch.release();
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		};
	}

	@After
	public void after() {
		m_allocator.close();
	}

	/**
	 * To be implemented on Python side.
	 *
	 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
	 */
	public interface KnimeArrowExtensionTypeEntryPoint extends PythonEntryPoint {

		void registerPythonValueFactory(final String pythonModule, final String pythonValueFactoryName,
				final String dataSpec, final String dataTraitsfinal, final String pythonValueTypeName,
				final boolean isDefaultPythonRepresentation);

		void assertIntListEquals(PythonArrowDataSource dataSource, int a, int b, int c, int d, int e);

		void assertFsLocationEquals(PythonDataSource dataSource, String category, String specifier, String path);

		void writeFsLocationViaPandas(final PythonDataSink dataSink, String category, String specifier, String path);

		void writeFsLocationViaPyList(final PythonDataSink dataSink, String category, String specifier, String path);

		void launchPythonTests();

		void copy(PythonDataSource dataSource, PythonDataSink dataSink);

		void copyThroughPandas(PythonDataSource dataSource, PythonDataSink dataSink);
	}

	@FunctionalInterface
	private interface DataPreparer {
		void writeBatch(final BatchWriter writer) throws IOException;
	}

	@FunctionalInterface
	private interface DataTester<E extends PythonEntryPoint> {
		void test(final E entryPoint, PythonArrowDataSource dataSource);
	}

	private final class PythonArrowEntryPointTester<E extends PythonEntryPoint> implements AutoCloseable {

		private final PythonGateway<E> m_gateway;

		PythonArrowEntryPointTester(final Class<E> entryPointClass, final String pythonEntryPointModule,
				final PythonModule... modules) throws IOException, InterruptedException {
			m_gateway = openPythonGateway(entryPointClass, pythonEntryPointModule, modules);
		}

		E getEntryPoint() {
			return m_gateway.getEntryPoint();
		}

		@Override
		public void close() throws Exception {
			m_gateway.close();
		}

		void runJavaToPythonTest(final ColumnarSchema schema, final DataPreparer preparer, final DataTester<E> tester)
				throws IOException {
			final var writePath = Python3ArrowTestUtils.createTmpKNIMEArrowFileHandle();
			try (final var store = m_storeFactory.createStore(schema, writePath)) {
				try (final BatchWriter writer = store.getWriter()) {
					preparer.writeBatch(writer);
				}
				// HACK for tests: skip the first real column spec because Python will prepend
				// <RowKey> to the list of names
				var columnNames = schema.specStream().skip(1).map(DataSpec::toString).toArray(String[]::new);
				final var dataSource = PythonArrowDataUtils.createSource(store, 1, columnNames);
				tester.test(m_gateway.getEntryPoint(), dataSource);
			}
		}

		void runPythonToJavaTest(BiConsumer<E, PythonDataSink> pythonCommand,
				Consumer<SequentialBatchReadable> storeTester) throws IOException {
			final var writePath = createTmpKNIMEArrowPath();
			final var dataSink = PythonArrowDataUtils.createSink(writePath);
			pythonCommand.accept(getEntryPoint(), dataSink);
			try (var store = PythonArrowDataUtils.createReadable(dataSink, m_storeFactory)) {
				storeTester.accept(store);
			}
		}

		void runJavaToPythonToJavaTest(TriConsumer<E, PythonDataSource, PythonDataSink> entryPointSelector,
				RowFiller rowFiller, Consumer<UnsavedColumnarContainerTable> javaResultTester, List<String> columnNames,
				List<ValueFactory<?, ?>> valueFactories) throws Exception {
			final var inputPath = createTmpKNIMEArrowFileHandle();
			final var outputPath = createTmpKNIMEArrowPath();
			var valueFactoriesWithRowKey = new ArrayList<>(valueFactories);
			valueFactoriesWithRowKey.add(0, new DefaultRowKeyValueFactory());
			var rowIndex = new SimpleColumnDataIndex();
			var writeAccesses = valueFactoriesWithRowKey.stream()//
					.map(ValueFactory::getSpec)//
					.map(ColumnarAccessFactoryMapper::createAccessFactory)//
					.map(f -> f.createWriteAccess(rowIndex))//
					.toArray(ColumnarWriteAccess[]::new);
			var writeValues = IntStream.range(0, writeAccesses.length)//
					.mapToObj(i -> createWriteValue(valueFactoriesWithRowKey.get(i), writeAccesses[i]))//
					.toArray(WriteValue[]::new);
			var schema = buildSchemaWithLogicalTypeTraits(valueFactoriesWithRowKey);
			var rowWrite = new ArrayRowWrite(writeValues, writeAccesses);
			try (final var arrowStore = m_storeFactory.createStore(schema, inputPath);
					var columnarStore = new ColumnarBatchStoreBuilder(arrowStore)//
							.enableDictEncoding(true)//
							.build()) {
				try (var writer = columnarStore.getWriter()) {
					int numRows = rowFiller.getNumRows();
					final var batch = writer.create(numRows);
					IntStream.range(0, writeAccesses.length).forEach(i -> writeAccesses[i].setData(batch.get(i)));
					for (int i = 0; i < numRows; i++) {
						rowFiller.accept(rowWrite);
						rowIndex.incrementIndex();
					}
					final ReadBatch readBatch = batch.close(1);
					writer.write(readBatch);
					readBatch.release();
				}
				final var dataSource = PythonArrowDataUtils.createSource(arrowStore, 1,
						columnNames.toArray(String[]::new));
				final var dataSink = PythonArrowDataUtils.createSink(outputPath);
				entryPointSelector.accept(getEntryPoint(), dataSource, dataSink);

				try (var table = PythonArrowDataUtils.createTable(dataSink, TableDomainAndMetadata.empty(),
						m_storeFactory, NotInWorkflowDataRepository.newInstance())) {
					javaResultTester.accept(table);
				}
			}
		}
	}

	private static ColumnarSchema buildSchemaWithLogicalTypeTraits(List<ValueFactory<?, ?>> valueFactories) {
		var builder = DefaultColumnarSchema.builder();
		// row key doesn't have any special traits (at least not right now)
		var rowKeyValueFactory = valueFactories.get(0);
		builder.addColumn(rowKeyValueFactory.getSpec(), ValueFactoryUtils.getTraits(rowKeyValueFactory));
		valueFactories.stream().skip(1).forEach(f -> builder.addColumn(f.getSpec(), ValueFactoryUtils.getTraits(f)));
		return builder.build();
	}

	private static final class SimpleColumnDataIndex implements ColumnDataIndex {

		private int m_index = 0;

		@Override
		public int getIndex() {
			return m_index;
		}

		void incrementIndex() {
			m_index++;
		}

	}

	private static final class ArrayRowWrite implements RowWrite {

		private final WriteValue<?>[] m_writes;

		private final WriteAccess[] m_accesses;

		private final RowKeyWriteValue m_rowKeyWrite;

		ArrayRowWrite(WriteValue<?>[] writes, WriteAccess[] accesses) {
			m_writes = writes;
			m_accesses = accesses;
			m_rowKeyWrite = (RowKeyWriteValue) writes[0];
		}

		@Override
		public void setFrom(RowRead values) {
			setRowKey(values.getRowKey());
			IntStream.range(1, m_writes.length).forEach(i -> m_writes[i].setValue(values.getValue(i - 1)));
		}

		@SuppressWarnings("unchecked")
		@Override
		public <W extends WriteValue<?>> W getWriteValue(int index) {
			return (W) m_writes[index + 1];
		}

		@Override
		public int getNumColumns() {
			return m_writes.length - 1;
		}

		@Override
		public void setMissing(int index) {
			m_accesses[index + 1].setMissing();
		}

		@Override
		public void setRowKey(String rowKey) {
			m_rowKeyWrite.setRowKey(rowKey);
		}

		@Override
		public void setRowKey(RowKeyValue rowKey) {
			m_rowKeyWrite.setRowKey(rowKey);
		}

	}

	interface RowFiller extends Consumer<RowWrite> {
		int getNumRows();
	}

	@FunctionalInterface
	interface TriConsumer<A, B, C> {
		void accept(A a, B b, C c);
	}

	private static <E extends PythonEntryPoint> PythonGateway<E> openPythonGateway(final Class<E> entryPointClass,
			final String launcherModule, final PythonModule... modules) throws IOException, InterruptedException {
		final PythonCommand command = Python3TestUtils.getPythonCommand();
		final String launcherPath = Paths.get(System.getProperty("user.dir"), "src/test/python", launcherModule)
				.toString();
		final PythonPathBuilder builder = PythonPath.builder()//
				.add(Python3SourceDirectory.getPath()) //
				.add(Python3ArrowSourceDirectory.getPath());
		for (final PythonModule module : modules) {
			builder.add(module.getParentDirectory());
		}
		final PythonPath pythonPath = builder.build();
		final List<PythonExtension> pyExtensions = new ArrayList<>();
		pyExtensions.add(PythonArrowExtension.INSTANCE);

		return DefaultPythonGateway.create(command.createProcessBuilder(), launcherPath, entryPointClass, pyExtensions,
				pythonPath);
	}
}
