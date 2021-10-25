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
import static org.knime.core.table.schema.DataSpecs.INT;
import static org.knime.core.table.schema.DataSpecs.STRING;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
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
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.data.ListData.ListWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.data.StructData.StructReadData;
import org.knime.core.columnar.data.StructData.StructWriteData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.data.BooleanValue;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DataValue;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.IntValue;
import org.knime.core.data.LongValue;
import org.knime.core.data.RowKeyValue;
import org.knime.core.data.collection.ListCell;
import org.knime.core.data.columnar.table.UnsavedColumnarContainerTable;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.StringValue;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.RowKeyWriteValue;
import org.knime.core.data.v2.RowRead;
import org.knime.core.data.v2.RowValueRead;
import org.knime.core.data.v2.RowValueWrite;
import org.knime.core.data.v2.RowWrite;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.WriteValue;
import org.knime.core.data.v2.value.BooleanValueFactory;
import org.knime.core.data.v2.value.DefaultRowKeyValueFactory;
import org.knime.core.data.v2.value.IntListValueFactory;
import org.knime.core.data.v2.value.DictEncodedStringValueFactory;
import org.knime.core.data.v2.value.DoubleValueFactory;
import org.knime.core.data.v2.value.IntValueFactory;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpecs;
import org.knime.core.table.schema.DataSpecs.DataSpecWithTraits;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.traits.DataTraitUtils;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.LogicalTypeTrait;
import org.knime.filehandling.core.connections.FSCategory;
import org.knime.filehandling.core.connections.FSLocation;
import org.knime.filehandling.core.data.location.FSLocationValue;
import org.knime.filehandling.core.data.location.FSLocationValueFactory;
import org.knime.filehandling.core.data.location.FSLocationValueFactory.FSLocationWriteValue;
import org.knime.filehandling.core.data.location.cell.SimpleFSLocationCellFactory;
import org.knime.python3.PythonCommand;
import org.knime.python3.PythonDataSink;
import org.knime.python3.PythonDataSource;
import org.knime.python3.PythonEntryPoint;
import org.knime.python3.PythonExtension;
import org.knime.python3.PythonGateway;
import org.knime.python3.PythonModule;
import org.knime.python3.PythonModuleKnimeGateway;
import org.knime.python3.PythonPath;
import org.knime.python3.PythonPath.PythonPathBuilder;
import org.knime.python3.SimplePythonCommand;
import org.knime.python3.arrow.PythonArrowDataSource;
import org.knime.python3.arrow.PythonArrowDataUtils;
import org.knime.python3.arrow.PythonArrowExtension;
import org.knime.python3.arrow.PythonModuleKnimeArrow;
import org.knime.python3.arrow.types.utf8string.Utf8StringCell;
import org.knime.python3.arrow.types.utf8string.Utf8StringValue;
import org.knime.python3.arrow.types.utf8string.Utf8StringValueFactory;
import org.knime.python3.arrow.types.utf8string.Utf8StringValueFactory.Utf8StringWriteValue;
import org.knime.python3.data.PythonValueFactoryModule;
import org.knime.python3.data.PythonValueFactoryRegistry;
import org.knime.core.data.v2.value.LongValueFactory;
import org.knime.core.data.v2.value.ValueInterfaces.BooleanWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.DoubleWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.IntListReadValue;
import org.knime.core.data.v2.value.ValueInterfaces.IntListWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.IntWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.LongWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.StringWriteValue;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public class KnimeArrowExtensionTypesTest {

	@SuppressWarnings("deprecation")
	private static final TypeAndFactory<StringValue, StringWriteValue> STRING_TF = TypeAndFactory
			.create(StringCell.TYPE, new DictEncodedStringValueFactory(), StringWriteValue.class);

	private static final TypeAndFactory<DoubleValue, DoubleWriteValue> DOUBLE_TF = TypeAndFactory
			.create(DoubleCell.TYPE, new DoubleValueFactory(), DoubleWriteValue.class);

	private static final TypeAndFactory<LongValue, LongWriteValue> LONG_TF = TypeAndFactory.create(LongCell.TYPE,
			new LongValueFactory(), LongWriteValue.class);

	private static final TypeAndFactory<BooleanValue, BooleanWriteValue> BOOLEAN_TF = TypeAndFactory
			.create(BooleanCell.TYPE, new BooleanValueFactory(), BooleanWriteValue.class);

	private static final TypeAndFactory<IntValue, IntWriteValue> INT_TF = TypeAndFactory.create(IntCell.TYPE,
			new IntValueFactory(), IntWriteValue.class);

	/**
	 * Only OK in this test setting. When used in a workflow, the table id should be
	 * provided by the framework (WorkflowDataRepository) so that the table can get
	 * cleaned up properly.
	 */
	private static final int TABLE_ID = -1;

	private static final String FS_LOCATION_VALUE_FACTORY = FSLocationValueFactory.class.getName();

	private static final String[] FS_LOCATION_LOGICAL_TYPE = { FS_LOCATION_VALUE_FACTORY };

	private static final String[] UTF8_ENCODED_STRING_LOGICAL_TYPE = { Utf8StringValueFactory.class.getName() };

	private static final String INT_LIST_VALUE_FACTORY = IntListValueFactory.class.getName();

	private static final String[] INT_LIST_LOGICAL_TYPE = { INT_LIST_VALUE_FACTORY };

	private ArrowColumnStoreFactory m_storeFactory;

	private BufferAllocator m_allocator;

	@Before
	public void init() {
		m_allocator = new RootAllocator();
		m_storeFactory = new ArrowColumnStoreFactory(m_allocator, 0, m_allocator.getLimit(),
				ArrowCompressionUtil.ARROW_NO_COMPRESSION);
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
		return DataSpecs.STRUCT(new LogicalTypeTrait(FS_LOCATION_VALUE_FACTORY)).of(STRING, STRING, STRING);
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
		return DataSpecs.LIST(new LogicalTypeTrait(INT_LIST_VALUE_FACTORY)).of(INT);
	}

	private PythonArrowEntryPointTester<KnimeArrowExtensionTypeEntryPoint> createTester() throws IOException {
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
						factory.getDataSpecRepresentation(), factory.getJavaValueFactoryName(),
						factory.getDataTraitsJson());
			}
		}
	}

	private static void prepareUtf8EncodedStringData(final BatchWriter writer, final String value) throws IOException {
		final var batch = writer.create(1);
		final VarBinaryWriteData data = (VarBinaryWriteData) batch.get(0);
		data.setBytes(0, value.getBytes(StandardCharsets.UTF_8));
		final ReadBatch readBatch = batch.close(1);
		writer.write(readBatch);
		readBatch.release();
	}

	private static ColumnarSchema createUtf8EncodedSchema() {
		return ColumnarSchema.of(DataSpecs.VARBINARY(new LogicalTypeTrait(Utf8StringValueFactory.class.getName())));
	}

	@Test
	public void testFsLocation() throws Exception {
		try (var tester = createTester()) {
			tester.runJavaToPythonTest(//
					createFsLocationSchema(), //
					FS_LOCATION_LOGICAL_TYPE, //
					w -> prepareFsLocationData(w, "Python", "Value", "Factory"), //
					(e, s) -> e.assertFsLocationEquals(s, "Python", "Value", "Factory")//
			);
		}
	}

	@Test
	public void testUtf8EncodedString() throws Exception {
		try (var tester = createTester()) {
			tester.runJavaToPythonTest(//
					createUtf8EncodedSchema(), //
					UTF8_ENCODED_STRING_LOGICAL_TYPE, //
					w -> prepareUtf8EncodedStringData(w, "barfoo"), //
					(e, s) -> e.assertUtf8EncodedStringEquals(s, "barfoo")//
			);
		}
	}

	@Test
	public void testUtf8EncodedStringFromPythonViaPandas() throws Exception {
		try (var tester = createTester()) {
			tester.runPythonToJavaTest(//
					(e, s) -> e.writeUtf8EncodedStringViaPandas(s, "raboof"), //
					createSingleUtf8Tester("raboof"));
		}
	}

	@Test
	public void testIntListJavaToPython() throws Exception {
		try (var tester = createTester()) {
			tester.runJavaToPythonTest(//
					createIntListSchema(), //
					INT_LIST_LOGICAL_TYPE, //
					w -> prepareIntListData(w, 1, 2, 3, 4, 5), //
					(e, s) -> e.assertIntListEquals(s, 1, 2, 3, 4, 5)//
			);
		}
	}

	private Consumer<RandomAccessBatchReadable> createSingleUtf8Tester(String expected) {
		return createSingleDataTester(0, VarBinaryReadData.class, d -> {
			var decoded = new String(d.getBytes(0), StandardCharsets.UTF_8);
			assertEquals(expected, decoded);// NOSONAR this is test code...
		});
	}

	@Test
	public void testUtf8EncodedStringFromPythonViaPyList() throws Exception {
		try (var tester = createTester()) {
			tester.runPythonToJavaTest((e, s) -> e.writeUtf8EncodedStringViaPyList(s, "bazbar"), //
					createSingleUtf8Tester("bazbar"));
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

	private Consumer<RandomAccessBatchReadable> createSingleFsLocationTester(String category, String specifier,
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
			w.<Utf8StringWriteValue>getWriteValue(1).setValue("bar");
		});
		var tableTester = singleRowTableTester(dataTableSpec(List.of("location", "utf8string"),
				List.of(SimpleFSLocationCellFactory.TYPE, Utf8StringCell.TYPE)), "foo", r -> {
					assertEquals(location, r.<FSLocationValue>getValue(0).getFSLocation());
					assertEquals("bar", r.<Utf8StringValue>getValue(1).getString());
				});
		testCopyingData(() -> rowFiller, tableTester, List.of("location", "utf8string"),
				List.of(new FSLocationValueFactory(), new Utf8StringValueFactory()), EnumSet.allOf(CopyPathway.class));
	}

	@Test
	public void testCopyingSingleIntCell() throws Exception {
		testCopySingleCell(INT_TF, w -> w.setIntValue(42), r -> assertEquals(42, r.getIntValue()),
				EnumSet.allOf(CopyPathway.class));
	}

	@Test
	public void testCopyingMissingIntCell() throws Exception {
		// TODO support pandas rountrip
		// Problem: Missing ints are represented as float NaN in pandas (as are missing floats and missing longs)
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
		// Problem: Missing boolean in pandas is just None, which is the same as a missing string and many other objects
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
		// Problem: A missing long in pandas is represented as float (as are floats and ints)
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
		// Problem: A missing string in pandas is just None, as is a missing boolean or most other objects as well
		testCopySingleMissingCell(STRING_TF, EnumSet.of(CopyPathway.JAVA_PYTHON_JAVA));
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

	@Test
	public void testCopyingSingleUtf8EncodedStringCellTable() throws Exception {
		var rowFiller = singleRowFiller("foobar", r -> r.<Utf8StringWriteValue>getWriteValue(0).setValue("barfoo"));
		var tableTester = singleRowTableTester(dataTableSpec("dummy", Utf8StringCell.TYPE), "foobar",
				r -> assertEquals("barfoo", r.<Utf8StringValue>getValue(0).getString()));
		testCopyingData(() -> rowFiller, tableTester, List.of("dummy"), List.of(new Utf8StringValueFactory()),
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
	private static <D extends NullableReadData> Consumer<RandomAccessBatchReadable> createSingleDataTester(int colIdx,
			Class<D> dataClass, final Consumer<D> dataTester) {
		return createSingleBatchTester(b -> dataTester.accept((D) b.get(colIdx)));
	}

	private static Consumer<RandomAccessBatchReadable> createSingleBatchTester(final Consumer<ReadBatch> batchTester) {
		return s -> {
			try (var reader = s.createRandomAccessReader()) {
				final ReadBatch batch = reader.readRetained(0);
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
				final String dataSpec, final String javaValueFactory, final String dataTraits);

		void assertIntListEquals(PythonArrowDataSource dataSource, int a, int b, int c, int d, int e);

		void assertFsLocationEquals(PythonDataSource dataSource, String category, String specifier, String path);

		void assertUtf8EncodedStringEquals(PythonDataSource dataSource, String value);

		void writeUtf8EncodedStringViaPandas(final PythonDataSink dataSink, String value);

		void writeUtf8EncodedStringViaPyList(final PythonDataSink dataSink, String value);

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
				final PythonModule... modules) throws IOException {
			m_gateway = openPythonGateway(entryPointClass, pythonEntryPointModule, modules);
		}

		E getEntryPoint() {
			return m_gateway.getEntryPoint();
		}

		@Override
		public void close() throws Exception {
			m_gateway.close();
		}

		void runJavaToPythonTest(final ColumnarSchema schema, final String[] logicalTypes, final DataPreparer preparer,
				final DataTester<E> tester) throws IOException {
			final var writePath = createTmpKNIMEArrowPath();
			try (final var store = m_storeFactory.createStore(schema, writePath)) {
				try (final BatchWriter writer = store.getWriter()) {
					preparer.writeBatch(writer);
				}
				final var dataSource = PythonArrowDataUtils.createSource(store, 1);
				tester.test(m_gateway.getEntryPoint(), dataSource);
			}
		}

		void runPythonToJavaTest(BiConsumer<E, PythonDataSink> pythonCommand,
				Consumer<RandomAccessBatchReadable> storeTester) throws IOException {
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
			final var inputPath = createTmpKNIMEArrowPath();
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
			try (final var inputStore = m_storeFactory.createStore(schema, inputPath)) {
				try (var writer = inputStore.getWriter()) {
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
				final var dataSource = PythonArrowDataUtils.createSource(inputStore, 1,
						columnNames.toArray(String[]::new));
				final var dataSink = PythonArrowDataUtils.createSink(outputPath);
				entryPointSelector.accept(getEntryPoint(), dataSource, dataSink);

				try (var table = PythonArrowDataUtils.createTable(dataSink, m_storeFactory, TABLE_ID)) {
					javaResultTester.accept(table);
				}
			}
		}
	}

	private static ColumnarSchema buildSchemaWithLogicalTypeTraits(List<ValueFactory<?, ?>> valueFactories) {
		var builder = DefaultColumnarSchema.builder();
		// row key doesn't have any special traits (at least not right now)
		builder.addColumn(valueFactories.get(0).getSpec());
		valueFactories.stream().skip(1).forEach(f -> builder.addColumn(f.getSpec(), getTraitsWithLogicalType(f)));
		return builder.build();
	}

	private static DataTraits getTraitsWithLogicalType(final ValueFactory<?, ?> valueFactory) {
		var traits = valueFactory.getTraits();
		if (LogicalTypeTrait.hasLogicalType(traits)) {
			return traits;
		} else {
			return DataTraitUtils.withTrait(traits, new LogicalTypeTrait(valueFactory.getClass().getName()));
		}
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

	static Path createTmpKNIMEArrowPath() throws IOException {
		final Path path = Files.createTempFile("KNIME-" + UUID.randomUUID().toString(), ".knarrow");
		path.toFile().deleteOnExit();
		return path;
	}

	private static final String PYTHON_EXE_ENV = "PYTHON3_EXEC_PATH";

	private static <E extends PythonEntryPoint> PythonGateway<E> openPythonGateway(final Class<E> entryPointClass,
			final String launcherModule, final PythonModule... modules) throws IOException {
		final PythonCommand command = getPythonCommand();
		final String launcherPath = Paths.get(System.getProperty("user.dir"), "src/test/python", launcherModule)
				.toString();
		final String knimeGateway = PythonModuleKnimeGateway.getPythonModule();
		final String knimeArrow = PythonModuleKnimeArrow.getPythonModule();
		final PythonPathBuilder builder = PythonPath.builder()//
				.add(knimeGateway) //
				.add(knimeArrow);
		for (final PythonModule module : modules) {
			builder.add(module.getParentDirectory());
		}
		final PythonPath pythonPath = builder.build();
		final List<PythonExtension> pyExtensions = new ArrayList<>();
		pyExtensions.add(PythonArrowExtension.INSTANCE);

		return new PythonGateway<>(command, launcherPath, entryPointClass, pyExtensions, pythonPath);
	}

	/** Create a Python command from the path in the env var PYTHON3_EXEC_PATH */
	private static PythonCommand getPythonCommand() throws IOException {
		final String python3path = System.getenv(PYTHON_EXE_ENV);
		if (python3path != null) {
			return new SimplePythonCommand(python3path);
		}
		throw new IOException(
				"Please set the environment variable '" + PYTHON_EXE_ENV + "' to the path of the Python 3 executable.");
	}

}