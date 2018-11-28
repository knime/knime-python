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
 */
package org.knime.python2.serde;

import static org.knime.python2.serde.SerializationTestUtil.DEFAULT_SERIALIZATION_OPTIONS;
import static org.knime.python2.serde.SerializationTestUtil.DEFAULT_TABLE_MISSING_CELL_RATIO;
import static org.knime.python2.serde.SerializationTestUtil.DEFAULT_TABLE_SIZE;
import static org.knime.python2.serde.SerializationTestUtil.assertRowsEqual;
import static org.knime.python2.serde.SerializationTestUtil.assertTableSpecEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.python2.extensions.serializationlibrary.SerializationException;
import org.knime.python2.extensions.serializationlibrary.SerializationLibraryExtension;
import org.knime.python2.extensions.serializationlibrary.SerializationLibraryExtensions;
import org.knime.python2.extensions.serializationlibrary.interfaces.Cell;
import org.knime.python2.extensions.serializationlibrary.interfaces.Row;
import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibrary;
import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibraryFactory;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableCreator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.CellImpl;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.RowImpl;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.TableSpecImpl;
import org.knime.python2.kernel.FlowVariableOptions;
import org.knime.python2.kernel.PythonCancelable;
import org.knime.python2.kernel.PythonCanceledExecutionException;
import org.knime.python2.kernel.PythonKernel;
import org.knime.python2.kernel.PythonKernelOptions;
import org.knime.python2.serde.SerializationTestUtil.RowListCreator;
import org.knime.python2.serde.SerializationTestUtil.RowListCreatorFactory;
import org.knime.python2.serde.SerializationTestUtil.RowListIterator;
import org.knime.python2.serde.SerializationTestUtil.SingleChunkTableChunker;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public abstract class SerializationTest {

	protected static final String DEFAULT_TABLE_NAME = "test_table";

	protected SerializationLibraryExtension m_serializationLibraryExtension;

	protected SerializationLibrary m_serializer;

	protected SerializationTestUtil m_util;

	/**
	 * @return the factory class of the serialization library under test.
	 */
	protected abstract Class<? extends SerializationLibraryFactory> getSerializationLibraryFactoryClass();

	@Before
	public void setUp() {
		final Class<? extends SerializationLibraryFactory> factoryClass = getSerializationLibraryFactoryClass();
		m_serializationLibraryExtension = SerializationLibraryExtensions.getExtensions().stream()
				.filter(e -> e.getJavaSerializationLibraryFactory().getClass() == factoryClass).findFirst().get();
		m_serializer = m_serializationLibraryExtension.getJavaSerializationLibraryFactory().createInstance();
		m_util = new SerializationTestUtil();
	}

	// Tests:

	// TODO: Add tests for other types.
	// Generalize #createDefaultFloat32Table() to create arbitrary test tables based on a table spec.
	// Move generalized method to SerializationTestUtil.

	/**
	 * Tests Java side only.
	 */
	@Test
	public void testFloat32OfflineSerializationDeserializationIdentity()
			throws SerializationException, PythonCanceledExecutionException {
		final TestTable floatTable = createDefaultFloat32Table();
		testOfflineSerializationDeserializationIdentity(floatTable);
	}

	/**
	 * Tests Java side and Python side.
	 */
	@Test
	public void testFloat32OnlineSerializationDeserializationIdentity()
			throws PythonCanceledExecutionException, IOException {
		final TestTable floatTable = createDefaultFloat32Table();
		testOnlineSerializationDeserializationIdentity(floatTable);
	}

	// Helpers:

	protected PythonKernelOptions createConfiguredKernelOptions(final PythonKernelOptions options) {
		Map<String, FlowVariable> variables = new HashMap<>();
		variables.put(FlowVariableOptions.PYTHON_SERIALIZATION_LIBRARY,
				new FlowVariable(FlowVariableOptions.PYTHON_SERIALIZATION_LIBRARY, m_serializationLibraryExtension.getId()));
		FlowVariableOptions flowOptions = FlowVariableOptions.create(variables);
		final PythonKernelOptions configuredOptions = new PythonKernelOptions(options);
		configuredOptions.setFlowVariableOptions(flowOptions);
		configuredOptions.setSerializationOptions(DEFAULT_SERIALIZATION_OPTIONS);
		return configuredOptions;
	}

	protected TestTable createDefaultFloat32Table() {
		final Type[] types = new Type[] { Type.FLOAT, Type.FLOAT_LIST, Type.FLOAT_SET };
		final String[] names = new String[] { "scalar", "list", "set" };
		final TableSpecImpl spec = new TableSpecImpl(types, names, null);
		final int numberOfColumns = spec.getNumberColumns();
		final Row[] rows = new Row[DEFAULT_TABLE_SIZE];
		final int numberOfRows = rows.length;
		for (int i = 0; i < numberOfRows; i++) {
			final Row row = new RowImpl("Row" + i, numberOfColumns);
			for (int j = 0; j < numberOfColumns; j++) {
				Cell cell;
				if (m_util.getMissingDecision(DEFAULT_TABLE_MISSING_CELL_RATIO)) {
					cell = new CellImpl();
				} else {
					// Missing collection elements only in the lower half of the table.
					final float missingProbability = Math.max(0, i / (float) numberOfRows - 0.5f);
					switch (types[j]) {
					case FLOAT:
						cell = m_util.createRandomFloatCell();
						break;
					case FLOAT_LIST:
						cell = m_util.createRandomFloatListCell(m_util.getRandomNumberOfCollectionElements(),
								missingProbability);
						break;
					case FLOAT_SET:
						cell = m_util.createRandomFloatSetCell(m_util.getRandomNumberOfCollectionElements(),
								missingProbability);
						break;
					default:
						throw new IllegalStateException("Implementation error.");
					}
				}
				row.setCell(cell, j);
			}
			rows[i] = row;
		}
		return new TestTable(rows, spec);
	}

	protected void testOfflineSerializationDeserializationIdentity(final TestTable testTable)
			throws SerializationException, PythonCanceledExecutionException {
		final TableSpec originalSpec = testTable.m_spec;
		final Row[] originalRows = testTable.m_rows;

		final byte[] bytes = m_serializer.tableToBytes(new RowListIterator(originalSpec, originalRows),
				DEFAULT_SERIALIZATION_OPTIONS, PythonCancelable.NOT_CANCELABLE);

		final TableSpec deserializedSpec = m_serializer.tableSpecFromBytes(bytes, PythonCancelable.NOT_CANCELABLE);

		assertTableSpecEquals(originalSpec, deserializedSpec);

		final RowListCreator creator = new RowListCreator(deserializedSpec);
		m_serializer.bytesIntoTable(creator, bytes, DEFAULT_SERIALIZATION_OPTIONS, PythonCancelable.NOT_CANCELABLE);
		final List<Row> deserializedTable = creator.getTable();

		assertRowsEqual(Arrays.asList(originalRows), deserializedTable);
	}

	protected void testOnlineSerializationDeserializationIdentity(final TestTable testTable)
			throws IOException, PythonCanceledExecutionException {
		final TableSpec originalSpec = testTable.m_spec;
		final Row[] originalRows = testTable.m_rows;

		try (PythonKernel kernel = new PythonKernel(createConfiguredKernelOptions(new PythonKernelOptions()))) {
			kernel.putData(DEFAULT_TABLE_NAME,
					new SingleChunkTableChunker(new RowListIterator(originalSpec, originalRows)), originalRows.length,
					PythonCancelable.NOT_CANCELABLE);

			@SuppressWarnings("unchecked")
			final TableCreator<List<Row>> creator = (TableCreator<List<Row>>) kernel.getData(DEFAULT_TABLE_NAME,
					new RowListCreatorFactory(), PythonCancelable.NOT_CANCELABLE);

			final TableSpec deserializedSpec = creator.getTableSpec();

			assertTableSpecEquals(originalSpec, deserializedSpec);

			final List<Row> deserializedTable = creator.getTable();

			assertRowsEqual(Arrays.asList(originalRows), deserializedTable);
		}
	}

	protected class TestTable {

		protected final Row[] m_rows;

		protected final TableSpec m_spec;

		protected TestTable(final Row[] rows, final TableSpec spec) {
			m_rows = rows;
			m_spec = spec;
		}
	}
}
