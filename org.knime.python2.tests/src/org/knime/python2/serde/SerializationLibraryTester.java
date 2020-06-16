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
public final class SerializationLibraryTester {

	private static final String DEFAULT_TABLE_NAME = "test_table";

	private final SerializationLibraryExtension m_serializationLibraryExtension;

	private final SerializationLibrary m_serializer;

	private final SerializationTestUtil m_util;

	/**
	 * @param factoryClass The factory class of the serialization library under test.
	 */
	public SerializationLibraryTester(final Class<? extends SerializationLibraryFactory> factoryClass) {
		m_serializationLibraryExtension = SerializationLibraryExtensions.getExtensions().stream()
				.filter(e -> e.getJavaSerializationLibraryFactory().getClass() == factoryClass).findFirst()
				.orElseThrow(() -> new IllegalStateException(
						"Serialization library extension could not be found: " + factoryClass.getName()));
		m_serializer = m_serializationLibraryExtension.getJavaSerializationLibraryFactory().createInstance();
		m_util = new SerializationTestUtil();
	}

	// Tests:

	// TODO:
	// - Add tests for other data types.
	// - Generalize #createDefaultIntTable() to create arbitrary test tables based on a table spec.
	// - Move generalized method to SerializationTestUtil.

	/**
	 * Tests Java side only. Note that this test only works properly if the data format used by the serialization
	 * library under test is "symmetric" with respect to the direction of (de)serialization.
	 *
	 * @throws SerializationException If something went wrong during (de)serialization.
	 */
	public void testIntOfflineSerializationDeserializationIdentity() throws SerializationException {
		final TestTable table = createDefaultIntTable();
		testOfflineSerializationDeserializationIdentity(table);
	}

	/**
	 * Tests Java side and Python side.
	 *
	 * @throws IOException If any error occurred while communicating with Python. This includes errors during
	 *             (de)serialization.
	 */
	public void testIntOnlineSerializationDeserializationIdentity() throws IOException {
		final TestTable table = createDefaultIntTable();
		testOnlineSerializationDeserializationIdentity(table);
	}

	// Helpers:

	private TestTable createDefaultIntTable() {
		final Type[] types = new Type[] { Type.INTEGER, Type.INTEGER_LIST, Type.INTEGER_SET };
		final String[] names = new String[] { "scalar", "list", "set" };
		final TableSpecImpl spec = new TableSpecImpl(types, names, new HashMap<>());
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
					switch (types[j]) {
					case INTEGER:
						cell = m_util.createRandomIntCell();
						break;
					case INTEGER_LIST:
						cell = m_util.createRandomIntListCell(m_util.getRandomNumberOfCollectionElements(), -1f);
						break;
					case INTEGER_SET:
						cell = m_util.createRandomIntSetCell(m_util.getRandomNumberOfCollectionElements(), -1f);
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

	private void testOfflineSerializationDeserializationIdentity(final TestTable testTable)
			throws SerializationException {
		final TableSpec originalSpec = testTable.m_spec;
		final Row[] originalRows = testTable.m_rows;
		try {
			final byte[] bytes = m_serializer.tableToBytes(new RowListIterator(originalSpec,
					originalRows),
					DEFAULT_SERIALIZATION_OPTIONS, PythonCancelable.NOT_CANCELABLE);

			final TableSpec deserializedSpec = m_serializer.tableSpecFromBytes(bytes, PythonCancelable.NOT_CANCELABLE);
			assertTableSpecEquals(originalSpec, deserializedSpec);

			final RowListCreator creator = new RowListCreator(deserializedSpec);
			m_serializer.bytesIntoTable(creator, bytes, DEFAULT_SERIALIZATION_OPTIONS, PythonCancelable.NOT_CANCELABLE);
			final List<Row> deserializedTable = creator.getTable();
			assertRowsEqual(Arrays.asList(originalRows), deserializedTable);
		} catch (final PythonCanceledExecutionException ex) {
			// Cannot happen, we pass non-cancelables above.
			throw new IllegalStateException(ex);
		}
	}

	private void testOnlineSerializationDeserializationIdentity(final TestTable testTable) throws IOException {
		final TableSpec originalSpec = testTable.m_spec;
		final Row[] originalRows = testTable.m_rows;
		try (@SuppressWarnings("deprecation")
		PythonKernel kernel = new PythonKernel(createConfiguredKernelOptions(new PythonKernelOptions()))) {
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
		} catch (final PythonCanceledExecutionException ex) {
			// Cannot happen, we pass non-cancelables above.
			throw new IllegalStateException(ex);
		}
	}

	private PythonKernelOptions createConfiguredKernelOptions(final PythonKernelOptions options) {
		return options.forSerializationOptions(
				options.getSerializationOptions().forSerializerId(m_serializationLibraryExtension.getId()));
	}

	private class TestTable {

		private final Row[] m_rows;

		private final TableSpec m_spec;

		private TestTable(final Row[] rows, final TableSpec spec) {
			m_rows = rows;
			m_spec = spec;
		}
	}
}
