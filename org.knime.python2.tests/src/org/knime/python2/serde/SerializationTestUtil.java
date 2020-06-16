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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Random;

import org.knime.python2.extensions.serializationlibrary.SerializationOptions;
import org.knime.python2.extensions.serializationlibrary.interfaces.Cell;
import org.knime.python2.extensions.serializationlibrary.interfaces.Row;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableChunker;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableCreator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableCreatorFactory;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableIterator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.CellImpl;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class SerializationTestUtil {

	public static final SerializationOptions DEFAULT_SERIALIZATION_OPTIONS = new SerializationOptions();

	public static final int DEFAULT_TABLE_SIZE = 30;

	public static final float DEFAULT_TABLE_MISSING_CELL_RATIO = -1f;

	public static final int DEFAULT_MIN_NUM_COLLECTION_ELEMENTS = 1;

	public static final int DEFAULT_MAX_NUM_COLLECTION_ELEMENTS = 200;

	public static final long DEFAULT_RANDOM_SEED = 1234567;

	// Static utilities for test setup:

	public static byte[] createMissingsVector(final int numberOfElements) {
		return new byte[numberOfElements / 8 + (numberOfElements % 8 == 0 ? 0 : 1)];
	}

	public static void populateMissingsVectorIndex(final byte[] missings, final int index) {
		missings[index / 8] += (1 << (index % 8));
	}

	public static class SingleChunkTableChunker implements TableChunker {

		private final TableIterator m_delegate;

		private boolean m_hasNext = true;

		public SingleChunkTableChunker(final TableIterator iterator) {
			m_delegate = iterator;
		}

		@Override
		public boolean hasNextChunk() {
			return m_hasNext;
		}

		@Override
		public TableIterator nextChunk(final int numRows) {
			if (m_hasNext) {
				if (numRows != m_delegate.getNumberRemainingRows()) {
					throw new IllegalStateException("Not implemented.");
				}
				m_hasNext = false;
				return m_delegate;
			} else {
				throw new NoSuchElementException();
			}
		}

		@Override
		public int getNumberRemainingRows() {
			return m_delegate.getNumberRemainingRows();
		}

		@Override
		public TableSpec getTableSpec() {
			return m_delegate.getTableSpec();
		}
	}

	public static class RowListIterator implements TableIterator {

		private final TableSpec m_spec;

		private final ListIterator<Row> m_delegate;

		private int m_remainingRows;

		public RowListIterator(final TableSpec spec, final Row... rows) {
			this(spec, Arrays.asList(rows));
		}

		public RowListIterator(final TableSpec spec, final List<Row> rows) {
			m_spec = spec;
			m_delegate = rows.listIterator();
			m_remainingRows = rows.size();
		}

		@Override
		public Row next() {
			final Row row = m_delegate.next();
			m_remainingRows--;
			return row;
		}

		@Override
		public boolean hasNext() {
			return m_delegate.hasNext();
		}

		@Override
		public TableSpec getTableSpec() {
			return m_spec;
		}

		@Override
		public int getNumberRemainingRows() {
			return m_remainingRows;
		}
	}

	public static class RowListCreatorFactory implements TableCreatorFactory {

		@Override
		public TableCreator<?> createTableCreator(final TableSpec spec, final int tableSize) {
			return new RowListCreator(spec, tableSize);
		}
	}

	public static class RowListCreator implements TableCreator<List<Row>> {

		private final TableSpec m_spec;

		private final List<Row> m_rows;

		public RowListCreator(final TableSpec spec) {
			m_spec = spec;
			m_rows = new ArrayList<>();
		}

		public RowListCreator(final TableSpec spec, final int tableSize) {
			m_spec = spec;
			m_rows = new ArrayList<>(tableSize);
		}

		@Override
		public void addRow(final Row row) {
			m_rows.add(row);
		}

		@Override
		public TableSpec getTableSpec() {
			return m_spec;
		}

		@Override
		public List<Row> getTable() {
			return m_rows;
		}
	}

	// Static utilities for assertions:

	public static void assertTableSpecEquals(final TableSpec expected, final TableSpec actual) {
		assertEquals(expected.getNumberColumns(), actual.getNumberColumns());
		assertArrayEquals(expected.getColumnNames(), actual.getColumnNames());
		assertArrayEquals(expected.getColumnTypes(), actual.getColumnTypes());
		assertEquals(expected.getColumnSerializers(), actual.getColumnSerializers());
	}

	public static void assertRowsEqual(final List<Row> expected, final List<Row> actual) {
		assertEquals(expected.size(), actual.size());
		final Iterator<Row> expectedRowIterator = expected.listIterator();
		final Iterator<Row> actualRowIterator = actual.listIterator();
		while (expectedRowIterator.hasNext()) {
			final Row expectedRow = expectedRowIterator.next();
			final Row actualRow = actualRowIterator.next();
			assertRowEquals(expectedRow, actualRow);
		}
	}

	public static void assertRowEquals(final Row expected, final Row actual) {
		assertEquals(expected.getRowKey(), actual.getRowKey());
		assertEquals(expected.getNumberCells(), actual.getNumberCells());
		final Iterator<Cell> expectedCellIterator = expected.iterator();
		final Iterator<Cell> actualCellIterator = actual.iterator();
		while (expectedCellIterator.hasNext()) {
			final Cell expectedCell = expectedCellIterator.next();
			final Cell actualCell = actualCellIterator.next();
			assertCellEquals(expectedCell, actualCell);
		}
	}

	public static void assertCellEquals(final Cell expectedCell, final Cell actualCell) {
		if (!(expectedCell instanceof CellImpl && actualCell instanceof CellImpl)) {
			throw new IllegalStateException("Not implemented.");
		}
		CellImpl.cellImplEquals((CellImpl) expectedCell, (CellImpl) actualCell);
	}

	// Stateful utilities for test setup:

	public final Random m_random;

	/**
	 * Equivalent to {@link #SerializationTestUtil(long)} with argument {@link #DEFAULT_RANDOM_SEED}.
	 */
	public SerializationTestUtil() {
		this(DEFAULT_RANDOM_SEED);
	}

	public SerializationTestUtil(final long randomSeed) {
		m_random = new Random(randomSeed);
	}

	/**
	 * @return true if the cell should be missing, false otherwise
	 */
	public boolean getMissingDecision(final float missingProbability) {
		return m_random.nextFloat() < missingProbability;
	}

	public int createRandomInt() {
		return m_random.nextInt();
	}

	public Cell createRandomIntCell() {
		return new CellImpl(createRandomInt());
	}

	public int getRandomNumberOfCollectionElements() {
		return m_random.nextInt(DEFAULT_MAX_NUM_COLLECTION_ELEMENTS - DEFAULT_MIN_NUM_COLLECTION_ELEMENTS)
				+ DEFAULT_MIN_NUM_COLLECTION_ELEMENTS;
	}

	public Cell createRandomIntListCell(final int numberOfElements, final float missingElementProbability) {
		final int[] elements = new int[numberOfElements];
		final byte[] missings = createMissingsVector(numberOfElements);
		for (int i = 0; i < numberOfElements; i++) {
			if (!getMissingDecision(missingElementProbability)) {
				elements[i] = createRandomInt();
				populateMissingsVectorIndex(missings, i);
			}
		}
		return new CellImpl(elements, missings);
	}

	public Cell createRandomIntSetCell(final int numberOfElements, final float missingElementProbability) {
		final boolean hasMissingElement = getMissingDecision(missingElementProbability);
		final int numberOfNonMissingElements = hasMissingElement ? numberOfElements - 1 : numberOfElements;
		final int[] elements = new int[numberOfNonMissingElements];
		for (int i = 0; i < numberOfNonMissingElements; i++) {
			elements[i] = createRandomInt();
		}
		return new CellImpl(elements, hasMissingElement);
	}
}
