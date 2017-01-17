package org.knime.python2.extensions.serializationlibrary.interfaces.impl;

import java.util.Iterator;

import org.knime.python2.extensions.serializationlibrary.interfaces.Cell;
import org.knime.python2.extensions.serializationlibrary.interfaces.Row;

public class RowImpl implements Row {
	
	private String m_key;
	private Cell[] m_cells;
	
	public RowImpl(final String rowKey, final int numberCells) {
		m_key = rowKey;
		m_cells = new Cell[numberCells];
	}

	@Override
	public void setCell(final Cell cell, final int index) {
		m_cells[index] = cell;
	}
	
	@Override
	public int getNumberCells() {
		return m_cells.length;
	}
	
	@Override
	public String getRowKey() {
		return m_key;
	}

	@Override
	public Iterator<Cell> iterator() {
		return new Iterator<Cell>() {
			int m_index = 0;

			@Override
			public boolean hasNext() {
				return m_index < m_cells.length;
			}

			@Override
			public Cell next() {
				return m_cells[m_index++];
			}
		};
	}
	
	@Override
	public Cell getCell(final int index) {
		return m_cells[index];
	}

}
