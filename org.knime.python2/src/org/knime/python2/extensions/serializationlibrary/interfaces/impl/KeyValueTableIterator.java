package org.knime.python2.extensions.serializationlibrary.interfaces.impl;

import org.knime.python2.extensions.serializationlibrary.interfaces.Row;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableIterator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;

public class KeyValueTableIterator implements TableIterator {
	
	private TableSpec m_spec;
	private Row m_row;
	
	public KeyValueTableIterator(final TableSpec spec, final Row row) {
		m_spec = spec;
		m_row = row;
	}

	@Override
	public Row next() {
		Row next = m_row;
		m_row = null;
		return next;
	}

	@Override
	public boolean hasNext() {
		return m_row != null;
	}

	@Override
	public int getNumberRemainingRows() {
		return m_row != null ? 1 : 0;
	}

	@Override
	public TableSpec getTableSpec() {
		return m_spec;
	}

}
