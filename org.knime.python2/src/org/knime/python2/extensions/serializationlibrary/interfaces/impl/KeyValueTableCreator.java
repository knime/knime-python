package org.knime.python2.extensions.serializationlibrary.interfaces.impl;

import org.knime.python2.extensions.serializationlibrary.interfaces.Row;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableCreator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;

public class KeyValueTableCreator implements TableCreator {
	
	private Row m_row;

	@Override
	public void addRow(final Row row) {
		m_row = row;
	}

	@Override
	public TableSpec getTableSpec() {
		return null;
	}
	
	public Row getRow() {
		return m_row;
	}

}
