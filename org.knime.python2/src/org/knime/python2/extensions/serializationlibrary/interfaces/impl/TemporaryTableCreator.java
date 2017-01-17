package org.knime.python2.extensions.serializationlibrary.interfaces.impl;

import java.util.ArrayList;
import java.util.List;

import org.knime.python2.extensions.serializationlibrary.interfaces.Row;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableCreator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;

public class TemporaryTableCreator implements TableCreator {
	
	private final List<Row> m_rows;
	private final TableSpec m_spec;
	
	public TemporaryTableCreator(final TableSpec spec) {
		m_rows = new ArrayList<Row>();
		m_spec = spec;
	}

	@Override
	public void addRow(final Row row) {
		m_rows.add(row);
	}

	@Override
	public TableSpec getTableSpec() {
		return m_spec;
	}
	
	public List<Row> getRows() {
		return m_rows;
	}

}
