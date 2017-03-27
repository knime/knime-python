package org.knime.python2.extensions.serializationlibrary.interfaces.impl;

import java.util.Map;

import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;

public class TableSpecImpl implements TableSpec {
	
	private final Type[] m_types;
	private final String[] m_names;
	private final Map<String, String> m_columnSerializers;
	
	public TableSpecImpl(final Type[] types, final String[] names, final Map<String, String> columnSerializers) {
		m_types = types;
		m_names = names;
		m_columnSerializers = columnSerializers;
	}

	@Override
	public Type[] getColumnTypes() {
		return m_types;
	}

	@Override
	public String[] getColumnNames() {
		return m_names;
	}

	@Override
	public int getNumberColumns() {
		return m_types.length;
	}
	
	@Override
	public int findColumn(final String name) {
		int index = -1;
		for (int i = 0; i < m_names.length; i++) {
			if (m_names[i].equals(name)) {
				index = i;
			}
		}
		return index;
	}
	
	@Override
	public Map<String, String> getColumnSerializers() {
		return m_columnSerializers;
	}

}
