package org.knime.python2.extensions.serializationlibrary.interfaces.impl;

import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;

public class TableSpecImpl implements TableSpec {
	
	private final Type[] m_types;
	private final String[] m_names;
	
	public TableSpecImpl(final Type[] types, final String[] names) {
		m_types = types;
		m_names = names;
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

}
