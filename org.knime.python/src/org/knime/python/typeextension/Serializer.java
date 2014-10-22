package org.knime.python.typeextension;

import java.io.IOException;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.DataValue;

public abstract class Serializer<Value extends DataValue> {
	
	private DataType m_type;
	private Class<? extends DataValue> m_value;
	
	public Serializer(final DataType dataType, final Class<? extends DataValue> dataValue) {
		m_type = dataType;
		m_value = dataValue;
	}
	
	public final DataType getDataType() {
		return m_type;
	}
	
	public final Class<? extends DataValue> getDataValue() {
		return m_value;
	}
	
	public final boolean isCompatible(final DataType type) {
		return type.isCompatible(m_value);
	}
	
	public abstract byte[] serialize(final Value value) throws IOException;
	
	public abstract DataCell deserialize(final byte[] bytes) throws IOException;

}
