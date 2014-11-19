package org.knime.python.typeextension;

import java.io.IOException;

import org.knime.core.data.DataType;
import org.knime.core.data.DataValue;

public abstract class Serializer<Value extends DataValue> {
	
	private Class<? extends DataValue> m_value;
	
	public Serializer(final Class<? extends DataValue> dataValue) {
		m_value = dataValue;
	}
	
	public final Class<? extends DataValue> getDataValue() {
		return m_value;
	}
	
	public final boolean isCompatible(final DataType type) {
		return type.isCompatible(m_value);
	}
	
	public abstract byte[] serialize(final Value value) throws IOException;

}
