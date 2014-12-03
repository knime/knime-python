package org.knime.python.typeextension;

import org.knime.core.data.DataType;
import org.knime.core.data.DataValue;

public abstract class SerializerFactory<Value extends DataValue> {
	
	private Class<? extends Value> m_value;
	
	public SerializerFactory(final Class<? extends Value> dataValue) {
		m_value = dataValue;
	}
	
	public final Class<? extends Value> getDataValue() {
		return m_value;
	}
	
	public final boolean isCompatible(final DataType type) {
		return type.isCompatible(m_value);
	}
	
	public abstract Serializer<? extends Value> createSerializer();

}
