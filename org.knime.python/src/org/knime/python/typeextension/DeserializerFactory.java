package org.knime.python.typeextension;

import org.knime.core.data.DataType;

public abstract class DeserializerFactory {
	
	private DataType m_type;
	
	public DeserializerFactory(final DataType dataType) {
		m_type = dataType;
	}
	
	public final DataType getDataType() {
		return m_type;
	}
	
	public abstract Deserializer createDeserializer();

}
