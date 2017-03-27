package org.knime.python.typeextension;

import org.knime.core.data.DataValue;
import org.knime.python2.typeextension.Serializer;
import org.knime.python2.typeextension.SerializerFactory;

public class SerializerFactoryWrapper<Value extends DataValue> extends SerializerFactory<Value> {
	
	private final org.knime.python.typeextension.SerializerFactory<Value> m_serializerFactory;
	
	public SerializerFactoryWrapper(org.knime.python.typeextension.SerializerFactory<Value> serializerFactory) {
		super(serializerFactory.getDataValue());
		m_serializerFactory = serializerFactory;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Serializer<Value> createSerializer() {
		return new SerializerWrapper<Value>((org.knime.python.typeextension.Serializer<Value>)m_serializerFactory.createSerializer());
	}

}
