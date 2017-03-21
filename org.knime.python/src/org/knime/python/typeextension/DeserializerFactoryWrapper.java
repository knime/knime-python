package org.knime.python.typeextension;

import org.knime.python2.typeextension.Deserializer;
import org.knime.python2.typeextension.DeserializerFactory;

public class DeserializerFactoryWrapper extends DeserializerFactory {
	
	private final org.knime.python.typeextension.DeserializerFactory m_deserializerFactory;
	
	public DeserializerFactoryWrapper(org.knime.python.typeextension.DeserializerFactory deserializerFactory) {
		super(deserializerFactory.getDataType());
		m_deserializerFactory = deserializerFactory;
	}

	@Override
	public Deserializer createDeserializer() {
		return new DeserializerWrapper(m_deserializerFactory.createDeserializer());
	}

}
