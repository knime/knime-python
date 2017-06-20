package org.knime.python.typeextension;

import java.io.IOException;

import org.knime.core.data.DataValue;
import org.knime.python2.typeextension.Serializer;

public class SerializerWrapper<Value extends DataValue> implements Serializer<Value> {
	
	private final org.knime.python.typeextension.Serializer<Value> m_serializer;
	
	public SerializerWrapper(org.knime.python.typeextension.Serializer<Value> serializer) {
		m_serializer = serializer;
	}

	@Override
	public byte[] serialize(Value value) throws IOException {
		return m_serializer.serialize(value);
	}
}