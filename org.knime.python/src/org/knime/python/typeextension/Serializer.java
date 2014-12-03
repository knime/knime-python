package org.knime.python.typeextension;

import java.io.IOException;

import org.knime.core.data.DataValue;

public interface Serializer<Value extends DataValue> {
	
	public byte[] serialize(final Value value) throws IOException;

}
