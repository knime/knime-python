package org.knime.python.typeextension.example;

import java.io.IOException;

import org.knime.core.data.xml.XMLValue;
import org.knime.python.typeextension.Serializer;

public class XMLSerializer extends Serializer<XMLValue> {
	
	public XMLSerializer() {
		super(XMLValue.class);
	}

	@Override
	public byte[] serialize(XMLValue value) throws IOException {
		return value.toString().getBytes();
	}

}
