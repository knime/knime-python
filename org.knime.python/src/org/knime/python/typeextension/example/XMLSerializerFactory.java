package org.knime.python.typeextension.example;

import java.io.IOException;

import org.knime.core.data.xml.XMLValue;
import org.knime.python.typeextension.Serializer;
import org.knime.python.typeextension.SerializerFactory;

public class XMLSerializerFactory extends SerializerFactory<XMLValue> {
	
	public XMLSerializerFactory() {
		super(XMLValue.class);
	}
	
	@Override
	public Serializer<XMLValue> createSerializer() {
		return new XMLSerializer();
	}
	
	private class XMLSerializer implements Serializer<XMLValue> {

		@Override
		public byte[] serialize(XMLValue value) throws IOException {
			return value.toString().getBytes();
		}
	
	}

}
