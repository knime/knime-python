package org.knime.python2.typeextension.builtin.image;

import java.io.IOException;

import org.knime.base.data.xml.SvgValue;
import org.knime.python2.typeextension.Serializer;
import org.knime.python2.typeextension.SerializerFactory;

public class SvgSerializerFactory extends SerializerFactory<SvgValue> {
	
	public SvgSerializerFactory() {
		super(SvgValue.class);
	}
	
	@Override
	public Serializer<? extends SvgValue> createSerializer() {
		return new SVGSerializer();
	}
	
	private class SVGSerializer implements Serializer<SvgValue> {

		@Override
		public byte[] serialize(SvgValue value) throws IOException {
			return value.toString().getBytes();
		}
	
	}

}
